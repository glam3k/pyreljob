"""Worker — asyncio-native executor that polls the database for runnable runs.

A claimed unit of work is a :class:`RunRecord` — one invocation of a Job. The
worker loads the run's job (for the job class, args, and the framework-level
retry count), builds a shared :class:`TaskContext`, then runs the job's
``tasks`` in order as coroutines on the event loop. Up to ``max_concurrency``
runs execute concurrently per worker process.

Completed tasks are never re-run: task state is durable in the ``tasks``
table, so if the process dies the next claim resumes at the first
non-succeeded task. Retries are per-task but configured per job: every task is
retried up to the job's ``max_attempts`` with exponential backoff + jitter
(tracked via the run's ``scheduled_at``). When a task exhausts its attempts,
the worker compensates the completed tasks in reverse order and marks the run
``failed``. Cancellation is cooperative and is *stop only* — it never
compensates.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from pyreljob.backends.base import Backend
from pyreljob.job import (
    JobRecord,
    JobSource,
    JobStatus,
    RunRecord,
    TaskRecord,
    TaskStatus,
)
from pyreljob.task import (
    Job,
    JobCancelledError,
    Task,
    TaskContext,
    resolve_job,
    task_for_record,
)

logger = logging.getLogger("pyreljob.worker")


class Worker:
    def __init__(
        self,
        backend: Backend,
        *,
        queue: str | None = None,
        poll_interval: float = 1.0,
        registry: dict[str, type[Job]] | None = None,
        retry_backoff: float = 2.0,
        lease_seconds: int = 30,
        worker_id: str | None = None,
        max_concurrency: int = 1,
    ) -> None:
        self._backend = backend
        self._queue = queue
        self._poll_interval = poll_interval
        self._retry_backoff = retry_backoff
        self._lease_seconds = lease_seconds
        self._max_concurrency = max_concurrency
        self._registry: dict[str, type[Job]] = registry or {}
        # A fresh, unique id per process, generated at startup — so every
        # crash/restart gets a new identity. Used to tag lease ownership
        # (`runs.worker_id`). Pass an explicit id (e.g. hostname:pid or a
        # fixed label) to override; reusing one across restarts is safe since
        # lease expiry, not the id, detects crashes.
        self._worker_id = worker_id or uuid.uuid4().hex[:12]
        self._running = False
        # run_id -> (job_id, ctx) for all in-flight runs (heartbeat + cancel).
        self._active: dict[int, tuple[int, TaskContext]] = {}

    def register(self, name: str, job_cls: type[Job]) -> None:
        """Register a job class under a dotted name."""
        self._registry[name] = job_cls

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def run_forever(self) -> None:
        """Poll for and execute runs until interrupted (SIGINT/SIGTERM)."""

        import signal
        import threading

        if threading.current_thread() is threading.main_thread():
            def handle_signal() -> None:
                self.stop()

            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    signal.signal(sig, lambda s, f: handle_signal())
                except (ValueError, OSError):
                    pass

        self._running = True
        logger.info(
            "worker %s started (queue=%s, max_concurrency=%d)",
            self._worker_id, self._queue or "*", self._max_concurrency,
        )
        try:
            asyncio.run(self._main())
        except KeyboardInterrupt:
            pass
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    async def _main(self) -> None:
        heartbeat = asyncio.create_task(self._heartbeat_loop())
        pending: set[asyncio.Task[Any]] = set()
        try:
            while self._running:
                # Claim runs up to the concurrency cap.
                while self._running and len(pending) < self._max_concurrency:
                    run = await asyncio.to_thread(
                        self._backend.claim,
                        self._worker_id,
                        self._queue,
                        lease_seconds=self._lease_seconds,
                    )
                    if run is None:
                        break
                    pending.add(asyncio.create_task(self._execute(run)))
                if not pending:
                    await asyncio.sleep(self._poll_interval)
                else:
                    await asyncio.wait(
                        pending, timeout=self._poll_interval,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    pending = {t for t in pending if not t.done()}
        finally:
            self._running = False
            heartbeat.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)  # drain
        logger.info("worker %s stopped", self._worker_id)

    async def _heartbeat_loop(self) -> None:
        interval = max(1.0, self._lease_seconds / 3)
        while self._running:
            await asyncio.sleep(interval)
            for run_id, (job_id, ctx) in list(self._active.items()):
                if await asyncio.to_thread(self._backend.is_job_cancelled, job_id):
                    ctx.cancelled = True
                    continue
                if not await asyncio.to_thread(
                    self._backend.renew_lease, run_id, self._worker_id
                ):
                    logger.warning(
                        "run %s: lease lost (another worker claimed it)", run_id
                    )
                    self._active.pop(run_id, None)

    async def _tick(self) -> None:
        """Claim and execute a single run (handy for tests/one-offs)."""
        run = await asyncio.to_thread(
            self._backend.claim,
            self._worker_id,
            self._queue,
            lease_seconds=self._lease_seconds,
        )
        if run is None:
            return
        await self._execute(run)

    async def _execute(self, run: RunRecord) -> None:
        assert run.id is not None
        try:
            await self._run(run)
        except Exception:
            logger.exception("run %s: unhandled error", run.id)
            await asyncio.to_thread(self._backend.fail_run, run.id, "unhandled error")

    async def _run(self, run: RunRecord) -> None:
        assert run.id is not None
        job = await asyncio.to_thread(self._backend.get, run.job_id)
        if job is None:
            logger.error("run %s: job %s missing", run.id, run.job_id)
            await asyncio.to_thread(self._backend.fail_run, run.id, "job no longer exists")
            return
        if job.status == JobStatus.CANCELLED:
            await asyncio.to_thread(self._backend.cancel_run, run.id)
            return

        cls = self._resolve(job.job)
        if cls is None:
            logger.error("run %s: no class registered for %r", run.id, job.job)
            await asyncio.to_thread(
                self._backend.fail_run, run.id, f"No class registered for {job.job!r}"
            )
            return

        ctx = self._build_ctx(run, job)
        records = await asyncio.to_thread(self._backend.tasks, run.id)
        records_by_pos = {record.position: record for record in records}
        start = next(
            (
                i
                for i in range(len(cls.tasks))
                if i not in records_by_pos
                or records_by_pos[i].status != TaskStatus.SUCCEEDED
            ),
            len(cls.tasks),
        )

        self._active[run.id] = (run.job_id, ctx)
        try:
            # Resume after a prior worker that exhausted a task's retries but
            # crashed before finalizing the run as failed.
            if start < len(cls.tasks):
                existing = records_by_pos.get(start)
                if existing is not None and (
                    existing.status == TaskStatus.FAILED
                    and existing.attempts >= self._task_budget(cls.tasks[start], job)
                ):
                    await asyncio.to_thread(self._backend.set_run_ctx, run.id, ctx.as_dict())
                    await self._compensate(run.id, cls, ctx)
                    await self._finalize_failed(job, run.id, existing.error or "task exhausted")
                    await self._reschedule(cls, job, run.id, ctx)
                    return

            for pos in range(start, len(cls.tasks)):
                record = await self._run_task(
                    run.id, cls, job, ctx, pos, records_by_pos.get(pos)
                )
                if record == "cancel":
                    return
                if record == "retry":
                    return
                if record == "failed":
                    await self._reschedule(cls, job, run.id, ctx)
                    return
            await asyncio.to_thread(self._backend.complete_run, run.id, ctx.results)
            if job.id is not None:
                await asyncio.to_thread(self._backend.reset_job_attempts, job.id)
            await self._reschedule(cls, job, run.id, ctx)
            logger.info("run %s: done", run.id)
        finally:
            self._active.pop(run.id, None)

    async def _finalize_failed(
        self,
        job: JobRecord,
        run_id: int,
        error: str,
    ) -> None:
        """Mark a run failed; schedule a whole-run retry if the job has any left."""
        await asyncio.to_thread(self._backend.fail_run, run_id, error)
        if job.id is None or job.retries <= 0:
            return
        attempts = await asyncio.to_thread(self._backend.increment_job_attempts, job.id)
        if attempts <= job.retries:
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=self._backoff(attempts))
            await asyncio.to_thread(self._backend.create_run, job.id, scheduled_at=retry_at)
            logger.info(
                "job %s: whole-run attempt %d/%d failed, retrying at %s",
                job.id, attempts, job.retries, retry_at,
            )

    async def _reschedule(
        self,
        job_cls: type[Job],
        job: JobRecord,
        run_id: int,
        ctx: TaskContext,
    ) -> None:
        """Ask a maintained job when it should run next and re-arm the schedule.

        On-demand jobs run once — their runs are created by ``enqueue`` and
        picked up directly by the worker, so they are never re-armed.
        """
        if job.source != JobSource.SCHEDULED or job.id is None:
            return
        run = await asyncio.to_thread(self._backend.get_run, run_id)
        if run is None:
            return
        instance = job_cls.from_dict(job.args or {})
        next_runtime = instance.next_runtime(run, ctx)
        await asyncio.to_thread(self._backend.set_next_run_at, job.id, next_runtime)

    async def _run_task(
        self,
        run_id: int,
        job_cls: type[Job],
        job: JobRecord,
        ctx: TaskContext,
        pos: int,
        existing: TaskRecord | None,
    ) -> str:
        task_cls = job_cls.tasks[pos]
        max_attempts = self._task_budget(task_cls, job)
        attempts = (existing.attempts if existing else 0) + 1

        await asyncio.to_thread(
            self._backend.start_task, run_id, pos, task_cls.task_name(), attempts=attempts
        )
        try:
            result = await self._run_task_with_timeout(task_cls, ctx)
        except JobCancelledError:
            logger.warning("run %s: cancelled during task %s", run_id, task_cls.__name__)
            await asyncio.to_thread(self._backend.cancel_task, run_id, pos)
            await asyncio.to_thread(self._backend.cancel_run, run_id)
            return "cancel"
        except Exception as exc:  # noqa: BLE001 - any task failure is retried/failed
            error = f"{type(exc).__name__}: {exc}"
            if attempts >= max_attempts:
                logger.error(
                    "run %s: task %s gave up after %d attempts: %s",
                    run_id, task_cls.__name__, attempts, error,
                )
                await asyncio.to_thread(self._backend.set_run_ctx, run_id, ctx.as_dict())
                await asyncio.to_thread(
                    self._backend.fail_task, run_id, pos, error, attempts=attempts
                )
                await self._compensate(run_id, job_cls, ctx)
                await self._finalize_failed(job, run_id, error)
                return "failed"  # run finalized as failed; nothing more to run
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=self._backoff(attempts))
            logger.warning(
                "run %s: task %s attempt %d/%d failed, retrying in %.1fs",
                run_id, task_cls.__name__, attempts, max_attempts,
                (retry_at - datetime.now(timezone.utc)).total_seconds(),
            )
            await asyncio.to_thread(self._backend.set_run_ctx, run_id, ctx.as_dict())
            await asyncio.to_thread(
                self._backend.fail_task, run_id, pos, error, attempts=attempts,
                retry_at=retry_at,
            )
            await asyncio.to_thread(
                self._backend.fail_run, run_id, error, retry_at=retry_at, attempts=attempts
            )
            return "retry"

        ctx.results[task_cls.__name__] = result
        await asyncio.to_thread(self._backend.succeed_task, run_id, pos, result)
        await asyncio.to_thread(self._backend.set_run_ctx, run_id, ctx.as_dict())
        logger.info("run %s: task %s ok", run_id, task_cls.__name__)
        return "ok"

    @staticmethod
    def _task_budget(task_cls: type[Task], job: JobRecord) -> int:
        """Per-task retry budget: the task's own override, else the job's."""
        return task_cls.max_attempts or job.max_attempts

    async def _compensate(
        self,
        run_id: int,
        job_cls: type[Job],
        ctx: TaskContext,
    ) -> None:
        records = await asyncio.to_thread(self._backend.tasks, run_id)
        for record in reversed(records):
            if record.status != TaskStatus.SUCCEEDED:
                continue
            task_cls = task_for_record(record, job_cls)
            try:
                await task_cls().undo(ctx)
            except Exception:
                logger.exception(
                    "run %s: task %s undo failed", run_id, task_cls.__name__
                )
            await asyncio.to_thread(self._backend.compensate_task, run_id, record.position)

    async def _run_task_with_timeout(self, task_cls: type[Task], ctx: TaskContext) -> object:
        task = task_cls()
        timeout = task.timeout
        if timeout is None:
            return await task.run(ctx)
        return await asyncio.wait_for(task.run(ctx), timeout=timeout)

    def _build_ctx(self, run: RunRecord, job: JobRecord) -> TaskContext:
        ctx = (
            TaskContext.from_dict(run.ctx)
            if run.ctx
            else TaskContext(job_id=run.job_id, job=job.job, args=job.args or {})
        )
        ctx.job_id = run.job_id
        ctx.progress = run.progress
        assert run.id is not None
        run_id = run.id
        ctx._progress_hook = lambda value: asyncio.to_thread(
            self._backend.set_run_progress, run_id, value
        )
        return ctx

    def _backoff(self, attempts: int) -> float:
        return self._retry_backoff**attempts + random.uniform(0, 1)

    def _resolve(self, name: str) -> type[Job] | None:
        cls = self._registry.get(name)
        if cls is not None:
            return cls
        cls = resolve_job(name)
        if cls is not None:
            self._registry[name] = cls
        return cls
