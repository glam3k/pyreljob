"""Worker — polls the database for runnable runs and executes them.

A claimed unit of work is a :class:`RunRecord` — one invocation of a Job. The
worker loads the run's job (for the job class, args, and the framework-level
retry count), builds a shared :class:`TaskContext`, then runs the job's
``tasks`` in order. Completed tasks are never re-run: task state is durable in
the ``tasks`` table, so if the process dies the next claim resumes at the
first non-succeeded task.

Retries are per-task but configured per job: every task is retried up to the
job's ``max_attempts`` with exponential backoff + jitter (tracked via the
run's ``scheduled_at``). When a task exhausts its attempts, the worker
compensates the completed tasks in reverse order and marks the run ``failed``.
Cancellation is cooperative and is *stop only* — it never compensates.
"""

from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from pyreljob.backends.base import Backend
from pyreljob.core.job import JobRecord, JobStatus, RunRecord, TaskRecord, TaskStatus
from pyreljob.signals import install_shutdown_handler
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
    ) -> None:
        self._backend = backend
        self._queue = queue
        self._poll_interval = poll_interval
        self._retry_backoff = retry_backoff
        self._lease_seconds = lease_seconds
        self._registry: dict[str, type[Job]] = registry or {}
        # A fresh, unique id per process, generated at startup — so every
        # crash/restart gets a new identity. Used to tag lease ownership
        # (`runs.worker_id`). Pass an explicit id (e.g. hostname:pid or a
        # fixed label) to override; reusing one across restarts is safe since
        # lease expiry, not the id, detects crashes.
        self._worker_id = worker_id or uuid.uuid4().hex[:12]
        self._running = False
        self._current_run_id: int | None = None
        self._current_job_id: int | None = None
        self._current_ctx: TaskContext | None = None

    def register(self, name: str, job_cls: type[Job]) -> None:
        """Register a job class under a dotted name."""
        self._registry[name] = job_cls

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def run_forever(self) -> None:
        """Poll for and execute runs until interrupted (SIGINT/SIGTERM).

        On shutdown the current run is drained to completion before exiting.
        """
        install_shutdown_handler(self.stop)
        self._running = True
        heartbeat = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="pyreljob-heartbeat"
        )
        heartbeat.start()
        logger.info("worker %s started (queue=%s)", self._worker_id, self._queue or "*")
        try:
            while self._running:
                try:
                    self._tick()
                except KeyboardInterrupt:
                    break
                except Exception:
                    logger.exception("worker tick failed")
                time.sleep(self._poll_interval)
        finally:
            self._running = False
            heartbeat.join(timeout=2)
            logger.info("worker %s stopped", self._worker_id)

    def stop(self) -> None:
        self._running = False

    def _heartbeat_loop(self) -> None:
        interval = max(1.0, self._lease_seconds / 3)
        while self._running:
            time.sleep(interval)
            job_id = self._current_job_id
            if job_id is None:
                continue
            if self._backend.is_job_cancelled(job_id):
                ctx = self._current_ctx
                if ctx is not None:
                    ctx.cancelled = True
                continue
            run_id = self._current_run_id
            if run_id is not None and not self._backend.renew_lease(run_id, self._worker_id):
                logger.warning(
                    "run %s: lease lost (another worker claimed it)", run_id
                )
                self._current_run_id = None
                self._current_job_id = None
                self._current_ctx = None

    def _tick(self) -> None:
        run = self._backend.claim(
            self._worker_id, self._queue, lease_seconds=self._lease_seconds
        )
        if run is None:
            return
        self._execute(run)

    def _execute(self, run: RunRecord) -> None:
        assert run.id is not None
        job = self._backend.get(run.job_id)
        if job is None:
            logger.error("run %s: job %s missing", run.id, run.job_id)
            self._backend.fail_run(run.id, "job no longer exists")
            return
        if job.status == JobStatus.CANCELLED:
            self._backend.cancel_run(run.id)
            return

        cls = self._resolve(job.job)
        if cls is None:
            logger.error("run %s: no class registered for %r", run.id, job.job)
            self._backend.fail_run(run.id, f"No class registered for {job.job!r}")
            return

        ctx = self._build_ctx(run, job)
        records = self._backend.tasks(run.id)
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

        self._current_run_id = run.id
        self._current_job_id = run.job_id
        self._current_ctx = ctx
        try:
            # Resume after a prior worker that exhausted a task's retries but
            # crashed before finalizing the run as failed.
            if start < len(cls.tasks):
                existing = records_by_pos.get(start)
                if existing is not None and (
                    existing.status == TaskStatus.FAILED
                    and existing.attempts >= job.max_attempts
                ):
                    self._backend.set_run_ctx(run.id, ctx.as_dict())
                    self._compensate(run.id, cls, ctx)
                    self._backend.fail_run(run.id, existing.error or "task exhausted")
                    self._reschedule(cls, job, run.id, ctx)
                    return

            for pos in range(start, len(cls.tasks)):
                record = self._run_task(
                    run.id, cls, ctx, pos, records_by_pos.get(pos), job.max_attempts
                )
                if record == "cancel":
                    return
                if record == "retry":
                    return
                if record == "failed":
                    self._reschedule(cls, job, run.id, ctx)
                    return
            self._backend.complete_run(run.id, ctx.results)
            self._reschedule(cls, job, run.id, ctx)
            logger.info("run %s: done", run.id)
        finally:
            self._current_run_id = None
            self._current_job_id = None
            self._current_ctx = None

    def _reschedule(
        self,
        job_cls: type[Job],
        job: JobRecord,
        run_id: int,
        ctx: TaskContext,
    ) -> None:
        """Ask the job when it should run next and re-arm the schedule."""
        run = self._backend.get_run(run_id)
        if run is None or job.id is None:
            return
        instance = job_cls.from_dict(job.args or {})
        next_runtime = instance.next_runtime(run, ctx)
        self._backend.set_next_run_at(job.id, next_runtime)

    def _run_task(
        self,
        run_id: int,
        job_cls: type[Job],
        ctx: TaskContext,
        pos: int,
        existing: TaskRecord | None,
        max_attempts: int,
    ) -> str:
        task_cls = job_cls.tasks[pos]
        attempts = (existing.attempts if existing else 0) + 1

        self._backend.start_task(run_id, pos, task_cls.task_name(), attempts=attempts)
        try:
            result = self._run_task_with_timeout(task_cls, ctx)
        except JobCancelledError:
            logger.warning("run %s: cancelled during task %s", run_id, task_cls.__name__)
            self._backend.cancel_task(run_id, pos)
            self._backend.cancel_run(run_id)
            return "cancel"
        except Exception as exc:  # noqa: BLE001 - any task failure is retried/failed
            error = f"{type(exc).__name__}: {exc}"
            if attempts >= max_attempts:
                logger.error(
                    "run %s: task %s gave up after %d attempts: %s",
                    run_id, task_cls.__name__, attempts, error,
                )
                self._backend.set_run_ctx(run_id, ctx.as_dict())
                self._backend.fail_task(run_id, pos, error, attempts=attempts)
                self._compensate(run_id, job_cls, ctx)
                self._backend.fail_run(run_id, error)
                return "failed"  # run finalized as failed; nothing more to run
            retry_at = datetime.now() + timedelta(seconds=self._backoff(attempts))
            logger.warning(
                "run %s: task %s attempt %d/%d failed, retrying in %.1fs",
                run_id, task_cls.__name__, attempts, max_attempts,
                (retry_at - datetime.now()).total_seconds(),
            )
            self._backend.set_run_ctx(run_id, ctx.as_dict())
            self._backend.fail_task(
                run_id, pos, error, attempts=attempts, retry_at=retry_at
            )
            self._backend.fail_run(run_id, error, retry_at=retry_at, attempts=attempts)
            return "retry"

        ctx.results[task_cls.__name__] = result
        self._backend.succeed_task(run_id, pos, result)
        self._backend.set_run_ctx(run_id, ctx.as_dict())
        logger.info("run %s: task %s ok", run_id, task_cls.__name__)
        return "ok"

    def _compensate(
        self,
        run_id: int,
        job_cls: type[Job],
        ctx: TaskContext,
    ) -> None:
        for record in reversed(self._backend.tasks(run_id)):
            if record.status != TaskStatus.SUCCEEDED:
                continue
            task_cls = task_for_record(record, job_cls)
            try:
                task_cls().undo(ctx)
            except Exception:
                logger.exception(
                    "run %s: task %s undo failed", run_id, task_cls.__name__
                )
            self._backend.compensate_task(run_id, record.position)

    def _run_task_with_timeout(self, task_cls: type[Task], ctx: TaskContext) -> object:
        task = task_cls()
        timeout = task.timeout
        if timeout is None:
            return task.run(ctx)

        box: dict[str, Any] = {}

        def _target() -> None:
            try:
                box["result"] = task.run(ctx)
            except BaseException as exc:  # noqa: BLE001 - captured, re-raised below
                box["error"] = exc

        thread = threading.Thread(target=_target, daemon=True)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            raise TimeoutError(f"{task_cls.__name__} exceeded {timeout}s timeout")
        if "error" in box:
            raise box["error"]
        return box.get("result")

    def _build_ctx(self, run: RunRecord, job: JobRecord) -> TaskContext:
        ctx = (
            TaskContext.from_dict(run.ctx)
            if run.ctx
            else TaskContext(job_id=run.job_id, job=job.job, args=job.args or {})
        )
        ctx.job_id = run.job_id
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
