"""JobManager — the control plane for jobs.

A Job is a durable entity (one-off or maintained). Each invocation of it is a
Run. ``JobManager`` is both the client API (enqueue/schedule/cancel/query) and
the beat (``run_forever()`` fires due maintained jobs, creating a run per
job). It wraps a backend (a SQL database) and is the single public entry point
— the ``Worker`` is the separate executor.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from pyreljob.backends import backend_from_url
from pyreljob.backends.base import Backend
from pyreljob.job import JobRecord, RunRecord, TaskRecord, TaskStatus
from pyreljob.task import (
    Job,
    TaskContext,
    job_name,
    resolve_job,
    task_for_record,
    validate_job,
)

logger = logging.getLogger("pyreljob.manager")


class JobManager:
    """A relational-table-backed job manager.

    Usage::

        from dataclasses import dataclass
        from pyreljob import JobManager, Job, Task, TaskContext

        class SendEmail(Task):
            def run(self, ctx: TaskContext) -> None:
                send(ctx.args["to"], ctx.args.get("subject", "hi"))

        @dataclass
        class Notification(Job):
            to: str
            tasks = [SendEmail]

        manager = JobManager("sqlite:///jobs.db")   # or postgresql://...
        manager.migrate()
        job = manager.enqueue(Notification("x@y.z"))
        manager.runs(job.id)   # the job's invocations
    """

    def __init__(self, database_url: str, backend: Backend | None = None) -> None:
        self._backend = backend or backend_from_url(database_url)
        self._running = False
        self._misfire_grace_seconds: int | None = None

    @property
    def backend(self) -> Backend:
        return self._backend

    def migrate(self) -> list[str]:
        """Create/upgrade the database schema. Call once per deployment."""
        return self._backend.migrate()

    def enqueue(
        self,
        job: Job,
        *,
        queue: str | None = None,
        priority: int | None = None,
        max_attempts: int = 3,
        retries: int = 0,
        idempotency_key: str | None = None,
        scheduled_at: datetime | None = None,
    ) -> JobRecord:
        """Create a durable job and its first run.

        ``max_attempts`` is the per-task retry budget (a task can override it
        with ``Task.max_attempts``). ``retries`` is the whole-run retry count:
        if a run fails, the job re-runs from scratch up to ``retries`` more
        times. With ``idempotency_key``, re-enqueueing the same key returns
        the existing job instead of duplicating.
        """
        validate_job(job)
        return self._backend.enqueue(
            job_name(job),
            job.as_dict(),
            queue=queue or job.queue,
            priority=priority if priority is not None else job.priority,
            max_attempts=max_attempts,
            retries=retries,
            idempotency_key=idempotency_key,
            scheduled_at=scheduled_at,
        )

    def schedule(
        self,
        job: Job,
        *,
        queue: str | None = None,
        max_attempts: int = 3,
        retries: int = 0,
    ) -> JobRecord:
        """Register a maintained job.

        The job's :meth:`Job.next_runtime` controls the cadence: the framework
        calls ``next_runtime(None, ctx)`` for the first schedule — a returned
        datetime is when the first run fires, ``None`` fires it immediately.
        After each run the worker calls ``next_runtime(run, ctx)`` again to
        re-arm. Idempotent on job class: re-registering reuses the existing
        job.
        """
        validate_job(job)
        next_run = job.next_runtime(None, TaskContext(0, job_name(job)))
        return self._backend.schedule(
            job_name(job),
            job.as_dict(),
            queue=queue or job.queue,
            max_attempts=max_attempts,
            retries=retries,
            next_run_at=next_run,
        )

    def cancel(self, job_id: int) -> None:
        """Cancel a job (stops its active run and any future runs).

        Cancellation is cooperative (stop only — never compensates).
        """
        self._backend.cancel_job(job_id)

    def delete(self, job_id: int) -> None:
        """Hard-delete a job and all its runs and tasks.

        Raises ValueError if the job has a ready or running run — cancel it
        first.
        """
        self._backend.delete_job(job_id)

    def prune(self, older_than: timedelta) -> int:
        """Delete terminal runs (and their tasks) finished before
        ``now - older_than``. Returns the number of runs deleted."""
        return self._backend.prune_runs(datetime.now() - older_than)

    async def undo(self, job_id: int) -> None:
        """Manually compensate a job's most recent run (saga pattern).

        Reconstructs the job and ``await``s ``undo(ctx)`` on succeeded tasks in
        reverse order, marking each task ``compensated``. Async because task
        ``undo`` methods are coroutines.
        """
        record = self._backend.get(job_id)
        if record is None:
            raise KeyError(f"no job with id {job_id}")
        runs = self._backend.runs(job_id)
        if not runs:
            return
        run = runs[0]  # most recent run
        assert run.id is not None
        cls = resolve_job(record.job)
        if cls is None:
            raise ValueError(f"cannot resolve job class {record.job!r}")
        ctx = TaskContext.from_dict(run.ctx) if run.ctx else TaskContext(
            job_id=run.job_id, job=record.job, args=record.args or {}
        )
        ctx.job_id = run.job_id
        for task_record in reversed(self._backend.tasks(run.id)):
            if task_record.status != TaskStatus.SUCCEEDED:
                continue
            task_cls = task_for_record(task_record, cls)
            await task_cls().undo(ctx)
            self._backend.compensate_task(run.id, task_record.position)

    def get(self, job_id: int) -> JobRecord | None:
        return self._backend.get(job_id)

    def list_jobs(self, *, limit: int = 100, offset: int = 0) -> list[JobRecord]:
        """All jobs, newest first, with pagination."""
        return self._backend.list_jobs(limit=limit, offset=offset)

    def runs(self, job_id: int) -> list[RunRecord]:
        """The runs (invocations) of a job, newest first."""
        return self._backend.runs(job_id)

    def tasks(self, run_id: int) -> list[TaskRecord]:
        """The task executions of a specific run."""
        return self._backend.tasks(run_id)

    def counts(self, queue: str | None = None) -> dict[str, int]:
        """Run counts by status."""
        return self._backend.counts(queue)

    def recent_failures(self, limit: int = 10) -> list[RunRecord]:
        return self._backend.recent_failures(limit)

    # -- beat (maintained jobs) ---------------------------------------------

    def run_forever(
        self,
        poll_interval: float = 15.0,
        misfire_grace_seconds: int | None = None,
    ) -> None:
        """Run the beat loop: fire due maintained jobs until interrupted.

        Durable runs are created for each maintained job as its schedule comes
        due.
        """

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

        self._misfire_grace_seconds = misfire_grace_seconds
        self._running = True
        try:
            while self._running:
                try:
                    self.tick()
                except KeyboardInterrupt:
                    break
                except Exception:
                    logger.exception("scheduler tick failed")
                time.sleep(poll_interval)
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    def tick(self, misfire_grace_seconds: int | None = None) -> None:
        """Find due maintained jobs and create a run for each.

        Coalesces missed runs into a single fire. If ``misfire_grace_seconds``
        is set and a run is overdue by more than that, it is skipped. Each
        fire disarms the job (``next_run_at`` is cleared); the worker re-arms
        it after the run via ``Job.next_runtime``.
        """
        grace = (
            misfire_grace_seconds
            if misfire_grace_seconds is not None
            else self._misfire_grace_seconds
        )
        for job in self._backend.list_scheduled():
            if job.id is None:
                continue
            now = datetime.now()
            if job.next_run_at is not None and job.next_run_at > now:
                continue
            # Claim (clear next_run_at) first so concurrent beats never
            # double-fire; the loser's conditional update fails.
            fired = self._backend.claim_scheduled(job.id, None)
            if not fired:
                continue
            if self._is_misfired(job.next_run_at, now, grace):
                logger.warning(
                    "scheduled %s misfired and was skipped",
                    job.job,
                )
                continue
            logger.info("scheduled %s fired", job.job)
            self._backend.create_run(job.id)

    @staticmethod
    def _is_misfired(
        next_run_at: datetime | None, now: datetime, grace: int | None
    ) -> bool:
        if grace is None or next_run_at is None:
            return False
        return (now - next_run_at) > timedelta(seconds=grace)

    def __repr__(self) -> str:
        return f"<JobManager backend={type(self._backend).__name__}>"
