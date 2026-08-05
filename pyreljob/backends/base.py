"""Backend interface. Implementations abstract the database."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pyreljob.core.job import JobRecord, RunRecord, TaskRecord


class Backend(ABC):
    @abstractmethod
    def migrate(self) -> list[str]:
        """Create/upgrade schema. Returns descriptions of applied migrations."""

    # -- jobs (durable entities) ---------------------------------------------

    @abstractmethod
    def enqueue(
        self,
        job: str,
        args: Any = None,
        *,
        queue: str = "default",
        priority: int = 0,
        max_attempts: int = 3,
        idempotency_key: str | None = None,
        scheduled_at: datetime | None = None,
    ) -> JobRecord:
        """Create a durable job and its first run.

        With ``idempotency_key``, returns the existing job instead of creating
        a duplicate. ``scheduled_at`` delays the first run.
        """

    @abstractmethod
    def schedule(
        self,
        job: str,
        args: Any = None,
        cron: str | None = None,
        *,
        queue: str = "default",
        max_attempts: int = 3,
        next_run_at: datetime | None = None,
    ) -> JobRecord:
        """Register a maintained job. Idempotent on (job class, cron).

        The job fires a new run each time ``next_run_at`` comes due. ``cron``
        may be None for self-scheduling jobs that re-arm via ``Job.next_runtime``.
        """

    @abstractmethod
    def get(self, job_id: int) -> JobRecord | None: ...

    @abstractmethod
    def cancel_job(self, job_id: int) -> None:
        """Mark a job cancelled (stops future runs and its active run)."""

    @abstractmethod
    def is_job_cancelled(self, job_id: int) -> bool: ...

    @abstractmethod
    def delete_job(self, job_id: int) -> None:
        """Hard-delete a job and all its runs and tasks.

        Raises ValueError if the job has a pending or running run.
        """

    @abstractmethod
    def prune_runs(self, older_than: datetime) -> int:
        """Delete terminal runs finished before ``older_than`` (and their
        tasks). Returns the number of runs deleted."""

    @abstractmethod
    def list_scheduled(self) -> list[JobRecord]:
        """Return active maintained jobs (source=scheduled, not cancelled)."""

    @abstractmethod
    def claim_scheduled(self, job_id: int, next_run_at: datetime | None) -> bool:
        """Atomically claim a due maintained job and re-arm it.

        ``next_run_at`` is what the beat sets back (the next cron occurrence,
        or None for self-scheduling jobs awaiting worker reschedule). Returns
        True if this caller won the race and should fire it.
        """

    @abstractmethod
    def set_next_run_at(self, job_id: int, next_run_at: datetime | None) -> None:
        """Re-arm a job's next fire time (called by the worker after a run)."""

    # -- runs (invocations) --------------------------------------------------

    @abstractmethod
    def create_run(
        self,
        job_id: int,
        *,
        scheduled_at: datetime | None = None,
    ) -> RunRecord:
        """Create a new run of a job (an invocation)."""

    @abstractmethod
    def claim(
        self,
        worker_id: str,
        queue: str | None = None,
        *,
        lease_seconds: int = 30,
    ) -> RunRecord | None:
        """Atomically claim the next ready run, or None if none available.

        Reclaims ``running`` runs whose lease (``locked_at``) has expired.
        """

    @abstractmethod
    def renew_lease(self, run_id: int, worker_id: str) -> bool:
        """Refresh a run's lease. Returns False if the lease was lost."""

    @abstractmethod
    def complete_run(self, run_id: int, result: Any = None) -> None: ...

    @abstractmethod
    def fail_run(
        self,
        run_id: int,
        error: str,
        *,
        retry_at: datetime | None = None,
        attempts: int = 1,
    ) -> None: ...

    @abstractmethod
    def cancel_run(self, run_id: int) -> None: ...

    @abstractmethod
    def set_run_ctx(self, run_id: int, ctx: dict[str, Any] | None) -> None:
        """Persist the shared TaskContext between tasks (resumability)."""

    @abstractmethod
    def get_run(self, run_id: int) -> RunRecord | None: ...

    @abstractmethod
    def runs(self, job_id: int) -> list[RunRecord]:
        """All runs of a job, newest first."""

    @abstractmethod
    def recent_failures(self, limit: int = 10) -> list[RunRecord]:
        """The most recently failed runs, newest first."""

    @abstractmethod
    def counts(self, queue: str | None = None) -> dict[str, int]:
        """Run counts by status."""

    # -- tasks (task executions within a run) ------------------------------------

    @abstractmethod
    def tasks(self, run_id: int) -> list[TaskRecord]:
        """All task records for a run, ordered by position."""

    @abstractmethod
    def start_task(
        self,
        run_id: int,
        position: int,
        task_name: str,
        *,
        attempts: int = 1,
    ) -> None:
        """Mark a task running (creates it if absent, resets it on retry)."""

    @abstractmethod
    def succeed_task(self, run_id: int, position: int, result: Any = None) -> None: ...

    @abstractmethod
    def fail_task(
        self,
        run_id: int,
        position: int,
        error: str,
        *,
        attempts: int = 1,
        retry_at: datetime | None = None,
    ) -> None: ...

    @abstractmethod
    def cancel_task(self, run_id: int, position: int) -> None: ...

    @abstractmethod
    def compensate_task(self, run_id: int, position: int) -> None: ...
