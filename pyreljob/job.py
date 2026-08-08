"""Persisted state returned by the JobManager/Worker APIs.

``JobRecord`` is one row of the ``jobs`` table (a durable entity);
``RunRecord`` is one row of ``runs`` (an invocation of a job);
``TaskRecord`` is one row of ``tasks`` (a single task execution within a run).

These are plain data — the callable code lives in :mod:`pyreljob.task`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyreljob.models.orm import JobModel, RunModel, TaskModel


class JobStatus:
    ACTIVE = "active"
    CANCELLED = "cancelled"


class JobSource:
    ON_DEMAND = "on_demand"
    SCHEDULED = "scheduled"


class RunStatus:
    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus:
    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPENSATED = "compensated"


@dataclass
class JobRecord:
    id: int | None = None
    job: str = ""
    queue: str = "default"
    status: str = JobStatus.ACTIVE
    args: dict[str, Any] | None = None
    priority: int = 0
    source: str = JobSource.ON_DEMAND
    next_run_at: datetime | None = None
    max_attempts: int = 3
    retries: int = 0
    attempts: int = 0
    idempotency_key: str | None = None
    tags: list[str] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: JobModel) -> JobRecord:
        return cls(
            id=model.id,
            job=model.job,
            queue=model.queue,
            status=model.status,
            args=model.args,
            priority=model.priority,
            source=model.source,
            next_run_at=model.next_run_at,
            max_attempts=model.max_attempts,
            retries=model.retries,
            attempts=model.attempts,
            idempotency_key=model.idempotency_key,
            tags=model.tags,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )

    def __repr__(self) -> str:
        return f"<JobRecord id={self.id} job={self.job!r} status={self.status!r}>"


@dataclass
class RunRecord:
    id: int | None = None
    job_id: int = 0
    status: str = RunStatus.READY
    result: Any = None
    error: str | None = None
    ctx: dict[str, Any] | None = None
    worker_id: str | None = None
    progress: float | None = None
    scheduled_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @classmethod
    def from_model(cls, model: RunModel) -> RunRecord:
        return cls(
            id=model.id,
            job_id=model.job_id,
            status=model.status,
            result=model.result,
            error=model.error,
            ctx=model.ctx,
            worker_id=model.worker_id,
            progress=model.progress,
            scheduled_at=model.scheduled_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
            started_at=model.started_at,
            finished_at=model.finished_at,
        )

    @property
    def is_done(self) -> bool:
        return self.status in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED)

    def __repr__(self) -> str:
        return f"<RunRecord id={self.id} job={self.job_id} status={self.status!r}>"


@dataclass
class RunWithJob:
    """A run joined to its owning job.

    Returned by :meth:`JobManager.list_runs` so the UI can list runs directly
    (each is an actual execution) while still carrying the job's context
    (name, source, args, tags) for display and owner scoping.
    """

    run: RunRecord
    job: JobRecord


@dataclass
class TaskRecord:
    id: int | None = None
    run_id: int = 0
    position: int = 0
    task_name: str = ""
    status: str = TaskStatus.READY
    result: Any = None
    error: str | None = None
    attempts: int = 0
    retry_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    compensated_at: datetime | None = None

    @classmethod
    def from_model(cls, model: TaskModel) -> TaskRecord:
        return cls(
            id=model.id,
            run_id=model.run_id,
            position=model.position,
            task_name=model.task_name,
            status=model.status,
            result=model.result,
            error=model.error,
            attempts=model.attempts,
            retry_at=model.retry_at,
            created_at=model.created_at,
            updated_at=model.updated_at,
            started_at=model.started_at,
            finished_at=model.finished_at,
            compensated_at=model.compensated_at,
        )

    def __repr__(self) -> str:
        return (
            f"<TaskRecord run={self.run_id} pos={self.position} "
            f"name={self.task_name!r} status={self.status!r}>"
        )
