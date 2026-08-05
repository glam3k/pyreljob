"""The user-facing Task/Job base classes.

Tasks are the atomic unit of work — stateless classes with ``run(ctx)`` and
optional ``undo(ctx)``. A **Job** is the durable thing users define: a
``@dataclass`` (fields = parameters) whose class-level ``tasks`` list declares
the ordered tasks. A **Run** is one invocation of a job; the worker executes
the job's ``tasks`` in order for each run. Jobs needing behavior or
non-serialized attributes can be plain classes that override ``as_dict`` /
``from_dict``.

Example::

    from dataclasses import dataclass
    from pyreljob import JobManager, Job, Task, TaskContext

    class ReserveSlot(Task):
        async def run(self, ctx: TaskContext) -> str:
            return reserve(ctx.args["slot"])

        async def undo(self, ctx: TaskContext) -> None:
            release(ctx.args["slot"])

    class Charge(Task):
        async def run(self, ctx: TaskContext) -> None:
            charge(ctx.args["user"], ctx.result("ReserveSlot"))

    @dataclass
    class Booking(Job):
        user: str
        slot: str
        tasks = [ReserveSlot, Charge]

    manager = JobManager("sqlite:///jobs.db")
    manager.enqueue(Booking("u-1", "slot-1"), idempotency_key="booking-u1")
"""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, ClassVar

from croniter import croniter

from pyreljob.core.job import RunRecord, TaskRecord


class JobCancelledError(Exception):
    """Raised by a task to abort cooperatively when cancellation is requested."""


class TaskContext:
    """Shared, durable state flowing through every task execution of a run.

    Attributes:
        job_id: the id of the job being run.
        job: the dotted job class path.
        args: the job's serialized constructor arguments.
        results: per-task results, keyed by task class name.
        state: arbitrary task-shared mutable state (persisted between tasks).
        cancelled: cooperative cancellation flag (set by the worker heartbeat).
    """

    def __init__(
        self,
        job_id: int,
        job: str,
        args: dict[str, Any] | None = None,
        results: dict[str, Any] | None = None,
        state: dict[str, Any] | None = None,
        cancelled: bool = False,
    ) -> None:
        self.job_id = job_id
        self.job = job
        self.args = args or {}
        self.results = results or {}
        self.state = state or {}
        self.cancelled = cancelled

    def check_cancelled(self) -> None:
        """Abort cooperatively: raise :class:`JobCancelledError` if cancelled."""
        if self.cancelled:
            raise JobCancelledError("job cancelled")

    def result(self, task_name: str) -> Any:
        """The result produced by a previously succeeded task (by class name)."""
        return self.results.get(task_name)

    def as_dict(self) -> dict[str, Any]:
        """Serialize to the ``runs.ctx`` column (excludes the live cancel flag)."""
        return {
            "job_id": self.job_id,
            "job": self.job,
            "args": self.args,
            "results": self.results,
            "state": self.state,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskContext:
        return cls(
            job_id=int(data.get("job_id") or 0),
            job=data.get("job") or "",
            args=data.get("args") or {},
            results=data.get("results") or {},
            state=data.get("state") or {},
        )


class Task(ABC):
    """An atomic unit of work. Instantiated per task; stateless by design.

    ``run``/``undo`` are ``async def`` — the worker is asyncio-native, so each
    task runs as a coroutine on the event loop.
    """

    #: Optional human-friendly name; defaults to the dotted class path. Used
    #: for observability (stored in ``tasks.task_name``). Resolution back to
    #: the class is by position, so a custom name needs no registry.
    name: ClassVar[str | None] = None
    #: Optional watchdog timeout in seconds; the task is failed if exceeded.
    #: The retry budget is a framework-level ``max_attempts`` set per job.
    timeout: int | None = None

    @abstractmethod
    async def run(self, ctx: TaskContext) -> Any:
        """Perform the work. Receives everything it needs through ``ctx``."""

    async def undo(self, ctx: TaskContext) -> None:
        """Compensation, invoked in reverse order when a run fails
        (or manually via ``JobManager.undo``)."""

    @classmethod
    def task_name(cls) -> str:
        """The name stored for this task: a custom ``name`` if set, else the
        dotted class path."""
        if cls.name:
            return cls.name
        return f"{cls.__module__}.{cls.__qualname__}"


class Job(ABC):
    """A durable job definition — the ordered series of :class:`Task` classes.

    The class-level ``tasks`` list declares the task order. Subclasses are
    usually ``@dataclass`` — the dataclass fields become the job's serialized
    ``args`` with zero boilerplate. Jobs needing non-serialized attributes or
    custom parameter handling can be plain classes that override
    :meth:`as_dict` and :meth:`from_dict`.

    A job also owns its schedule: set ``cron`` for a fixed wall-clock
    schedule, or override :meth:`next_runtime` to self-schedule based on when
    each run started, how long it took, its result, or business logic.
    """

    tasks: ClassVar[list[type[Task]]] = []
    queue: ClassVar[str] = "default"
    priority: ClassVar[int] = 0
    #: Optional fixed cron schedule, e.g. ``"0 2 * * *"``. Used by the base
    #: :meth:`next_runtime`; ``None`` means the job only runs when enqueued or
    #: when an overridden :meth:`next_runtime` re-arms it.
    cron: ClassVar[str | None] = None

    def next_runtime(
        self,
        last_run: RunRecord,
        ctx: TaskContext,
    ) -> datetime | None:
        """When should this job run next?

        Called after a run finishes. The base implementation returns the next
        cron tick after the run finished (if ``cron`` is set), otherwise
        ``None`` (run once). Override to self-schedule — e.g. from
        ``last_run.started_at`` / ``finished_at`` (duration), the run's
        ``result``, or business logic in ``ctx``. Return ``None`` to stop.
        """
        if self.cron:
            base = last_run.finished_at or datetime.now()
            result = croniter(self.cron, base).get_next(datetime)
            return result if isinstance(result, datetime) else None
        return None

    def as_dict(self) -> dict[str, Any]:
        """Serialize the job's parameters.

        The default works for any ``@dataclass`` subclass (fields = params).
        Plain classes must override this.
        """
        if not is_dataclass(self):
            raise TypeError(
                f"{type(self).__name__} must be a @dataclass or implement "
                "as_dict()/from_dict()"
            )
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        """Reconstruct a job from its serialized parameters.

        The default works for ``@dataclass`` subclasses. Plain classes must
        override this.
        """
        return cls(**data)


def job_name(job: Job | type[Job]) -> str:
    """The dotted path used to store and resolve a job class."""
    cls = job if isinstance(job, type) else type(job)
    return f"{cls.__module__}.{cls.__qualname__}"


def validate_job(job: Any) -> None:
    if not isinstance(job, Job):
        raise TypeError(f"expected a Job instance, got {type(job).__name__}")
    if not job.tasks:
        raise ValueError(f"{type(job).__name__}.tasks must declare at least one Task")
    try:
        job.as_dict()
    except (TypeError, NotImplementedError) as exc:
        raise TypeError(
            f"{type(job).__name__} must be serializable: either a @dataclass "
            "or implement as_dict()/from_dict()"
        ) from exc


def _resolve(name: str, base: type) -> type | None:
    """Resolve a dotted class path to a subclass of ``base``."""
    parts = name.split(".")
    for i in range(len(parts) - 1, 0, -1):
        try:
            obj: Any = importlib.import_module(".".join(parts[:i]))
        except ImportError:
            continue
        try:
            for attr in parts[i:]:
                obj = getattr(obj, attr)
        except AttributeError:
            return None
        if isinstance(obj, type) and issubclass(obj, base):
            return obj
        return None
    return None


def resolve_job(name: str) -> type[Job] | None:
    """Resolve a dotted job-class path, e.g. ``app.jobs.Booking``."""
    return _resolve(name, Job)


def resolve_task(name: str) -> type[Task] | None:
    """Resolve a dotted task-class path, e.g. ``app.tasks.Charge``."""
    return _resolve(name, Task)


def task_for_record(record: TaskRecord, job_cls: type[Job]) -> type[Task]:
    """Map a persisted task execution back to the Task class declared in its job."""
    position = record.position
    if 0 <= position < len(job_cls.tasks):
        return job_cls.tasks[position]
    raise ValueError(
        f"task position {position} out of range for job {job_name(job_cls)}"
    )
