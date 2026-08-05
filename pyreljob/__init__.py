"""pyreljob — a portable, relational-table-backed job framework for Python.

Backed by SQLite or PostgreSQL. Ships with a built-in migration framework.
"""

from pyreljob.core.job import (
    JobRecord,
    JobSource,
    JobStatus,
    RunRecord,
    RunStatus,
    TaskRecord,
    TaskStatus,
)
from pyreljob.core.manager import JobManager
from pyreljob.task import (
    Job,
    JobCancelledError,
    Task,
    TaskContext,
    job_name,
    resolve_job,
    resolve_task,
)

__version__ = "0.3.0"

__all__ = [
    "Job",
    "JobCancelledError",
    "JobManager",
    "JobRecord",
    "JobSource",
    "JobStatus",
    "RunRecord",
    "RunStatus",
    "Task",
    "TaskContext",
    "TaskRecord",
    "TaskStatus",
    "__version__",
    "job_name",
    "resolve_job",
    "resolve_task",
]
