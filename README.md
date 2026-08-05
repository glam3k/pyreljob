# pyreljob

A portable, relational-table-backed **job/workflow framework** for Python —
durable jobs, not a distributed task queue.

`pyreljob` sits between heavy systems like Temporal and in-process schedulers
like APScheduler. Work is defined as **Task → Job → Run**: tasks are the
atomic unit, a **Job is the durable entity you define** (a `@dataclass` whose
`tasks` list declares the ordered tasks), and a **Run is one invocation of a
job**. Everything is stored in a SQL table (SQLite or PostgreSQL) with a
built-in migration framework. Import it in any project.

See [`DESIGN.md`](DESIGN.md) for the full architecture and semantics.

## Features

- **SQL-backed** — SQLite for dev, PostgreSQL for production; same code path
- **Durable jobs, one run at a time** — a Job is the thing you reference; each run is an invocation
- **Framework-level retries** — every task is retried up to the job's `max_attempts` (backoff + jitter); completed tasks never re-run
- **Resumable** — task state is durable; a worker crash resumes at the next unfinished task
- **Compensation** — when a task exhausts its retries, completed tasks are undone in reverse order
- **On-demand & maintained** — one-off jobs, or cron-scheduled jobs that fire a new run per schedule
- **Cooperative cancellation** — cancel a job; its run aborts at the next `check_cancelled()`
- **Idempotency keys** — dedupe re-enqueues (protects the at-least-once guarantee)
- **Task timeouts** — a task that exceeds its `timeout` is failed and retried/compensated
- **Crash-safe leases** — a dead worker's run is re-claimed after its lease expires
- **Migration framework** — versioned, idempotent schema migrations built in
- **Portable & importable** — a pip-installable library you import; same code works on SQLite and PostgreSQL

## Install

```bash
pip install pyreljob                # sqlite works out of the box
pip install 'pyreljob[postgres]'    # + psycopg for PostgreSQL
```

## Quick start

```python
from dataclasses import dataclass
from typing import ClassVar

from pyreljob import Job, JobManager, Task, TaskContext

class SendEmail(Task):
    def run(self, ctx: TaskContext) -> None:
        send(ctx.args["to"], ctx.args.get("subject", "hi"))

@dataclass
class Notification(Job):        # define a job: a @dataclass + ordered tasks
    to: str
    tasks: ClassVar = [SendEmail]

manager = JobManager("sqlite:///jobs.db")   # or postgresql://...
manager.migrate()

job = manager.enqueue(Notification("x@y.z"))                       # durable job + first run
manager.enqueue(Notification("x@y.z"), idempotency_key="n1")        # dedupe key
manager.enqueue(Notification("x@y.z"), max_attempts=5)              # per-task retry count
```

Run a worker — the framework is a library, so a worker is just a small entry
point that runs in any process that has your code installed:

```python
# worker.py
from pyreljob.backends.sqlalchemy_backend import backend_from_url
from pyreljob.core.worker import Worker

Worker(backend_from_url("sqlite:///jobs.db")).run_forever()
```

Workers poll the database, claim runs with a lease, execute the job's tasks,
and renew the lease via a heartbeat. Run as many as you like against the same
database.

## How-to guide

**1. Define a task** — a stateless class with `run(ctx)`:

```python
from pyreljob import Task, TaskContext

class SendEmail(Task):
    def run(self, ctx: TaskContext) -> None:
        send(ctx.args["to"], ctx.args.get("subject", "hi"))
```

**2. Compose tasks into a job** — a `@dataclass` whose fields are the job's
parameters and whose `tasks` list declares the execution order:

```python
from dataclasses import dataclass
from typing import ClassVar
from pyreljob import Job

@dataclass
class Notification(Job):
    to: str
    tasks: ClassVar = [SendEmail]
```

Put your job/task classes in a module that both your app and workers can
import (e.g. `app/jobs.py`).

**3. Create a manager and enqueue:**

```python
from pyreljob import JobManager

manager = JobManager("sqlite:///jobs.db")   # or postgresql://...
manager.migrate()                            # once per database
manager.enqueue(Notification("x@y.z"))
```

**4. Run a worker** — any process that can import your job code:

```python
from pyreljob.backends.sqlalchemy_backend import backend_from_url
from pyreljob.core.worker import Worker

Worker(backend_from_url("sqlite:///jobs.db")).run_forever()
```

**5. Schedule a recurring job** — fire it with the manager's beat loop:

```python
manager.schedule(Notification("daily@x.yz"), "0 2 * * *")
manager.run_forever()   # the beat; run this as its own process if you prefer
```

**6. Operate a job:**

```python
manager.runs(job_id)          # a job's runs
manager.tasks(run_id)         # a run's task executions
manager.cancel(job_id)        # cooperative stop (no compensation)
manager.undo(job_id)          # manual saga compensation
manager.counts()              # run counts by status
manager.recent_failures()     # most recently failed runs
```

## Priority

Priority is **data of the job** — each job type declares it as a class
attribute (higher wins; workers claim highest-priority runs first):

```python
@dataclass
class Billing(Job):
    priority = 50
    tasks: ClassVar = [Charge]

@dataclass
class Analytics(Job):
    priority = 10
    tasks: ClassVar = [Aggregate]
```

An optional per-enqueue override handles the case where one instance of a job
type needs a different priority:

```python
manager.enqueue(Billing("u-1"), priority=100)   # bump just this run
```

Priority is a property of the job (and each run it creates) — it is not part
of a cron schedule.

## Composing jobs from tasks

Tasks are stateless classes; everything flows through the shared `ctx`. Compose
new jobs by writing new `Task` classes and putting them in a job's `tasks`
list:

```python
class Reserve(Task):
    def run(self, ctx: TaskContext) -> str:
        return reserve(ctx.args["slot"])          # result stored under "Reserve"

    def undo(self, ctx: TaskContext) -> None:
        release(ctx.args["slot"])

class Charge(Task):
    def run(self, ctx: TaskContext) -> None:
        charge(ctx.args["user"], ctx.result("Reserve"))

@dataclass
class Booking(Job):
    user: str
    slot: str
    tasks: ClassVar = [Reserve, Charge]

job = manager.enqueue(Booking("u-1", "slot-1"))
manager.runs(job.id)     # the job's invocations (each execution is a Run)
```

If a task fails and exhausts the job's retry budget, the framework calls
`undo()` on the completed tasks **in reverse order** and marks the run
`failed`. You can also compensate a succeeded run manually (saga pattern):

```python
manager.undo(job_id)
```

## Maintained (periodic) jobs

### Fixed schedule (cron)

```python
manager.schedule(Notification("daily@x.yz"), "0 2 * * *")
manager.run_forever()   # the beat fires due cron runs
```

### Self-scheduling (`Job.next_runtime`)

Every job owns how often it runs. Override `next_runtime(last_run, ctx)` to
compute the next run from when the previous run started, how long it took, its
result, or business logic — then register the job without a cron:

```python
from datetime import datetime, timedelta
from pyreljob import Job, JobManager, Task, TaskContext
from pyreljob.core.job import RunRecord

@dataclass
class Poller(Job):
    tasks: ClassVar = [Fetch]

    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        return last_run.finished_at + timedelta(minutes=5)   # 5 min after it finished
```

```python
manager.schedule(Poller())     # fires now, then re-arms itself every run
```

The worker calls `next_runtime` after each run finishes and re-arms the job;
return `None` to stop. The base implementation uses `cron` (a class
attribute) if set, otherwise the job runs once. Note that completion-based
scheduling is serial — the next run only fires after the previous one
finishes, so a slow run shifts the cadence.

A maintained job is one durable `jobs` row; every fire creates a fresh run.
Misfire handling: a cron run overdue by more than `misfire_grace_seconds` is
skipped (coalesced); runs missed while the scheduler was down are collapsed
into one.

## Plain (non-dataclass) jobs

`@dataclass` is the zero-boilerplate way to declare a job's parameters. If a
job needs behavior or non-serialized attributes, make it a plain class that
implements `as_dict()` / `from_dict()`:

```python
class Booking(Job):
    tasks: ClassVar = [Reserve, Charge]

    def __init__(self, user: str, slot: str) -> None:
        self.user = user
        self.slot = slot
        self._db = connect()            # transient, never serialized

    def as_dict(self) -> dict:
        return {"user": self.user, "slot": self.slot}

    @classmethod
    def from_dict(cls, data: dict) -> "Booking":
        return cls(data["user"], data["slot"])
```

At runtime the Job object itself carries no state — durable state lives in the
run's `ctx` and task records.

## Cancellation

Cancellation is cooperative — the worker flags the live `ctx`; call
`check_cancelled()` between tasks:

```python
class BigImport(Task):
    def run(self, ctx: TaskContext) -> None:
        for chunk in fetch():
            ctx.check_cancelled()   # raises JobCancelledError if cancelled
            insert(chunk)
```

```python
manager.cancel(job_id)   # stop only — never compensates
```

## Timeouts

```python
class Slow(Task):
    timeout = 30          # seconds; exceeded -> failed, then retried

    def run(self, ctx: TaskContext) -> None: ...
```

A hung task cannot be force-killed in-process; it is marked failed and the
framework moves on (true isolation is a v2 concern).

## Retries & crash safety

Every task in a job is retried up to the job's `max_attempts` (default 3) with
exponential backoff + jitter (`2^attempt + random` seconds). Task state is
durable in the `tasks` table, and `ctx` is persisted after every task, so a
worker that dies mid-run is re-claimed by another worker which **resumes at the
next unfinished task** — completed work is never repeated.

## Inspecting failures

```python
manager.counts()                 # run counts by status
manager.recent_failures()        # failed runs
manager.runs(job_id)             # a job's runs
manager.tasks(run_id)            # per-task state of a run
```

There is no separate "replay" concept: retries are automatic, and a terminal
failure is re-run by creating a new job (or the next scheduled fire).

## Development

```bash
pip install -e '.[dev]'
make check        # lint + typecheck + all tests
make unit-test    # SQLite tests
make e2e          # PostgreSQL integration tests
make lint         # ruff
make typecheck    # mypy (strict)
```

Integration tests against real PostgreSQL:

```bash
docker compose up -d postgres
TEST_DATABASE_URL=postgresql+psycopg://pyreljob:pyreljob@localhost:5433/pyreljob pytest
```

## Architecture

```
pyreljob/
├── backends/          # Backend ABC + SQLAlchemy backend (sqlite & postgres)
├── core/              # records, JobManager API (client + beat), Worker
├── migrations/        # versioned migration runner + schema steps (v1-v4)
├── task.py            # user-facing Task/Job/TaskContext base classes
└── signals.py         # graceful SIGINT/SIGTERM shutdown helpers
```

The database — not `create_all` — is the source of truth: all schema changes go
through the versioned migration runner, and the ORM mirrors the result.
