# pyreljob design

pyreljob is a portable, relational-table-backed **job/workflow framework** for
Python — durable jobs, not a distributed task queue. It sits between heavy
systems like Temporal and in-process schedulers like APScheduler. This
document is the contract for the v1 implementation.

## Mental model

The model follows the workflow-engine convention of **durable entity vs. run**
(Temporal Workflow/Execution, Prefect Flow/Flow Run, Airflow DAG/DAG Run):

| Concept | Meaning | Persisted as |
|---|---|---|
| **Task** | The most atomic unit of work. Stateless code with `run(ctx)`; optional `undo(ctx)` compensation. | `tasks` row (one per task execution per run) |
| **Job** | The durable entity users define: a `@dataclass` whose class-level `tasks` list declares the ordered tasks. | `jobs` row (one per job) |
| **Run** | One invocation of a job — the ordered execution of its tasks. No class; just a record. | `runs` row (one per run) |

A **Job** is the thing you reference over time: it owns the input (`args`),
the retry policy, the idempotency key, and — for maintained jobs — the cron
schedule. Every time the job executes, that execution is a **Run**. There is no
separate "chain" abstraction: the ordered task list lives on the Job itself.

## Abstractions

```python
class Task(ABC):
    timeout: int | None = None      # optional watchdog, in seconds

    def run(self, ctx: TaskContext) -> Any: ...
    def undo(self, ctx: TaskContext) -> None: ...

@dataclass
class Job(ABC):
    tasks: ClassVar[list[type[Task]]]   # ordered, strictly linear
    queue: ClassVar[str] = "default"
    priority: ClassVar[int] = 0

    def as_dict(self) -> dict: ...      # dataclass asdict (override for plain classes)
    @classmethod
    def from_dict(cls, data) -> Job: ...
```

- Jobs are usually `@dataclass` (constructor args = the serialized `args`,
  stored on the job) — the zero-boilerplate parameter declaration. Jobs needing
  behavior or non-serialized attributes can be plain classes that override
  `as_dict()` / `from_dict()`. Tasks are instantiated per task as
  `TaskClass()` and receive all inputs through `ctx` — they are stateless code.
- The Job object itself carries no runtime state: durable state lives in the
  run's `ctx` and task records, exactly as in Prefect/Temporal.
- `TaskContext` carries `job_id`, `job` (dotted class path), `args`,
  `results` (per-task results keyed by task class name), `state` (arbitrary
  task-shared mutable state) and a cooperative `cancelled` flag
  (`ctx.check_cancelled()` raises `JobCancelledError`).

## Schema

```
jobs   entities:   id, job (dotted class), args, queue, priority,
                   status(active|cancelled), source(on_demand|scheduled),
                   cron, next_run_at, max_attempts, idempotency_key (unique),
                   created_at
runs   invocations: id, job_id, status(pending|running|succeeded|failed|
                   cancelled), result, error, ctx, worker_id, scheduled_at,
                   created_at, started_at, finished_at, locked_at (lease)
tasks  task execs:  id, run_id, position, task_name, status, result, error,
                   attempts, retry_at, started_at, finished_at, compensated_at
```

## Manager API

```python
manager = JobManager("sqlite:///jobs.db")      # or postgresql://...
manager.migrate()
job = manager.enqueue(Booking("u-1", 99), idempotency_key="booking-u1")
job = manager.enqueue(Booking("u-1", 99), max_attempts=5)   # retry count per task
manager.schedule(Booking("u-1", 99), "0 2 * * *")           # maintained job
manager.run_forever()               # beat: fire due cron (same manager)
manager.cancel(job_id)      # stop only, no compensation
manager.undo(job_id)        # manual saga: reverse-compensate a run's tasks
manager.runs(job_id)        # the job's invocations, newest first
manager.tasks(run_id)       # a run's task executions
manager.get(job_id)
manager.counts(); manager.recent_failures()
```

## Components

Two concepts, mirroring how the system is deployed (three physical roles,
connected only by the database):

```
Producers (your app) ── JobManager: enqueue / schedule / cancel / query
                                 │
                                 ▼
                            Database        ← the only shared state
                                 ▲
Workers (N processes) ──────────┘   claim + lease + execute tasks
Manager beat ───────────────────►   run_forever() fires cron → creates runs
```

- **`JobManager`** — the control plane: client API *and* the beat loop
  (`run_forever()` fires due cron). Used by producer processes and, in beat
  mode, as a long-running process.
- **`Worker`** — the executor: claims runs with a lease, heartbeats, executes
  the job's tasks. N processes scale horizontally.

## Execution semantics

- **Retries are framework-level, per job**: every task in a job is retried up
  to the job's `max_attempts` (default 3, tunable at enqueue/schedule time)
  with exponential backoff + jitter. A task that keeps failing exhausts the
  budget and the run compensates.
- **Resumable checkpoints**: `tasks` are durable. If a worker dies, the next
  claim resumes the run at the first non-succeeded task — earlier work is never
  repeated. `ctx` is persisted after every task.
- **Compensation**: when a task exhausts its retries, the framework runs
  `undo(ctx)` on completed tasks **in reverse order**, then marks the run
  `failed`. Cancellation is **stop only** — it never compensates.
- **At-least-once + idempotency keys**: SQL-backed claims are at-least-once,
  so an optional unique `idempotency_key` on `enqueue` dedupes re-enqueues
  (re-enqueue returns the existing job). Tasks should be written idempotently.
- **Timeouts**: a task whose `run()` exceeds `timeout` is marked failed and
  goes through normal retry/compensation. The task runs in a watchdog thread;
  a truly hung task cannot be force-killed in-process — the framework marks it
  failed and moves on (worker process isolation would be a v2 concern).
- **Cooperative cancellation**: `cancel(job_id)` marks the job cancelled,
  cancels its active run, and (for maintained jobs) stops future fires. The
  worker's heartbeat sets `ctx.cancelled` on the live run and the task aborts
  at its next `check_cancelled()`.
- **Maintained jobs**: `schedule()` registers one `jobs` row. The manager beat
  (`run_forever()`/`tick()`) fires when a job's `next_run_at` is due,
  atomically re-arming it (concurrent beats never double-fire). Every fire
  creates a fresh run of the job. Misfire grace applies to cron jobs.
- **Self-scheduling**: every job owns its cadence via
  `Job.next_runtime(last_run, ctx)`. The worker calls it after each run
  finishes and re-arms `next_run_at`, so a job can schedule its next run from
  when the run started, how long it took, its result, or business logic.
  Return `None` to stop. The base implementation uses the `cron` class
  attribute (next tick after the run finished) or `None` (run once).
  Completion-based scheduling is serial — the next run fires only after the
  previous one finishes.

## Durable guarantees

- Claims are atomic conditional UPDATEs (SQLite/Postgres portable), gated by
  `scheduled_at` for delayed/retried runs and by lease (`locked_at`) expiry
  for crash recovery.
- Heartbeats renew the lease (`locked_at`) every `lease_seconds/3`; a dead
  worker's lease expires and another worker re-claims the run.
- There is no separate "replay" concept: retries are automatic within a run's
  budget, and a terminal `failed` outcome is re-run by creating a new job (or,
  for maintained jobs, the next scheduled fire).

## Replica safety

Every component is safe to run with replicas — the database is the only shared
state and every transition is an atomic conditional UPDATE:

- **Workers (N processes)**: `claim()` is an atomic conditional UPDATE
  (`FOR UPDATE SKIP LOCKED` on Postgres) so two workers never execute the same
  run; leases + heartbeat handle crashes and re-claim.
- **Manager — client**: `enqueue`/`schedule`/`cancel`/`undo` are idempotent
  (idempotency keys dedupe; `schedule`/`cancel` are conditional), so any number
  of app processes can call them concurrently.
- **Manager — beat (N replicas)**: `claim_scheduled()` atomically advances
  `next_run_at`, so only one beat wins each cron fire — no double-firing.

## Deployment

pyreljob is an **embedded library** — the model that Celery, Dramatiq, RQ, and
arq all use:

- Import it in your app; producers call `JobManager.enqueue(...)` in-process.
- Workers and the manager beat are separate processes you deploy (e.g. Docker
  images running your code): `Worker(backend).run_forever()` and
  `manager.run_forever()`.
- The **database is the network boundary** — any process that can reach the DB
  can enqueue (via `JobManager` or raw SQL), so "enqueue over the network"
  needs no framework API.

## Future work (not yet built)

If producers can't reach the DB — other languages, services you won't give DB
credentials to, or you need authz/rate-limiting/audit — the natural extension
is a thin HTTP/REST service over `JobManager`. Every operation already exists;
the API is an adapter, not a new architecture:

```
POST /jobs                            -> enqueue
GET  /jobs/{id}                       -> get
POST /jobs/{id}/cancel                -> cancel
POST /jobs/{id}/undo                  -> undo
GET  /jobs/{id}/runs                  -> runs
GET  /jobs/{id}/runs/{run_id}/tasks   -> tasks
GET  /counts                          -> counts
GET  /failures                        -> recent_failures
```

HTTP/REST first (simple, portable); gRPC only if high-throughput typed
contracts across many languages are ever needed. The core library is unchanged.

Other planned work:
- Worker concurrency: N run slots per process + batch claim.
- Observability: run history, metrics, failure/alert hooks.
- Hardening: chaos/race tests, whole-run deadline, at-least-once contract.
- v2: true task isolation (subprocess) so a hung task can be killed.

## Reference points

- **Queue mechanics** (claims, leases, backoff): Graphile-worker, GoodJob.
- **Entity/run model** (durable job, invocations): Temporal (Workflow /
  Workflow Execution), Prefect (Flow / Flow Run).
- **Composition**: Prefect's task composition is the closest Python analog to
  `Job.tasks`.
