# pyreljob architecture — review guide

A companion to [`DESIGN.md`](DESIGN.md): this walks the actual code — package
layout, every file, why it exists, and the invariants a reviewer should check.
The conceptual contract (model, semantics, replica safety, deployment) lives in
DESIGN.md; this is the "where is everything and why" map.

## High-level architecture

Three physical roles, connected only by the database:

```
Producers (your app) ── JobManager: enqueue/schedule/cancel/query
                              │
                              ▼
                         Database (jobs, runs, tasks)   ← the only shared state
                              ▲
Workers (N processes) ────────┘   claim + lease + execute + reschedule
Manager beat ─────────────────►   run_forever() fires due cron/self-scheduled
```

Everything is a library — no server, no CLI. The **database is the network
boundary**: any process that can reach the DB can enqueue.

## Layering (dependency direction)

```
 task.py             user-facing API: Task, Job, TaskContext        ◄─ import here
   │
 core/manager.py     control plane: client + beat
 core/worker.py      executor
   │                          ▲
   ▼                          │
 backends/base.py     Backend ABC (the portability seam)
 backends/sqlalchemy_backend.py   SQLAlchemyBackend/SQLiteBackend/PostgresBackend
   │
   ▼
 migrations/runner.py  versioned migrations      orm.py  SQLAlchemy models
 migrations/versions.py (DDL v1–v4)
   │
   ▼
 core/job.py   records (JobRecord/RunRecord/StepRecord) + statuses   ◄─ shared types
```

Dependency rule: **everything converges on `core/job.py` (records) and the
`Backend` ABC.** `manager`/`worker` never touch SQLAlchemy or the ORM directly
— they talk only to `Backend`. `task.py` never touches the backend or ORM.
`orm.py` is leaf-most.

## Package layout

```
pyreljob/
├── __init__.py              public API re-exports
├── py.typed                 PEP 561: this package ships type hints
├── signals.py               SIGINT/SIGTERM graceful-shutdown helper
├── task.py                  user-facing Task / Job / TaskContext
├── orm.py                   SQLAlchemy models mirroring the current schema
├── core/
│   ├── job.py               plain-data records + status constants
│   ├── manager.py           JobManager (client + beat)
│   └── worker.py            Worker (executor)
├── backends/
│   ├── base.py              Backend ABC
│   └── sqlalchemy_backend.py  shared impl + SQLite/Postgres subclasses + factory
└── migrations/
    ├── runner.py            versioned migration runner
    └── versions.py          the schema steps (v1–v4)
```

## File by file

### `task.py` — the user-facing contract
What users import and subclass. Holds **no I/O, no DB**.
- `Task` — atomic unit: `run(ctx)`, optional `undo(ctx)`, `timeout`.
- `Job` — durable entity: `tasks` (ordered), `queue`, `priority`, `cron`,
  `as_dict`/`from_dict` (dataclass by default, overridable), and
  `next_runtime(last_run, ctx)` (self-scheduling; base uses `cron`).
- `TaskContext` — shared durable state (`args`, `results`, `state`,
  cooperative `cancelled`); serialized to the run's `ctx` column.
- `JobCancelledError`, `job_name`, `task_name`, `validate_job`, `resolve_job`,
  `resolve_task`, `task_for_record` — dotted-path naming/resolution helpers.
Why needed: it's the surface the framework is judged on; keeping it I/O-free is
what keeps the library portable and testable.

### `core/job.py` — records + statuses
Plain dataclasses mirroring rows (`JobRecord`, `RunRecord`, `StepRecord`) with
`from_model()` converters, plus the status enums (`JobStatus`, `JobSource`,
`RunStatus`, `StepStatus`). The **only shared vocabulary** between
manager/worker/backend. Why needed: a stable, typed intermediate so the
backend can be swapped.

### `core/manager.py` — `JobManager`
Control plane. Two modes:
- **Client**: `enqueue`, `schedule`, `cancel`, `undo`, `get`, `runs`, `tasks`,
  `counts`, `recent_failures`, `migrate`.
- **Beat**: `run_forever`/`tick`/`stop` — fires due maintained jobs
  (`claim_scheduled` + `create_run`), cron misfire grace, graceful shutdown.
Why needed: the single public entry point; separates "manage" from "execute"
(worker). It never touches SQLAlchemy — only `Backend`.

### `core/worker.py` — `Worker`
The executor. Responsibilities:
- `claim()` (lease via `locked_at`) → execute the job's tasks in order.
- Per-job retries: `max_attempts`, exponential backoff via `run.scheduled_at`.
- Resumable checkpoints: resumes at the first non-succeeded task; persists
  `ctx` after every task.
- Compensation: reverse `undo()` of succeeded tasks on exhaustion.
- Cooperative cancellation via heartbeat → `ctx.cancelled`.
- Timeout watchdog (daemon thread; a hung task can't be killed in-process).
- **Rescheduling**: after a terminal run, calls `Job.next_runtime(last_run,
  ctx)` and re-arms `jobs.next_run_at`.
- Dotted-path class resolution with a registry cache.
Why needed: the only place job code runs; it's where "at-least-once" is made
real.

### `backends/base.py` — `Backend` ABC
The portability seam: every operation the manager/worker need, declared
abstract. Why needed: makes SQLite/Postgres (and future backends) drop-in and
lets tests fake the DB.

### `backends/sqlalchemy_backend.py` — the one real backend
- `SQLAlchemyBackend` — all shared logic (enqueue, schedule, claims, leases,
  tasks, compensation, counts, idempotency-dedupe).
- `SQLiteBackend` — inherits the portable conditional-UPDATE claim.
- `PostgresBackend` — overrides `claim` with `SELECT ... FOR UPDATE SKIP
  LOCKED` (Graphile-worker style); home for future PG-only features
  (LISTEN/NOTIFY).
- `backend_from_url(url, **engine_kwargs)` — factory: `pool_pre_ping`, SQLite
  `check_same_thread=False`, picks the subclass from the URL scheme.
Why needed: SQLAlchemy is the portability layer; the dialect subclasses keep
the genuinely different bits (claims) isolated instead of `if dialect:` spread
through one class.

### `orm.py` — SQLAlchemy models
`JobModel`, `RunModel`, `StepModel` **mirror** the current schema. Why needed:
a typed query surface for the backend. **Not** the source of truth — the
migrations are; `create_all` is deliberately never called.

### `migrations/runner.py` — the migration framework
`MigrationRunner` + `Migration`: applies ordered, versioned steps exactly once,
tracked in `schema_migrations`. Why needed: schema changes are append-only and
portable — the framework's own "production-grade" story.

### `migrations/versions.py` — the schema steps (v1–v4)
- v1–v3: original single `jobs` table + lease column (legacy).
- v4: reshapes `jobs` into durable entities and adds `runs` + `tasks`; guarded
  renames/drops/backfills; DDL is per-dialect (`SERIAL` vs `INTEGER PRIMARY
  KEY`).
Why needed: fresh DBs build the whole schema from here; old DBs upgrade.

### `signals.py` — graceful shutdown
`install_shutdown_handler(callback)`: SIGINT/SIGTERM → `stop()`, no-op off the
main thread. Why needed: workers/beat drain their current run before exiting.

### `__init__.py`
Public API re-exports (`Job`, `Task`, `JobManager`, `TaskContext`, records,
statuses, helpers). Why needed: a clean `from pyreljob import ...`.

### `tests/`
- `test_core.py` (SQLite): end-to-end behavior — enqueue→worker→retry→
  compensation→cancel→resume→beat→self-scheduling→idempotency→migration upgrade.
- `test_postgres.py` (integration): same code path on real PostgreSQL,
  skipped unless `TEST_DATABASE_URL` is set.
Why needed: both dialects are a first-class guarantee; `make unit-test` /
`make e2e`.

### `examples/hello.py`
A runnable hello-world job for manual testing (library usage).

### Repo root
`pyproject.toml` (build config, deps, ruff/mypy strict), `Makefile` (dev
targets), `DESIGN.md` (contract), `README.md` (how-to), `docker-compose.yml`
(Postgres for e2e tests).

## Data model (three tables)

```
jobs    durable entities:  id, job (dotted class), args, queue, priority,
                           status(active|cancelled), source(on_demand|scheduled),
                           cron, next_run_at, max_attempts, idempotency_key
runs    invocations:       id, job_id, status, result, error, ctx, worker_id,
                           scheduled_at, created/started/finished_at, locked_at
tasks   task executions:  id, run_id, position, task_name, status, result,
                           error, attempts, retry_at, started_at,
                           finished_at, compensated_at
```

## Invariants a reviewer should check

1. **The DB is the single source of truth.** No `create_all`; only migrations
   mutate schema; the ORM mirrors it.
2. **Every state transition is an atomic conditional UPDATE.** Claims, lease
   renewals, completions, beat claims, idempotent enqueues — none are
   read-modify-write; concurrent replicas can't double-execute or double-fire.
3. **`JobManager`/`Worker` never touch SQLAlchemy.** They speak only to the
   `Backend` ABC → portability is structural, not incidental.
4. **Dotted paths, never code, cross the DB.** Jobs resolve classes in the
   worker's environment; the DB stores references + args.
5. **Resumability is checkpointed in `tasks`** + persisted `ctx` — a re-claimed
   run resumes at the first non-succeeded task.
6. **Retries are per-job** (`max_attempts`), tracked per-task (`attempts`),
   gated by `run.scheduled_at` (also the delayed/retry mechanism).
7. **Terminal semantics**: cancel = stop only (never compensates); exhaustion =
   reverse `undo`; a terminal run re-arms via `Job.next_runtime` (or stops).
8. **At-least-once is documented, not hidden** — idempotency keys are the
   mitigation; tasks should be idempotent.
9. **Portability is tested, not assumed** — same suite on SQLite and Postgres.

## Suggested review order

1. `task.py` + `core/job.py` — understand the model and vocabulary.
2. `backends/base.py` + `core/manager.py` + `core/worker.py` — the three
   surfaces (what the DB must provide, what the manager/worker do).
3. `backends/sqlalchemy_backend.py` — where the correctness lives (claims,
   leases, idempotency).
4. `migrations/versions.py` — the schema and the upgrade story.
5. `tests/` — the guarantees as executable spec.
