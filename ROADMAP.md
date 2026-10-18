# pyreljob roadmap

Status labels: **done** · **next** · **planned** · **v2**

## Released

- **v0.1.0** — core framework: Task/Job/Run model, SQLite + Postgres backends (SKIP LOCKED claim), leases + heartbeats, per-job retries, resumable checkpoints, saga compensation, `next_runtime` self-scheduling, cron beat, idempotency keys, migrations v1–v5, graceful shutdown.
- **v0.2.0** — **async-native worker**: `Task.run/undo` are coroutines; `Worker(..., max_concurrency=N)` runs N runs in parallel on an event loop; `asyncio.wait_for` timeouts (no thread-leak watchdog).
- **v0.2.1** — `JobManager.delete` + `prune` (cleanup).
- **Unreleased** — stuck-job recovery: `reconcile_abandoned` re-arms a job whose fire was claimed but never executed, and one left disarmed with nothing in flight (`2ddabbc`, `895a42f`); the worker's main loop tolerates a failed poll instead of dying (`2196ded`). This was the old "Reconciliation sweep" planned item.

## next

- **Run retention** — nothing ever prunes run history, so a long-lived database grows without bound. Real example: an app accumulated 2.3M run rows in two weeks, because per-user jobs kept firing for deleted users; 2.31M of those rows belonged to jobs that no longer existed and could never execute. Needs a retention policy with a default (age + a cap per job), plus a way to prune runs whose owning job is gone. `JobManager.prune` exists for explicit cleanup but nothing calls it on a schedule. Cheap to add, and it belongs here rather than in each app: an unbounded table is a framework-shaped problem, not an application one.
- **Failure hooks** — a callback registry (`manager.on("run.failed", handler)`, etc.) so a failed run can page/alert/Sentry automatically. The single most valuable production piece.
- **Metrics** — counts by status, run latency, retry rate (Prometheus/OpenTelemetry) for dashboards and alerting.

## planned

- **Hardening** — chaos/race tests (concurrent claims, crash mid-run at every step boundary), whole-run deadline, at-least-once + idempotency contract tests.
- **Observability UI** — browse jobs/runs/tasks without SQL (small web view or better CLI).
- **HTTP/REST API service** — thin adapter over `JobManager` for producers that can't reach the DB (other languages, no DB creds, authz).
- **Postgres `LISTEN/NOTIFY`** — wake workers instantly instead of polling.

## v2

- **Task isolation** — run tasks in subprocesses so a truly hung task can be killed (currently a coroutine that never awaits can't be force-cancelled).
- **Async SQLAlchemy engine** — replace the `asyncio.to_thread` backend calls with a native async engine.
- **Custom retry schedules** — beyond exponential backoff (fixed, custom functions).
- **Whole-job (run-level) deadlines** — in addition to per-task `timeout`.
