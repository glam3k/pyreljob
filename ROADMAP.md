# pyreljob roadmap

Status labels: **done** · **next** · **planned** · **v2**

## Released

- **v0.1.0** — core framework: Task/Job/Run model, SQLite + Postgres backends (SKIP LOCKED claim), leases + heartbeats, per-job retries, resumable checkpoints, saga compensation, `next_runtime` self-scheduling, cron beat, idempotency keys, migrations v1–v5, graceful shutdown.
- **v0.2.0** — **async-native worker**: `Task.run/undo` are coroutines; `Worker(..., max_concurrency=N)` runs N runs in parallel on an event loop; `asyncio.wait_for` timeouts (no thread-leak watchdog).
- **v0.2.1** — `JobManager.delete` + `prune` (cleanup).

## next

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
- **Reconciliation sweep** — detect and re-claim runs stuck without a terminal state.
- **Whole-job (run-level) deadlines** — in addition to per-task `timeout`.
