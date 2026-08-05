"""Versioned schema steps for pyreljob's own tables.

Add new migrations at the end; never edit or reorder existing ones. The
database is the source of truth — ``migrate()`` applies these steps and the
ORM simply mirrors the result.

Model (see DESIGN.md): a **Job** is the durable entity (one row of ``jobs``);
a **Run** is one invocation of a job (one row of ``runs``); a **TaskRun** is
one task execution within a run (one row of ``tasks``).

v4 transforms the v1–v3 ``jobs`` table (which stored one row per *execution*)
into durable entities, creates the ``runs`` and ``tasks`` tables, and
backfills one run per pre-existing row. It is guarded so it upgrades both
migration-built and older ``create_all``-era databases.

The DDL is emitted per-dialect so ``id`` primary keys auto-increment on both
SQLite (``INTEGER PRIMARY KEY``) and PostgreSQL (``SERIAL PRIMARY KEY``).
"""

from __future__ import annotations

from sqlalchemy import Connection, inspect, text

from pyreljob.migrations.runner import Migration


def _is_postgres(conn: Connection) -> bool:
    return conn.dialect.name == "postgresql"


def _id_column(conn: Connection) -> str:
    return "SERIAL" if _is_postgres(conn) else "INTEGER"


def _runs_table(conn: Connection) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS runs (
    id            {_id_column(conn)} PRIMARY KEY,
    job_id        INTEGER      NOT NULL,
    status        VARCHAR(16)  NOT NULL DEFAULT 'pending',
    result        JSON,
    error         TEXT,
    ctx           JSON,
    worker_id     VARCHAR(128),
    scheduled_at  TIMESTAMP,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    locked_at     TIMESTAMP
)
"""


def _tasks_table(conn: Connection) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS tasks (
    id              {_id_column(conn)} PRIMARY KEY,
    run_id          INTEGER      NOT NULL,
    position        INTEGER      NOT NULL,
    task_name       VARCHAR(255) NOT NULL,
    status          VARCHAR(16)  NOT NULL DEFAULT 'pending',
    result          JSON,
    error           TEXT,
    attempts        INTEGER      NOT NULL DEFAULT 0,
    retry_at        TIMESTAMP,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    compensated_at  TIMESTAMP
)
"""


def _jobs_table(conn: Connection) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS jobs (
    id            {_id_column(conn)} PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    queue         VARCHAR(64)  NOT NULL DEFAULT 'default',
    status        VARCHAR(16)  NOT NULL DEFAULT 'pending',
    payload       JSON,
    result        JSON,
    error         TEXT,
    attempts      INTEGER      NOT NULL DEFAULT 0,
    max_attempts  INTEGER      NOT NULL DEFAULT 3,
    priority      INTEGER      NOT NULL DEFAULT 0,
    worker_id     VARCHAR(128),
    schedule      VARCHAR(128),
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled_at  TIMESTAMP,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    next_run_at   TIMESTAMP
)
"""


def _create_jobs_table(conn: Connection) -> None:
    conn.execute(text(_jobs_table(conn)))
    conn.execute(
        text("CREATE INDEX IF NOT EXISTS ix_jobs_queue_status ON jobs (queue, status)")
    )
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_jobs_scheduled_at ON jobs (scheduled_at)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_jobs_next_run_at ON jobs (next_run_at)"))


def _create_schedule_index(conn: Connection) -> None:
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_jobs_name_schedule ON jobs (name, schedule)"))


def _add_locked_at(conn: Connection) -> None:
    columns = {col["name"] for col in inspect(conn).get_columns("jobs")}
    if "locked_at" not in columns:
        conn.execute(text("ALTER TABLE jobs ADD COLUMN locked_at TIMESTAMP"))


def _job_run_task_model(conn: Connection) -> None:
    """v4: jobs become durable entities; runs carry execution state; task executions are per-run."""
    conn.execute(text(_runs_table(conn)))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_runs_job_id ON runs (job_id)"))
    conn.execute(text(_tasks_table(conn)))
    conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tasks_run_id ON tasks (run_id)"))

    columns = {col["name"] for col in inspect(conn).get_columns("jobs")}

    # Drop indexes that reference columns that will be renamed or dropped.
    conn.execute(text("DROP INDEX IF EXISTS ix_jobs_name_schedule"))
    conn.execute(text("DROP INDEX IF EXISTS ix_jobs_name"))
    conn.execute(text("DROP INDEX IF EXISTS ix_jobs_scheduled_at"))

    if "name" in columns and "job" not in columns:
        conn.execute(text("ALTER TABLE jobs RENAME COLUMN name TO job"))
    if "payload" in columns and "args" not in columns:
        conn.execute(text("ALTER TABLE jobs RENAME COLUMN payload TO args"))

    if "source" not in columns:
        conn.execute(
            text("ALTER TABLE jobs ADD COLUMN source VARCHAR(16) NOT NULL DEFAULT 'on_demand'")
        )
    if "cron" not in columns:
        conn.execute(text("ALTER TABLE jobs ADD COLUMN cron VARCHAR(128)"))
    if "idempotency_key" not in columns:
        conn.execute(text("ALTER TABLE jobs ADD COLUMN idempotency_key VARCHAR(255)"))

    # Legacy rows that carried a schedule were maintained definitions; split
    # their schedule into the entity's cron/source.
    if "schedule" in columns:
        conn.execute(
            text(
                "UPDATE jobs SET source = 'scheduled', cron = schedule "
                "WHERE schedule IS NOT NULL"
            )
        )

    # Backfill one run per legacy row (they stored one row per execution).
    if "status" in columns:
        conn.execute(
            text(
                "INSERT INTO runs "
                "  (job_id, status, result, error, worker_id, scheduled_at, "
                "   started_at, finished_at, locked_at, created_at) "
                "SELECT id, "
                "       CASE WHEN status = 'running' THEN 'pending' ELSE status END, "
                "       result, error, worker_id, scheduled_at, started_at, "
                "       finished_at, locked_at, created_at "
                "FROM jobs"
            )
        )

    # Entity status is active | cancelled; legacy run statuses collapse to that.
    if "status" in columns:
        conn.execute(
            text(
                "UPDATE jobs SET status = "
                "  CASE WHEN status = 'cancelled' THEN 'cancelled' ELSE 'active' END"
            )
        )

    # Execution state moved to runs; drop it from jobs.
    for column in (
        "schedule",
        "result",
        "error",
        "attempts",
        "worker_id",
        "scheduled_at",
        "started_at",
        "finished_at",
        "locked_at",
    ):
        if column in columns:
            conn.execute(text(f"ALTER TABLE jobs DROP COLUMN {column}"))

    conn.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_idempotency_key "
            "ON jobs (idempotency_key)"
        )
    )


def _add_timestamps(conn: Connection) -> None:
    """v5: uniform created_at/updated_at on every table.

    ``updated_at`` is maintained by the backend on every write (app-level, so
    it stays portable); ``created_at`` defaults on insert.
    """
    jobs = {c["name"] for c in inspect(conn).get_columns("jobs")}
    runs = {c["name"] for c in inspect(conn).get_columns("runs")}
    tasks = {c["name"] for c in inspect(conn).get_columns("tasks")}

    if "updated_at" not in jobs:
        conn.execute(text("ALTER TABLE jobs ADD COLUMN updated_at TIMESTAMP"))
    if "updated_at" not in runs:
        conn.execute(text("ALTER TABLE runs ADD COLUMN updated_at TIMESTAMP"))
    if "created_at" not in tasks:
        conn.execute(
            text(
                "ALTER TABLE tasks ADD COLUMN created_at TIMESTAMP "
                "NOT NULL DEFAULT CURRENT_TIMESTAMP"
            )
        )
    if "updated_at" not in tasks:
        conn.execute(text("ALTER TABLE tasks ADD COLUMN updated_at TIMESTAMP"))


MIGRATIONS: list[Migration] = [
    Migration(1, "create jobs table", _create_jobs_table),
    Migration(2, "index job schedule", _create_schedule_index),
    Migration(3, "add job lease (locked_at)", _add_locked_at),
    Migration(4, "job entities, runs, tasks", _job_run_task_model),
    Migration(5, "add created_at/updated_at timestamps", _add_timestamps),
]
