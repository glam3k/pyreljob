"""Lightweight, database-agnostic migration framework.

Applies ordered, versioned schema steps to SQLite or PostgreSQL. Tracks
applied versions in a ``schema_migrations`` table, so each step runs
exactly once.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from sqlalchemy import Connection, inspect, text
from sqlalchemy.engine import Engine

# A migration step is a callable that receives a SQLAlchemy Connection.
MigrationFn = Callable[[Connection], None]


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    fn: MigrationFn


class MigrationRunner:
    def __init__(self, engine: Engine, migrations: Iterable[Migration] = ()) -> None:
        self._engine = engine
        self._migrations: list[Migration] = sorted(migrations, key=lambda m: m.version)

    @property
    def version(self) -> int | None:
        """The highest applied migration version, or None if never migrated."""
        if not self._table_exists():
            return None
        with self._engine.connect() as conn:
            row = conn.execute(
                text("SELECT MAX(version) FROM schema_migrations")
            ).scalar()
            return row

    def pending(self) -> list[Migration]:
        current = self.version or 0
        return [m for m in self._migrations if m.version > current]

    def up(self, steps: int | None = None) -> list[Migration]:
        """Apply pending migrations in order. Returns the migrations applied."""
        applied: list[Migration] = []
        for migration in self.pending():
            if steps is not None and len(applied) >= steps:
                break
            self._apply(migration)
            applied.append(migration)
        return applied

    def current(self) -> int:
        return self.version or 0

    def _apply(self, migration: Migration) -> None:
        with self._engine.begin() as conn:
            self._ensure_table(conn)
            migration.fn(conn)
            conn.execute(
                text(
                    "INSERT INTO schema_migrations (version, description) "
                    "VALUES (:version, :description)"
                ),
                {"version": migration.version, "description": migration.description},
            )

    def _table_exists(self) -> bool:
        with self._engine.connect() as conn:
            return inspect(conn).has_table("schema_migrations")

    @staticmethod
    def _ensure_table(conn: Connection) -> None:
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "  version INTEGER PRIMARY KEY,"
                "  description TEXT NOT NULL,"
                "  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
                ")"
            )
        )
