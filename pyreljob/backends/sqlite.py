"""SQLite backend — SQLAlchemy's SQLite dialect.

Uses the portable conditional-UPDATE claim (SQLite has no
``FOR UPDATE SKIP LOCKED``). SQLite-only tweaks live here.
"""

from pyreljob.backends.sqlalchemy import SQLAlchemyBackend


class SQLiteBackend(SQLAlchemyBackend):
    """SQLite-specific backend.

    Uses the portable conditional-UPDATE claim (SQLite has no
    ``FOR UPDATE SKIP LOCKED``). SQLite-only tweaks live here.
    """
