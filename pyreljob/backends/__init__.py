"""Backend selection — the ``backend_from_url`` factory.

Each backend is a concrete subclass of :class:`~pyreljob.backends.base.Backend`:
``SQLAlchemyBackend`` (shared portability layer) in
:mod:`pyreljob.backends.sqlalchemy`, with dialect-specific
``SQLiteBackend``/:mod:`pyreljob.backends.sqlite` and
``PostgresBackend``/:mod:`pyreljob.backends.postgres`.
"""

from __future__ import annotations

from typing import Any

from pyreljob.backends.postgres import PostgresBackend
from pyreljob.backends.sqlalchemy import SQLAlchemyBackend
from pyreljob.backends.sqlite import SQLiteBackend

__all__ = ["PostgresBackend", "SQLAlchemyBackend", "SQLiteBackend", "backend_from_url"]


def backend_from_url(url: str, **engine_kwargs: Any) -> SQLAlchemyBackend:
    """Build the right backend for a SQLAlchemy URL.

    Configures sensible engine defaults (``pool_pre_ping``, and
    ``check_same_thread=False`` for SQLite so runs can execute on worker
    threads) and returns a :class:`SQLiteBackend` or :class:`PostgresBackend`.
    """
    from sqlalchemy import create_engine

    options: dict[str, Any] = {"pool_pre_ping": True}
    options.update(engine_kwargs)
    if url.startswith("sqlite"):
        connect_args = dict(options.get("connect_args") or {})
        connect_args.setdefault("check_same_thread", False)
        options["connect_args"] = connect_args
    engine = create_engine(url, **options)
    if engine.dialect.name == "postgresql":
        return PostgresBackend(engine)
    return SQLiteBackend(engine)
