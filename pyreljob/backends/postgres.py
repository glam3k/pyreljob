"""Postgres backend — SQLAlchemy's PostgreSQL dialect.

Claims runs with ``SELECT ... FOR UPDATE SKIP LOCKED`` so concurrent workers
never block each other on the same rows. Postgres-only features (e.g. worker
wake-up via LISTEN/NOTIFY) have a clean home here.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import case, select, update
from sqlalchemy.orm import Session

from pyreljob.backends.sqlalchemy import SQLAlchemyBackend
from pyreljob.job import JobStatus, RunRecord, RunStatus
from pyreljob.models.orm import JobModel, RunModel


class PostgresBackend(SQLAlchemyBackend):
    """PostgreSQL-specific backend.

    Claims runs with ``SELECT ... FOR UPDATE SKIP LOCKED`` so concurrent
    workers never block each other on the same rows.
    """

    def claim(
        self,
        worker_id: str,
        queue: str | None = None,
        *,
        lease_seconds: int = 30,
    ) -> RunRecord | None:
        now = datetime.now()
        lease_expiry = datetime.now() - timedelta(seconds=lease_seconds)
        stmt = (
            select(RunModel.id)
            .join(JobModel, JobModel.id == RunModel.job_id)
            .where(
                JobModel.status == JobStatus.ACTIVE,
                self._run_ready_clause(now, lease_expiry),
            )
            .order_by(JobModel.priority.desc(), RunModel.id.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if queue:
            stmt = stmt.where(JobModel.queue == queue)

        with Session(self._engine) as session, session.begin():
            row = session.execute(stmt).first()
            if row is None:
                return None
            run_id = row[0]
            session.execute(
                update(RunModel)
                .where(RunModel.id == run_id)
                .values(
                    status=RunStatus.RUNNING,
                    worker_id=worker_id,
                    error=None,
                    started_at=case(
                        (RunModel.started_at.is_(None), now), else_=RunModel.started_at
                    ),
                    locked_at=now,
                    updated_at=now,
                )
            )
            model = session.get(RunModel, run_id)
            if model is None:
                return None
            return RunRecord.from_model(model)

