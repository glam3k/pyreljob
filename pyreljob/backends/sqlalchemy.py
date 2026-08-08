"""SQLAlchemy backend — the shared portability layer.

``SQLAlchemyBackend`` holds everything the two dialects share (SQLAlchemy is
the portability layer). The dialect subclasses live in
:mod:`pyreljob.backends.sqlite` and :mod:`pyreljob.backends.postgres`, and
override only the genuinely dialect-specific parts — today that's just
:meth:`claim`: PostgreSQL gets ``FOR UPDATE SKIP LOCKED`` (Graphile-worker
style), SQLite uses the portable conditional-UPDATE claim. Postgres-only
features (e.g. worker wake-up via LISTEN/NOTIFY) have a clean home in
``PostgresBackend``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Engine, and_, case, delete, exists, func, insert, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pyreljob.backends.base import Backend
from pyreljob.job import (
    JobRecord,
    JobSource,
    JobStatus,
    RunRecord,
    RunStatus,
    TaskRecord,
    TaskStatus,
)
from pyreljob.migrations.runner import MigrationRunner
from pyreljob.migrations.versions import MIGRATIONS
from pyreljob.models.orm import JobModel, RunModel, TaskModel


class SQLAlchemyBackend(Backend):
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    @property
    def engine(self) -> Engine:
        return self._engine

    def migrate(self) -> list[str]:
        runner = MigrationRunner(self._engine, MIGRATIONS)
        applied = runner.up()
        return [m.description for m in applied]

    # -- jobs (durable entities) ---------------------------------------------

    def enqueue(
        self,
        job: str,
        args: Any = None,
        *,
        queue: str = "default",
        priority: int = 0,
        max_attempts: int = 3,
        retries: int = 0,
        idempotency_key: str | None = None,
        scheduled_at: datetime | None = None,
    ) -> JobRecord:
        key = idempotency_key
        if key is not None:
            existing = self._find_by_idempotency_key(key)
            if existing is not None:
                return existing
        model = JobModel(
            job=job,
            queue=queue,
            args=args,
            priority=priority,
            source=JobSource.ON_DEMAND,
            max_attempts=max_attempts,
            retries=retries,
            idempotency_key=key,
            updated_at=datetime.now(),
        )
        try:
            with Session(self._engine) as session:
                session.add(model)
                session.flush()
                assert model.id is not None
                session.add(
                    RunModel(job_id=model.id, scheduled_at=scheduled_at, updated_at=datetime.now())
                )
                session.commit()
                return JobRecord.from_model(model)
        except IntegrityError:
            # Lost the race to a concurrent enqueue with the same key.
            if key is not None:
                existing = self._find_by_idempotency_key(key)
                if existing is not None:
                    return existing
            raise

    def _find_by_idempotency_key(self, key: str) -> JobRecord | None:
        with Session(self._engine) as session:
            model = session.execute(
                select(JobModel).where(JobModel.idempotency_key == key)
            ).scalar_one_or_none()
            return JobRecord.from_model(model) if model else None

    def schedule(
        self,
        job: str,
        args: Any = None,
        *,
        queue: str = "default",
        max_attempts: int = 3,
        retries: int = 0,
        next_run_at: datetime | None = None,
    ) -> JobRecord:
        with Session(self._engine) as session:
            existing = session.execute(
                select(JobModel).where(
                    JobModel.job == job,
                    JobModel.source == JobSource.SCHEDULED,
                )
            ).scalar_one_or_none()
            if existing is not None:
                return JobRecord.from_model(existing)
        model = JobModel(
            job=job,
            queue=queue,
            args=args,
            priority=0,
            source=JobSource.SCHEDULED,
            max_attempts=max_attempts,
            retries=retries,
            next_run_at=next_run_at,
            updated_at=datetime.now(),
        )
        with Session(self._engine) as session:
            session.add(model)
            session.commit()
            return JobRecord.from_model(model)

    def get(self, job_id: int) -> JobRecord | None:
        with Session(self._engine) as session:
            model = session.get(JobModel, job_id)
            return JobRecord.from_model(model) if model else None

    def cancel_job(self, job_id: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(JobModel)
                .where(JobModel.id == job_id)
                .values(status=JobStatus.CANCELLED, updated_at=datetime.now())
            )
            conn.execute(
                update(RunModel)
                .where(
                    RunModel.job_id == job_id,
                    RunModel.status.in_([RunStatus.READY, RunStatus.RUNNING]),
                )
                .values(
                    status=RunStatus.CANCELLED,
                    finished_at=datetime.now(),
                    locked_at=None,
                    updated_at=datetime.now(),
                )
            )

    def is_job_cancelled(self, job_id: int) -> bool:
        with Session(self._engine) as session:
            model = session.get(JobModel, job_id)
            return model is not None and model.status == JobStatus.CANCELLED

    def increment_job_attempts(self, job_id: int) -> int:
        with self._engine.begin() as conn:
            row = conn.execute(
                update(JobModel)
                .where(JobModel.id == job_id)
                .values(attempts=JobModel.attempts + 1, updated_at=datetime.now())
                .returning(JobModel.attempts)
            ).first()
            return row[0] if row else 0

    def reset_job_attempts(self, job_id: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(JobModel)
                .where(JobModel.id == job_id)
                .values(attempts=0, updated_at=datetime.now())
            )

    def delete_job(self, job_id: int) -> None:
        with Session(self._engine) as session:
            active = session.execute(
                select(RunModel.id).where(
                    RunModel.job_id == job_id,
                    RunModel.status.in_([RunStatus.READY, RunStatus.RUNNING]),
                )
            ).first()
            if active is not None:
                raise ValueError(
                    "cannot delete a job with ready or running runs; cancel it first"
                )
        with self._engine.begin() as conn:
            run_ids = [
                row[0]
                for row in conn.execute(
                    select(RunModel.id).where(RunModel.job_id == job_id)
                )
            ]
            if run_ids:
                conn.execute(delete(TaskModel).where(TaskModel.run_id.in_(run_ids)))
                conn.execute(delete(RunModel).where(RunModel.job_id == job_id))
            conn.execute(delete(JobModel).where(JobModel.id == job_id))

    def prune_runs(self, older_than: datetime) -> int:
        with Session(self._engine) as session:
            run_ids = session.execute(
                select(RunModel.id).where(
                    RunModel.status.in_(
                        [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED]
                    ),
                    RunModel.finished_at < older_than,
                )
            ).scalars().all()
        if not run_ids:
            return 0
        with self._engine.begin() as conn:
            conn.execute(delete(TaskModel).where(TaskModel.run_id.in_(run_ids)))
            conn.execute(delete(RunModel).where(RunModel.id.in_(run_ids)))
        return len(run_ids)

    def list_scheduled(self) -> list[JobRecord]:
        with Session(self._engine) as session:
            models = (
                session.execute(
                    select(JobModel)
                    .where(
                        JobModel.source == JobSource.SCHEDULED,
                        JobModel.status == JobStatus.ACTIVE,
                    )
                    .order_by(JobModel.id.asc())
                )
                .scalars()
                .all()
            )
            return [JobRecord.from_model(m) for m in models]

    def list_jobs(
        self, *, limit: int = 100, offset: int = 0
    ) -> list[JobRecord]:
        with Session(self._engine) as session:
            models = (
                session.execute(
                    select(JobModel)
                    .order_by(JobModel.id.desc())
                    .offset(offset)
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            return [JobRecord.from_model(m) for m in models]

    def claim_scheduled(self, job_id: int, next_run_at: datetime | None) -> bool:
        now = datetime.now()
        run_in_flight = exists(
            select(RunModel.id).where(
                RunModel.job_id == JobModel.id,
                RunModel.status.in_([RunStatus.READY, RunStatus.RUNNING]),
            )
        )
        with self._engine.begin() as conn:
            updated = conn.execute(
                update(JobModel)
                .where(
                    JobModel.id == job_id,
                    JobModel.status == JobStatus.ACTIVE,
                    JobModel.next_run_at <= now,
                    ~run_in_flight,
                )
                .values(next_run_at=next_run_at, updated_at=datetime.now())
                .returning(JobModel.id)
            ).first()
            return updated is not None

    def set_next_run_at(self, job_id: int, next_run_at: datetime | None) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(JobModel)
                .where(JobModel.id == job_id)
                .values(next_run_at=next_run_at, updated_at=datetime.now())
            )

    # -- runs (invocations) --------------------------------------------------

    def _run_ready_clause(self, now: datetime, lease_expiry: datetime) -> Any:
        ready = and_(
            RunModel.status == RunStatus.READY,
            (RunModel.scheduled_at.is_(None)) | (RunModel.scheduled_at <= now),
        )
        reclaim = and_(
            RunModel.status == RunStatus.RUNNING,
            RunModel.locked_at < lease_expiry,
        )
        return or_(ready, reclaim)

    def claim(
        self,
        worker_id: str,
        queue: str | None = None,
        *,
        lease_seconds: int = 30,
    ) -> RunRecord | None:
        """Portable claim: a conditional UPDATE that wins exactly one run.

        Correct on every dialect; ``PostgresBackend`` overrides this with
        ``FOR UPDATE SKIP LOCKED`` for better behavior under contention.
        """
        now = datetime.now()
        lease_expiry = datetime.now() - timedelta(seconds=lease_seconds)
        ready = and_(
            RunModel.status == RunStatus.READY,
            (RunModel.scheduled_at.is_(None)) | (RunModel.scheduled_at <= now),
        )
        reclaim = and_(
            RunModel.status == RunStatus.RUNNING,
            RunModel.locked_at < lease_expiry,
        )
        stmt = (
            select(RunModel.id)
            .join(JobModel, JobModel.id == RunModel.job_id)
            .where(JobModel.status == JobStatus.ACTIVE, self._run_ready_clause(now, lease_expiry))
            .order_by(JobModel.priority.desc(), RunModel.id.asc())
        )
        if queue:
            stmt = stmt.where(JobModel.queue == queue)
        stmt = stmt.limit(1)

        with Session(self._engine) as session, session.begin():
            row = session.execute(stmt).first()
            if row is None:
                return None
            run_id = row[0]
            updated = session.execute(
                update(RunModel)
                .where(RunModel.id == run_id, or_(ready, reclaim))
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
                .returning(RunModel.id)
            ).first()
            if updated is None:
                return None
            model = session.get(RunModel, run_id)
            if model is None:
                return None
            return RunRecord.from_model(model)

    def renew_lease(self, run_id: int, worker_id: str) -> bool:
        with self._engine.begin() as conn:
            updated = conn.execute(
                update(RunModel)
                .where(
                    RunModel.id == run_id,
                    RunModel.worker_id == worker_id,
                    RunModel.status == RunStatus.RUNNING,
                )
                .values(locked_at=datetime.now(), updated_at=datetime.now())
                .returning(RunModel.id)
            ).first()
            return updated is not None

    def complete_run(self, run_id: int, result: Any = None) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(RunModel)
                .where(RunModel.id == run_id, RunModel.status == RunStatus.RUNNING)
                .values(
                    status=RunStatus.SUCCEEDED,
                    result=result,
                    finished_at=datetime.now(),
                    locked_at=None,
                    updated_at=datetime.now(),
                )
            )

    def fail_run(
        self,
        run_id: int,
        error: str,
        *,
        retry_at: datetime | None = None,
        attempts: int = 1,
    ) -> None:
        with self._engine.begin() as conn:
            if retry_at is not None:
                conn.execute(
                    update(RunModel)
                    .where(RunModel.id == run_id, RunModel.status == RunStatus.RUNNING)
                    .values(
                        status=RunStatus.READY,
                        error=error,
                        scheduled_at=retry_at,
                        finished_at=None,
                        locked_at=None,
                        worker_id=None,
                        updated_at=datetime.now(),
                    )
                )
            else:
                conn.execute(
                    update(RunModel)
                    .where(RunModel.id == run_id, RunModel.status == RunStatus.RUNNING)
                    .values(
                        status=RunStatus.FAILED,
                        error=error,
                        finished_at=datetime.now(),
                        locked_at=None,
                        updated_at=datetime.now(),
                    )
                )

    def cancel_run(self, run_id: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(RunModel)
                .where(RunModel.id == run_id)
                .values(
                    status=RunStatus.CANCELLED,
                    finished_at=datetime.now(),
                    locked_at=None,
                    updated_at=datetime.now(),
                )
            )

    def set_run_ctx(self, run_id: int, ctx: dict[str, Any] | None) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(RunModel)
                .where(RunModel.id == run_id)
                .values(ctx=ctx, updated_at=datetime.now())
            )

    def set_run_progress(self, run_id: int, progress: float | None) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(RunModel)
                .where(RunModel.id == run_id)
                .values(progress=progress, updated_at=datetime.now())
            )

    def get_run(self, run_id: int) -> RunRecord | None:
        with Session(self._engine) as session:
            model = session.get(RunModel, run_id)
            return RunRecord.from_model(model) if model else None

    def runs(self, job_id: int) -> list[RunRecord]:
        with Session(self._engine) as session:
            models = (
                session.execute(
                    select(RunModel)
                    .where(RunModel.job_id == job_id)
                    .order_by(RunModel.id.desc())
                )
                .scalars()
                .all()
            )
            return [RunRecord.from_model(m) for m in models]

    def recent_failures(self, limit: int = 10) -> list[RunRecord]:
        with Session(self._engine) as session:
            models = (
                session.execute(
                    select(RunModel)
                    .where(RunModel.status == RunStatus.FAILED)
                    .order_by(RunModel.finished_at.desc())
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            return [RunRecord.from_model(m) for m in models]

    def counts(self, queue: str | None = None) -> dict[str, int]:
        with Session(self._engine) as session:
            stmt = (
                select(RunModel.status, func.count())
                .join(JobModel, JobModel.id == RunModel.job_id)
                .group_by(RunModel.status)
            )
            if queue:
                stmt = stmt.where(JobModel.queue == queue)
            return {status: count for status, count in session.execute(stmt).all()}

    def create_run(
        self,
        job_id: int,
        *,
        scheduled_at: datetime | None = None,
    ) -> RunRecord:
        model = RunModel(job_id=job_id, scheduled_at=scheduled_at, updated_at=datetime.now())
        with Session(self._engine) as session:
            session.add(model)
            session.commit()
            session.refresh(model)
            return RunRecord.from_model(model)

    # -- tasks (task executions within a run) ------------------------------------

    def tasks(self, run_id: int) -> list[TaskRecord]:
        with Session(self._engine) as session:
            models = (
                session.execute(
                    select(TaskModel)
                    .where(TaskModel.run_id == run_id)
                    .order_by(TaskModel.position.asc())
                )
                .scalars()
                .all()
            )
            return [TaskRecord.from_model(m) for m in models]

    def start_task(
        self,
        run_id: int,
        position: int,
        task_name: str,
        *,
        attempts: int = 1,
    ) -> None:
        with self._engine.begin() as conn:
            existing = conn.execute(
                select(TaskModel.id).where(
                    TaskModel.run_id == run_id,
                    TaskModel.position == position,
                )
            ).scalar_one_or_none()
            if existing is None:
                conn.execute(
                    insert(TaskModel)
                    .values(
                        run_id=run_id,
                        position=position,
                        task_name=task_name,
                        status=TaskStatus.RUNNING,
                        attempts=attempts,
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                        started_at=datetime.now(),
                    )
                )
            else:
                conn.execute(
                    update(TaskModel)
                    .where(
                        TaskModel.run_id == run_id,
                        TaskModel.position == position,
                    )
                    .values(
                        task_name=task_name,
                        status=TaskStatus.RUNNING,
                        result=None,
                        error=None,
                        attempts=attempts,
                        retry_at=None,
                        updated_at=datetime.now(),
                        started_at=datetime.now(),
                        finished_at=None,
                    )
                )

    def succeed_task(self, run_id: int, position: int, result: Any = None) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(TaskModel)
                .where(TaskModel.run_id == run_id, TaskModel.position == position)
                .values(
                    status=TaskStatus.SUCCEEDED,
                    result=result,
                    error=None,
                    finished_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )

    def fail_task(
        self,
        run_id: int,
        position: int,
        error: str,
        *,
        attempts: int = 1,
        retry_at: datetime | None = None,
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(TaskModel)
                .where(TaskModel.run_id == run_id, TaskModel.position == position)
                .values(
                    status=TaskStatus.FAILED,
                    error=error,
                    attempts=attempts,
                    retry_at=retry_at,
                    finished_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )

    def cancel_task(self, run_id: int, position: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(TaskModel)
                .where(TaskModel.run_id == run_id, TaskModel.position == position)
                .values(
                    status=TaskStatus.CANCELLED,
                    finished_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )

    def compensate_task(self, run_id: int, position: int) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                update(TaskModel)
                .where(TaskModel.run_id == run_id, TaskModel.position == position)
                .values(
                    status=TaskStatus.COMPENSATED,
                    compensated_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )

