"""Integration tests against a real PostgreSQL database.

Skipped unless TEST_DATABASE_URL is set, e.g.::

    docker compose up -d postgres
    TEST_DATABASE_URL=postgresql+psycopg://pyreljob:pyreljob@localhost:5433/pyreljob pytest
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import ClassVar

import pytest

from pyreljob import Job, JobManager, RunStatus, Task, TaskContext, TaskStatus
from pyreljob.worker import Worker

PG_URL = os.environ.get("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(PG_URL is None, reason="TEST_DATABASE_URL not set")

_release = threading.Event()


def tick(worker: Worker) -> None:
    """Run a single claim+execute pass on the async worker from a sync test."""
    asyncio.run(worker._tick())


class Add(Task):
    async def run(self, ctx: TaskContext) -> int:
        return ctx.args["a"] + ctx.args["b"]


@dataclass
class Sum(Job):
    a: int
    b: int

    tasks: ClassVar = [Add]


class Slow(Task):
    async def run(self, ctx: TaskContext) -> str:
        await asyncio.to_thread(_release.wait, 10)
        return "ok"


@dataclass
class SlowJob(Job):
    tasks: ClassVar = [Slow]


@pytest.fixture()
def manager():
    m = JobManager(PG_URL)
    m.migrate()
    yield m
    from sqlalchemy import text

    with m.backend._engine.begin() as conn:
        conn.execute(
            text("DROP TABLE IF EXISTS tasks, runs, jobs, schema_migrations")
        )


def test_backend_selection():
    from pyreljob.backends import backend_from_url
    from pyreljob.backends.postgres import PostgresBackend

    assert isinstance(backend_from_url(PG_URL), PostgresBackend)


def test_enqueue_worker_roundtrip(manager):
    job = manager.enqueue(Sum(3, 4))
    worker = Worker(manager.backend)
    worker.register("pg.Sum", Sum)
    tick(worker)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    assert run.result == {"Add": 7}
    assert manager.tasks(run.id)[0].status == TaskStatus.SUCCEEDED


def test_claim_is_atomic_across_workers(manager):
    for _ in range(5):
        manager.enqueue(Sum(0, 0))

    claimed_ids: list[int] = []

    def run():
        while True:
            record = manager.backend.claim("w")
            if record is None:
                break
            claimed_ids.append(record.id)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert len(claimed_ids) == 5
    assert len(set(claimed_ids)) == 5


def test_worker_reclaims_expired_lease(manager):
    _release.clear()
    job = manager.enqueue(SlowJob())
    run = manager.runs(job.id)[0]
    worker = Worker(manager.backend)
    worker.register("pg.SlowJob", SlowJob)
    t = threading.Thread(target=lambda: asyncio.run(worker._tick()))
    t.start()
    time.sleep(0.5)
    assert manager.runs(job.id)[0].status == RunStatus.RUNNING

    from sqlalchemy import text

    with manager.backend._engine.begin() as conn:
        conn.execute(
            text("UPDATE runs SET locked_at = :expired WHERE id = :id"),
            {"expired": datetime.now(timezone.utc) - timedelta(seconds=30), "id": run.id},
        )

    reclaimed = manager.backend.claim("worker-2", lease_seconds=5)
    assert reclaimed is not None and reclaimed.id == run.id
    _release.set()
    t.join(timeout=5)


@dataclass
class Poller(Job):
    tasks: ClassVar = [Add]
    a: int = 0
    b: int = 0

    def next_runtime(self, last_run, ctx: TaskContext) -> datetime | None:
        return datetime.now(timezone.utc) + timedelta(seconds=5)


def test_reconcile_abandoned_runs(manager):
    """A claimed-but-unexecuted fire must not end a job's schedule.

    Postgres is the dialect that matters: it uses ``FOR UPDATE SKIP LOCKED`` for
    claiming, so the recovery sweep has to coexist with concurrent claimers.
    """
    from sqlalchemy import update

    from pyreljob.models.orm import JobModel, RunModel

    job = manager.schedule(Poller(), idempotency_key="reconcile:1")
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
    manager.tick()
    assert manager.get(job.id).next_run_at is None  # claimed, no worker to run it

    run = manager.runs(job.id)[0]
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(RunModel)
            .where(RunModel.id == run.id)
            .values(created_at=datetime.now(timezone.utc) - timedelta(hours=1))
        )

    result = manager.reconcile()

    assert (result.runs, result.jobs) == (1, 1)
    assert manager.runs(job.id)[0].status == RunStatus.FAILED
    assert manager.get(job.id).next_run_at is not None

    # And it fires again.
    manager.tick()
    assert len(manager.runs(job.id)) == 2
