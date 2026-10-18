"""Recovering maintained jobs whose fire was claimed but never executed.

The beat claims a due job by disarming it (``next_run_at = NULL``) and creating a
run; the worker re-arms it after that run completes. Nothing else can revive it:
``claim_scheduled`` needs ``next_run_at <= now`` (NULL never compares true) and
refuses to re-fire while a run is in flight. So a run that is never executed —
every worker down, process killed between the claim and the run being claimed —
ends that job's schedule silently and permanently.

These tests pin the recovery, and just as importantly pin what must *not* be
touched: healthy runs, runs scheduled for the future, jobs that still have a
schedule of their own, and on-demand jobs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import ClassVar

import pytest
from sqlalchemy import update

from pyreljob import Job, JobManager, JobSource, RunStatus, Task, TaskContext
from pyreljob.models.orm import JobModel, RunModel
from pyreljob.worker import Worker


def job_cls_path(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


GRACE = 300


@dataclass
class Ticker(Task):
    async def run(self, ctx: TaskContext) -> str:
        return "ok"


@dataclass
class Poller(Job):
    tasks: ClassVar = [Ticker]

    def next_runtime(self, last_run, ctx: TaskContext) -> datetime | None:
        if last_run is None:
            return datetime.now(timezone.utc)
        return datetime.now(timezone.utc) + timedelta(seconds=5)


@pytest.fixture()
def manager(tmp_path):
    m = JobManager(f"sqlite:///{tmp_path}/jobs.db", abandoned_grace_seconds=GRACE)
    m.migrate()
    return m


def fire_once(manager: JobManager, job) -> None:
    """Let the beat claim the job, with no worker around to execute the run.

    This is the state a job is left in when nothing is running to pick the run
    up: disarmed, with a `ready` run nothing will execute.
    """
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
    manager.tick()
    assert manager.get(job.id).next_run_at is None


def age_run(manager: JobManager, run_id: int, seconds: int = GRACE + 60) -> None:
    """Backdate a run so it looks like it has been waiting far too long."""
    stamp = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    with manager.backend._engine.begin() as conn:
        conn.execute(update(RunModel).where(RunModel.id == run_id).values(created_at=stamp))


def test_abandoned_run_recovers_the_job(manager):
    job = manager.schedule(Poller())
    fire_once(manager, job)
    run = manager.runs(job.id)[0]
    age_run(manager, run.id)

    result = manager.reconcile()

    assert (result.runs, result.jobs) == (1, 1)
    assert manager.runs(job.id)[0].status == RunStatus.FAILED
    assert manager.runs(job.id)[0].error.startswith("abandoned")
    # Re-armed, so the beat fires it again.
    assert manager.get(job.id).next_run_at is not None


def test_the_recovered_job_actually_fires_again(manager):
    job = manager.schedule(Poller())
    fire_once(manager, job)
    age_run(manager, manager.runs(job.id)[0].id)

    manager.tick()  # reconcile + fire

    runs = manager.runs(job.id)
    assert len(runs) == 2
    assert {r.status for r in runs} == {RunStatus.FAILED, RunStatus.READY}


def test_the_beat_reconciles_on_its_own(manager):
    """A user never calls reconcile: the beat must, or nothing ever recovers."""
    job = manager.schedule(Poller())
    fire_once(manager, job)
    age_run(manager, manager.runs(job.id)[0].id)

    manager.tick()  # no explicit reconcile

    runs = manager.runs(job.id)
    assert len(runs) == 2
    assert sum(r.status == RunStatus.FAILED for r in runs) == 1


def test_a_fresh_run_is_left_alone(manager):
    """A run that has not waited long enough may still get a worker."""
    job = manager.schedule(Poller())
    fire_once(manager, job)

    result = manager.reconcile()
    assert (result.runs, result.jobs) == (0, 0)
    assert manager.runs(job.id)[0].status == RunStatus.READY
    assert manager.get(job.id).next_run_at is None


def test_a_run_scheduled_for_the_future_is_not_abandoned(manager):
    """Waiting for its scheduled_at is not the same as being abandoned."""
    job = manager.enqueue(Poller(), scheduled_at=datetime.now(timezone.utc) + timedelta(hours=1))
    run = manager.runs(job.id)[0]
    age_run(manager, run.id, seconds=GRACE * 10)  # old, but due in an hour

    result = manager.reconcile()
    assert (result.runs, result.jobs) == (0, 0)
    assert manager.runs(job.id)[0].status == RunStatus.READY


def test_an_armed_job_keeps_its_own_schedule(manager):
    """Re-arm nothing that is not disarmed: it already has a next fire time."""
    job = manager.schedule(Poller())
    fire_once(manager, job)
    age_run(manager, manager.runs(job.id)[0].id)
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    with manager.backend._engine.begin() as conn:
        conn.execute(update(JobModel).where(JobModel.id == job.id).values(next_run_at=future))

    result = manager.reconcile()

    assert result.runs == 1  # the duplicate run is cleared
    assert result.jobs == 0  # but the job was left armed
    assert manager.get(job.id).next_run_at == future


def test_on_demand_jobs_are_not_re_armed(manager):
    """They are never disarmed, so there is nothing to recover."""
    job = manager.enqueue(Poller())
    age_run(manager, manager.runs(job.id)[0].id)

    result = manager.reconcile()

    assert result.runs == 1
    assert result.jobs == 0
    assert manager.get(job.id).next_run_at is None
    assert manager.get(job.id).source == JobSource.ON_DEMAND


def test_reconcile_is_idempotent(manager):
    job = manager.schedule(Poller())
    fire_once(manager, job)
    age_run(manager, manager.runs(job.id)[0].id)

    first = manager.reconcile()
    assert (first.runs, first.jobs) == (1, 1)
    second = manager.reconcile()
    assert (second.runs, second.jobs) == (0, 0)


def test_a_cancelled_job_is_not_revived(manager):
    job = manager.schedule(Poller())
    fire_once(manager, job)
    age_run(manager, manager.runs(job.id)[0].id)
    with manager.backend._engine.begin() as conn:
        conn.execute(update(JobModel).where(JobModel.id == job.id).values(status="cancelled"))

    result = manager.reconcile()

    assert result.jobs == 0
    assert manager.get(job.id).next_run_at is None


def test_the_worker_still_re_arms_normally(manager):
    """Reconciliation must not double-fire a job whose run did execute."""
    import asyncio

    job = manager.schedule(Poller())
    fire_once(manager, job)
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Poller), Poller)
    asyncio.run(worker._tick())

    assert manager.runs(job.id)[0].status == RunStatus.SUCCEEDED
    assert manager.get(job.id).next_run_at is not None
    result = manager.reconcile()
    assert (result.runs, result.jobs) == (0, 0)
    assert len(manager.runs(job.id)) == 1


def test_a_worker_picking_up_a_run_races_safely(manager):
    """If the worker claims the run mid-sweep, the sweep must not fail it."""
    job = manager.schedule(Poller())
    fire_once(manager, job)
    run = manager.runs(job.id)[0]
    age_run(manager, run.id)
    # Simulate the worker winning: the run is no longer ready.
    with manager.backend._engine.begin() as conn:
        conn.execute(update(RunModel).where(RunModel.id == run.id).values(status=RunStatus.RUNNING))

    result = manager.reconcile()
    assert (result.runs, result.jobs) == (0, 0)
    assert manager.runs(job.id)[0].status == RunStatus.RUNNING


def test_a_disarmed_job_with_no_in_flight_run_is_rearmed(manager):
    """The other way a job gets lost: its run finished and nothing re-armed it.

    The worker deliberately skips ``_reschedule`` for a run whose job class cannot
    be resolved, so a module that moved or was renamed leaves the job disarmed
    with every run terminal. Nothing is left for the stale-run sweep to notice, and
    the job is silently gone from the schedule.
    """
    job = manager.schedule(Poller())
    fire_once(manager, job)
    run = manager.runs(job.id)[0]
    # The run completed, but the job was never re-armed.
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(RunModel)
            .where(RunModel.id == run.id)
            .values(
                status=RunStatus.FAILED,
                created_at=datetime.now(timezone.utc) - timedelta(hours=1),
            )
        )
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(updated_at=datetime.now(timezone.utc) - timedelta(hours=1))
        )

    result = manager.reconcile()

    assert (result.runs, result.jobs) == (0, 1)
    assert manager.get(job.id).next_run_at is not None


def test_a_claim_in_flight_is_not_double_fired(manager):
    """Claiming and creating the run are two statements, not one transaction.

    Between them the job is disarmed with no run at all. Treating that as an
    orphan would let two beats race and fire the job twice, so the grace window
    is what makes the sweep safe.
    """
    job = manager.schedule(Poller())
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=None, updated_at=datetime.now(timezone.utc))
        )

    result = manager.reconcile()

    assert (result.runs, result.jobs) == (0, 0)
    assert manager.get(job.id).next_run_at is None
