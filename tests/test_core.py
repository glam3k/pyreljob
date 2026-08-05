"""End-to-end tests for manager, worker, scheduler, and migrations.

Model: a Job is a durable entity; a Run is one invocation of it; a TaskRun is
one step within a run.
"""

from __future__ import annotations

import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import ClassVar

import pytest
from sqlalchemy import create_engine, text, update

from pyreljob import (
    Job,
    JobCancelledError,
    JobManager,
    JobSource,
    JobStatus,
    RunStatus,
    Task,
    TaskContext,
    TaskStatus,
)
from pyreljob.core.job import JobRecord, RunRecord
from pyreljob.core.worker import Worker
from pyreljob.orm import JobModel, RunModel

_release = threading.Event()
UNDONE: list[str] = []


def job_cls_path(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def make_due(backend, run_id: int) -> None:
    with backend._engine.begin() as conn:
        conn.execute(
            update(RunModel)
            .where(RunModel.id == run_id)
            .values(scheduled_at=datetime.now() - timedelta(seconds=1))
        )


class Add(Task):
    def run(self, ctx: TaskContext) -> int:
        return ctx.args["a"] + ctx.args["b"]


@dataclass
class Sum(Job):
    a: int
    b: int

    tasks: ClassVar = [Add]


class Flaky(Task):
    def run(self, ctx: TaskContext) -> None:
        if not ctx.state.get("tried"):
            ctx.state["tried"] = True
            raise ValueError("boom")


@dataclass
class FlakyJob(Job):
    tasks: ClassVar = [Flaky]


class AlwaysFails(Task):
    def run(self, ctx: TaskContext) -> None:
        raise ValueError("always")


@dataclass
class FailingJob(Job):
    tasks: ClassVar = [AlwaysFails]


class SlowTask(Task):
    timeout = 1

    def run(self, ctx: TaskContext) -> str:
        _release.wait(10)
        return "ok"


@dataclass
class SlowJob(Job):
    tasks: ClassVar = [SlowTask]


class TaskA(Task):
    def run(self, ctx: TaskContext) -> str:
        ctx.state.setdefault("order", []).append("1")
        return "one"


class TaskB(Task):
    def run(self, ctx: TaskContext) -> str:
        ctx.state.setdefault("order", []).append("2")
        return ctx.result("TaskA") + "-two"


class TaskC(Task):
    def run(self, ctx: TaskContext) -> str:
        ctx.state.setdefault("order", []).append("3")
        raise RuntimeError("nope")


@dataclass
class ThreeTask(Job):
    tasks: ClassVar = [TaskA, TaskB, TaskC]


class Reserve(Task):
    def run(self, ctx: TaskContext) -> str:
        return "reserved"

    def undo(self, ctx: TaskContext) -> None:
        UNDONE.append("reserve")


class Charge(Task):
    def run(self, ctx: TaskContext) -> str:
        return "charged"

    def undo(self, ctx: TaskContext) -> None:
        UNDONE.append("charge")


@dataclass
class Saga(Job):
    tasks: ClassVar = [Reserve, Charge]


class BlockingTask(Task):
    def run(self, ctx: TaskContext) -> str:
        while not ctx.cancelled:
            time.sleep(0.02)
        raise JobCancelledError()


@dataclass
class BlockingJob(Job):
    tasks: ClassVar = [BlockingTask]


class Resumable1(Task):
    def run(self, ctx: TaskContext) -> str:
        return "one"


class Resumable2(Task):
    def run(self, ctx: TaskContext) -> str:
        if not ctx.state.get("tried"):
            ctx.state["tried"] = True
            raise ValueError("retry me")
        return "two"


@dataclass
class Resumable(Job):
    tasks: ClassVar = [Resumable1, Resumable2, AlwaysFails]


class Poll(Task):
    def run(self, ctx: TaskContext) -> str:
        return "ok"


@dataclass
class Poller(Job):
    tasks: ClassVar = [Poll]

    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        return last_run.finished_at + timedelta(seconds=5)


class NamedTask(Task):
    name = "send-email"

    def run(self, ctx: TaskContext) -> str:
        return "ok"

    def undo(self, ctx: TaskContext) -> None:
        UNDONE.append("send-email")


@dataclass
class NamedJob(Job):
    tasks: ClassVar = [NamedTask]


@pytest.fixture()
def manager(tmp_path):
    m = JobManager(f"sqlite:///{tmp_path}/jobs.db")
    m.migrate()
    return m


def test_migrate_is_idempotent(manager):
    assert manager.migrate() == []
    assert manager.counts() == {}


def test_worker_id_generated_per_process(manager):
    w1 = Worker(manager.backend)
    w2 = Worker(manager.backend)
    assert w1.worker_id != w2.worker_id  # fresh id per process/restart
    assert w1.worker_id

    w3 = Worker(manager.backend, worker_id="worker-1")
    assert w3.worker_id == "worker-1"  # explicit id wins

    job = manager.enqueue(Sum(1, 1))
    w3.register(job_cls_path(Sum), Sum)
    w3._tick()
    assert manager.runs(job.id)[0].worker_id == "worker-1"  # lease tagged with the id


def test_custom_task_name(manager):
    assert NamedTask.task_name() == "send-email"

    job = manager.enqueue(NamedJob())
    worker = Worker(manager.backend)
    worker.register(job_cls_path(NamedJob), NamedJob)
    worker._tick()
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    record = manager.tasks(run.id)[0]
    assert record.task_name == "send-email"  # custom name stored, not the dotted path

    UNDONE.clear()
    manager.undo(job.id)  # still resolves the class by position
    assert UNDONE == ["send-email"]


def test_backend_selection(tmp_path):
    from pyreljob.backends.sqlalchemy_backend import (
        SQLiteBackend,
        backend_from_url,
    )

    backend = backend_from_url(f"sqlite:///{tmp_path}/jobs.db")
    assert isinstance(backend, SQLiteBackend)


def test_worker_stops_cleanly(manager):
    job = manager.enqueue(Sum(1, 1))
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    thread = threading.Thread(target=worker.run_forever)
    thread.start()
    deadline = time.time() + 5
    while manager.runs(job.id)[0].status != RunStatus.SUCCEEDED and time.time() < deadline:
        time.sleep(0.02)
    worker.stop()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert manager.runs(job.id)[0].status == RunStatus.SUCCEEDED


def test_enqueue_creates_job_and_run(manager):
    job = manager.enqueue(Sum(1, 2))
    assert isinstance(job, JobRecord)
    assert job.job == job_cls_path(Sum)
    assert job.args == {"a": 1, "b": 2}
    assert job.status == JobStatus.ACTIVE
    assert job.source == JobSource.ON_DEMAND
    assert job.max_attempts == 3

    runs = manager.runs(job.id)
    assert len(runs) == 1
    assert isinstance(runs[0], RunRecord)
    assert runs[0].status == RunStatus.PENDING


def test_enqueue_rejects_non_serializable(manager):
    class NotSerializable(Job):
        tasks: ClassVar = [Add]

    with pytest.raises(TypeError):
        manager.enqueue(NotSerializable())


def test_plain_job_class_with_custom_serialization(manager):
    class CustomJob(Job):
        tasks: ClassVar = [Add]

        def __init__(self, a: int, b: int) -> None:
            self.a = a
            self.b = b
            self._conn = object()  # transient, never serialized

        def as_dict(self) -> dict:
            return {"a": self.a, "b": self.b}

        @classmethod
        def from_dict(cls, data: dict) -> CustomJob:
            return cls(data["a"], data["b"])

    instance = CustomJob(2, 5)
    assert CustomJob.from_dict(instance.as_dict()).b == 5  # round-trips, drops _conn

    job = manager.enqueue(instance)
    assert job.args == {"a": 2, "b": 5}
    worker = Worker(manager.backend)
    worker.register(job_cls_path(CustomJob), CustomJob)
    worker._tick()
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    assert run.result == {"Add": 7}


def test_enqueue_rejects_empty_chain(manager):
    @dataclass
    class Empty(Job):
        pass

    with pytest.raises(ValueError):
        manager.enqueue(Empty())


def test_worker_executes_run(manager):
    job = manager.enqueue(Sum(2, 3))
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    worker._tick()

    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    assert run.result == {"Add": 5}
    records = manager.tasks(run.id)
    assert len(records) == 1
    assert records[0].status == TaskStatus.SUCCEEDED
    assert records[0].result == 5


def test_timestamps_are_recorded(manager):
    job = manager.enqueue(Sum(1, 1))
    job_record = manager.get(job.id)
    assert job_record.created_at is not None
    assert job_record.updated_at is not None

    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    worker._tick()
    run = manager.runs(job.id)[0]
    assert run.created_at is not None
    assert run.updated_at is not None
    assert run.started_at is not None
    assert run.finished_at is not None

    record = manager.tasks(run.id)[0]
    assert record.created_at is not None
    assert record.updated_at is not None


def test_worker_runs_tasks_in_order_with_shared_ctx(manager):
    job = manager.enqueue(ThreeTask(), max_attempts=1)
    worker = Worker(manager.backend)
    worker.register(job_cls_path(ThreeTask), ThreeTask)
    worker._tick()

    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.FAILED
    records = manager.tasks(run.id)
    assert [r.status for r in records] == [
        TaskStatus.COMPENSATED,
        TaskStatus.COMPENSATED,
        TaskStatus.FAILED,
    ]
    assert records[1].result == "one-two"
    assert run.ctx["state"]["order"] == ["1", "2", "3"]


def test_worker_skips_delayed_run(manager):
    job = manager.enqueue(Sum(1, 1), scheduled_at=datetime.now() + timedelta(hours=1))
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    worker._tick()
    assert manager.runs(job.id)[0].status == RunStatus.PENDING


def test_worker_retries_task_with_job_max_attempts(manager):
    job = manager.enqueue(FlakyJob(), max_attempts=4)
    worker = Worker(manager.backend)
    worker.register(job_cls_path(FlakyJob), FlakyJob)

    worker._tick()
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.PENDING
    assert run.scheduled_at is not None

    make_due(manager.backend, run.id)
    worker._tick()
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    record = manager.tasks(run.id)[0]
    assert record.attempts == 2  # retried in place, never re-run


def test_failing_chain_compensates_in_reverse(manager):
    UNDONE.clear()

    @dataclass
    class Bad(Job):
        tasks: ClassVar = [Reserve, Charge, AlwaysFails]

    bad_job = manager.enqueue(Bad(), max_attempts=1)
    worker = Worker(manager.backend, retry_backoff=2.0)
    worker.register(job_cls_path(Bad), Bad)
    worker._tick()
    bad_run = manager.runs(bad_job.id)[0]
    assert bad_run.status == RunStatus.FAILED
    assert "always" in bad_run.error
    assert UNDONE == ["charge", "reserve"]
    assert [s.status for s in manager.tasks(bad_run.id)] == [
        TaskStatus.COMPENSATED,
        TaskStatus.COMPENSATED,
        TaskStatus.FAILED,
    ]

    # manual saga undo compensates a succeeded run in reverse
    job = manager.enqueue(Saga())
    worker2 = Worker(manager.backend)
    worker2.register(job_cls_path(Saga), Saga)
    worker2._tick()
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED

    UNDONE.clear()
    manager.undo(job.id)
    assert UNDONE == ["charge", "reserve"]
    assert [s.status for s in manager.tasks(run.id)] == [
        TaskStatus.COMPENSATED,
        TaskStatus.COMPENSATED,
    ]


def test_task_timeout_fails_task(manager):
    _release.clear()
    job = manager.enqueue(SlowJob())
    worker = Worker(manager.backend)
    worker.register(job_cls_path(SlowJob), SlowJob)
    start = time.time()
    worker._tick()
    elapsed = time.time() - start
    assert elapsed < 3
    run = manager.runs(job.id)[0]
    record = manager.tasks(run.id)[0]
    assert record.status == TaskStatus.FAILED
    assert "timeout" in record.error.lower()
    _release.set()


def test_cancel_job(manager):
    job = manager.enqueue(Sum(1, 1))
    manager.cancel(job.id)
    assert manager.get(job.id).status == JobStatus.CANCELLED
    assert manager.runs(job.id)[0].status == RunStatus.CANCELLED


def test_cooperative_cancel(manager):
    job = manager.enqueue(BlockingJob())
    worker = Worker(manager.backend, lease_seconds=3)
    worker.register(job_cls_path(BlockingJob), BlockingJob)

    thread = threading.Thread(target=worker.run_forever)
    thread.start()
    try:
        deadline = time.time() + 10
        while manager.runs(job.id)[0].status != RunStatus.RUNNING and time.time() < deadline:
            time.sleep(0.02)
        manager.cancel(job.id)
        while manager.runs(job.id)[0].status != RunStatus.CANCELLED and time.time() < deadline:
            time.sleep(0.02)
        time.sleep(1.5)  # heartbeat delivers the flag; task aborts
        assert manager.get(job.id).status == JobStatus.CANCELLED
        assert manager.tasks(manager.runs(job.id)[0].id)[0].status == TaskStatus.CANCELLED
    finally:
        worker.stop()
        thread.join(timeout=5)


def test_worker_resumes_after_crash(manager):
    # Resumable1 succeeds, Resumable2 fails -> run pending; a fresh claim must
    # resume at Resumable2 (never re-running Resumable1).
    job = manager.enqueue(Resumable(), max_attempts=2)
    run = manager.runs(job.id)[0]
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Resumable), Resumable)
    worker._tick()
    records = manager.tasks(run.id)
    assert records[0].status == TaskStatus.SUCCEEDED
    assert records[1].status == TaskStatus.FAILED

    make_due(manager.backend, run.id)
    worker._tick()  # resumes at Resumable2; succeeds; AlwaysFails fails once
    records = manager.tasks(run.id)
    assert records[0].status == TaskStatus.SUCCEEDED  # never re-run
    assert records[0].attempts == 1
    assert records[1].status == TaskStatus.SUCCEEDED
    assert records[1].attempts == 2
    assert manager.runs(job.id)[0].status == RunStatus.PENDING

    make_due(manager.backend, run.id)
    worker._tick()  # AlwaysFails exhausts -> compensate -> failed
    records = manager.tasks(run.id)
    assert records[0].status == TaskStatus.COMPENSATED
    assert records[1].status == TaskStatus.COMPENSATED
    assert records[2].status == TaskStatus.FAILED
    assert manager.runs(job.id)[0].status == RunStatus.FAILED


def test_worker_fails_when_class_cannot_resolve(manager, monkeypatch):
    job = manager.enqueue(Sum(1, 1))
    worker = Worker(manager.backend)
    monkeypatch.setattr("pyreljob.core.worker.resolve_job", lambda name: None)
    worker._tick()
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.FAILED
    assert "No class registered" in run.error


def test_worker_resolves_class_by_dotted_path(manager, monkeypatch):
    monkeypatch.setattr(Sum, "__module__", "demo_chains")
    demo = sys.modules.get("demo_chains")
    if demo is None:
        import types

        demo = types.ModuleType("demo_chains")
        sys.modules["demo_chains"] = demo
    demo.Sum = Sum

    job = manager.enqueue(Sum(4, 5))
    worker = Worker(manager.backend)
    worker._tick()
    assert manager.runs(job.id)[0].result == {"Add": 9}


def test_schedule_is_idempotent(manager):
    a = manager.schedule(Saga(), "* * * * *")
    b = manager.schedule(Saga(), "* * * * *")
    assert a.id == b.id
    assert manager.runs(a.id) == []  # no run until the cron fires


def test_beat_creates_run_for_maintained_job(manager):
    job = manager.schedule(Saga(), "* * * * *")

    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=datetime.now() - timedelta(seconds=1))
        )

    manager.tick()
    manager.tick()  # no double-fire
    runs = manager.runs(job.id)
    assert len(runs) == 1
    assert runs[0].status == RunStatus.PENDING
    assert manager.get(job.id).next_run_at is not None


def test_self_scheduling_job(manager):
    # No cron: fires now, then re-arms itself via Job.next_runtime.
    job = manager.schedule(Poller())
    manager.tick()
    assert len(manager.runs(job.id)) == 1

    worker = Worker(manager.backend)
    worker.register(job_cls_path(Poller), Poller)
    worker._tick()  # executes run 1 -> re-arms next_runtime from the run's finish time
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    scheduled = manager.get(job.id).next_run_at
    assert scheduled is not None and scheduled > datetime.now()

    # Force the re-armed time due; the beat fires run 2.
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=datetime.now() - timedelta(seconds=1))
        )
    manager.tick()
    assert len(manager.runs(job.id)) == 2


def test_beat_misfire_grace_skips_stale(manager):
    job = manager.schedule(Saga(), "* * * * *")
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=datetime.now() - timedelta(hours=2))
        )
    manager.tick(misfire_grace_seconds=30)
    assert manager.runs(job.id) == []


def test_cancel_stops_future_scheduled_runs(manager):
    job = manager.schedule(Saga(), "* * * * *")
    manager.cancel(job.id)
    assert manager.get(job.id).status == JobStatus.CANCELLED
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=datetime.now() - timedelta(seconds=1))
        )
    manager.tick()
    assert manager.runs(job.id) == []


def test_idempotency_key_dedupes(manager):
    a = manager.enqueue(Saga(), idempotency_key="saga-1")
    b = manager.enqueue(Saga(), idempotency_key="saga-1")
    assert a.id == b.id
    assert len(manager.runs(a.id)) == 1


def test_counts_by_queue(manager):
    manager.enqueue(Sum(1, 1), queue="a")
    manager.enqueue(Sum(1, 1), queue="b")
    assert manager.counts("a") == {"pending": 1}


def test_claim_reclaims_expired_lease(manager):
    _release.clear()
    job = manager.enqueue(SlowJob())
    run = manager.runs(job.id)[0]
    worker = Worker(manager.backend)
    worker.register(job_cls_path(SlowJob), SlowJob)

    thread = threading.Thread(target=worker._tick)
    thread.start()
    time.sleep(0.2)
    assert manager.runs(job.id)[0].status == RunStatus.RUNNING

    assert manager.backend.claim("worker-2", lease_seconds=1) is None  # lease alive

    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(RunModel)
            .where(RunModel.id == run.id)
            .values(locked_at=datetime.now() - timedelta(seconds=5))
        )

    reclaimed = manager.backend.claim("worker-2", lease_seconds=1)
    assert reclaimed is not None
    assert reclaimed.id == run.id
    assert reclaimed.worker_id == "worker-2"

    _release.set()
    thread.join(timeout=5)


def test_renew_lease(manager):
    _release.clear()
    job = manager.enqueue(SlowJob())
    run = manager.runs(job.id)[0]
    worker = Worker(manager.backend)
    worker.register(job_cls_path(SlowJob), SlowJob)

    thread = threading.Thread(target=worker._tick)
    thread.start()
    time.sleep(0.2)

    assert manager.backend.renew_lease(run.id, worker.worker_id) is True
    assert manager.backend.renew_lease(run.id, "some-other-worker") is False

    _release.set()
    thread.join(timeout=5)


def test_retry_backoff_uses_jitter(manager, monkeypatch):
    import pyreljob.core.worker as worker_mod

    monkeypatch.setattr(worker_mod.random, "uniform", lambda a, b: 0.5)

    job = manager.enqueue(FlakyJob())
    worker = Worker(manager.backend, retry_backoff=2.0)
    worker.register(job_cls_path(FlakyJob), FlakyJob)
    worker._tick()

    after = manager.runs(job.id)[0]
    assert after.status == RunStatus.PENDING
    expected = datetime.now() + timedelta(seconds=2**1 + 0.5)
    assert abs((after.scheduled_at - expected).total_seconds()) < 1.0


def test_migration_upgrades_legacy_v1_schema(tmp_path):
    url = f"sqlite:///{tmp_path}/legacy.db"
    engine = create_engine(url)
    from pyreljob.migrations.runner import MigrationRunner
    from pyreljob.migrations.versions import MIGRATIONS

    MigrationRunner(engine, [m for m in MIGRATIONS if m.version <= 3]).up()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO jobs (name, queue, status, payload, schedule) "
                "VALUES ('app.Old', 'default', 'pending', '{\"a\":\"x\"}', '0 2 * * *')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO jobs (name, queue, status, payload) "
                "VALUES ('app.OneOff', 'default', 'succeeded', '{\"b\":\"y\"}')"
            )
        )

    manager = JobManager(url)
    assert manager.migrate() == [
        "job entities, runs, tasks",
        "add created_at/updated_at timestamps",
    ]

    with manager.backend._engine.connect() as conn:
        jobs = conn.execute(text("SELECT job, source, cron, status FROM jobs")).all()
        assert jobs == [
            ("app.Old", "scheduled", "0 2 * * *", "active"),
            ("app.OneOff", "on_demand", None, "active"),
        ]
        runs = conn.execute(text("SELECT job_id, status FROM runs")).all()
        assert runs == [(1, "pending"), (2, "succeeded")]
        columns = {c["name"] for c in __import__("sqlalchemy").inspect(conn).get_columns("jobs")}
        assert {"job", "args", "source", "cron", "idempotency_key", "next_run_at"} <= columns
        assert not {"name", "payload", "schedule", "worker_id", "locked_at"} & columns
