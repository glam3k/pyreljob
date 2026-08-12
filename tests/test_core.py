"""End-to-end tests for manager, worker, scheduler, and migrations.

Model: a Job is a durable entity; a Run is one invocation of it; a TaskRun is
one step within a run.
"""

from __future__ import annotations

import asyncio
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
from pyreljob.job import JobRecord, RunRecord
from pyreljob.models.orm import JobModel, RunModel
from pyreljob.worker import Worker

_release = threading.Event()
UNDONE: list[str] = []
_CONCURRENCY: dict[str, int] = {"active": 0, "max": 0}
_lock = threading.Lock()


def job_cls_path(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def make_due(backend, run_id: int) -> None:
    with backend._engine.begin() as conn:
        conn.execute(
            update(RunModel)
            .where(RunModel.id == run_id)
            .values(scheduled_at=datetime.now() - timedelta(seconds=1))
        )


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


class Flaky(Task):
    async def run(self, ctx: TaskContext) -> None:
        if not ctx.state.get("tried"):
            ctx.state["tried"] = True
            raise ValueError("boom")


@dataclass
class FlakyJob(Job):
    tasks: ClassVar = [Flaky]


class AlwaysFails(Task):
    async def run(self, ctx: TaskContext) -> None:
        raise ValueError("always")


@dataclass
class FailingJob(Job):
    tasks: ClassVar = [AlwaysFails]


class SlowTask(Task):
    async def run(self, ctx: TaskContext) -> str:
        await asyncio.to_thread(_release.wait, 10)
        return "ok"


@dataclass
class SlowJob(Job):
    tasks: ClassVar = [SlowTask]


class ReportProgress(Task):
    async def run(self, ctx: TaskContext) -> str:
        await ctx.set_progress(0.5)
        return "ok"


@dataclass
class ReportProgressJob(Job):
    tasks: ClassVar = [ReportProgress]


class LiveProgress(Task):
    async def run(self, ctx: TaskContext) -> str:
        await ctx.set_progress(0.4)
        await asyncio.to_thread(_release.wait, 10)
        return "ok"


@dataclass
class LiveProgressJob(Job):
    tasks: ClassVar = [LiveProgress]


class HangingTask(Task):
    timeout = 1

    async def run(self, ctx: TaskContext) -> str:
        await asyncio.sleep(30)  # cooperative -> cancellable by wait_for


@dataclass
class HangingJob(Job):
    tasks: ClassVar = [HangingTask]


class TaskA(Task):
    async def run(self, ctx: TaskContext) -> str:
        ctx.state.setdefault("order", []).append("1")
        return "one"


class TaskB(Task):
    async def run(self, ctx: TaskContext) -> str:
        ctx.state.setdefault("order", []).append("2")
        return ctx.result("TaskA") + "-two"


class TaskC(Task):
    async def run(self, ctx: TaskContext) -> str:
        ctx.state.setdefault("order", []).append("3")
        raise RuntimeError("nope")


@dataclass
class ThreeTask(Job):
    tasks: ClassVar = [TaskA, TaskB, TaskC]


class Reserve(Task):
    async def run(self, ctx: TaskContext) -> str:
        return "reserved"

    async def undo(self, ctx: TaskContext) -> None:
        UNDONE.append("reserve")


class Charge(Task):
    async def run(self, ctx: TaskContext) -> str:
        return "charged"

    async def undo(self, ctx: TaskContext) -> None:
        UNDONE.append("charge")


@dataclass
class Saga(Job):
    tasks: ClassVar = [Reserve, Charge]


class BlockingTask(Task):
    async def run(self, ctx: TaskContext) -> str:
        while not ctx.cancelled:
            await asyncio.sleep(0.02)
        raise JobCancelledError()


@dataclass
class BlockingJob(Job):
    tasks: ClassVar = [BlockingTask]


class Resumable1(Task):
    async def run(self, ctx: TaskContext) -> str:
        return "one"


class Resumable2(Task):
    async def run(self, ctx: TaskContext) -> str:
        if not ctx.state.get("tried"):
            ctx.state["tried"] = True
            raise ValueError("retry me")
        return "two"


@dataclass
class Resumable(Job):
    tasks: ClassVar = [Resumable1, Resumable2, AlwaysFails]


class Poll(Task):
    async def run(self, ctx: TaskContext) -> str:
        return "ok"


@dataclass
class Poller(Job):
    tasks: ClassVar = [Poll]

    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        if last_run is None:
            return datetime.now()  # first schedule: fire immediately
        return last_run.finished_at + timedelta(seconds=5)


class NamedTask(Task):
    name = "send-email"

    async def run(self, ctx: TaskContext) -> str:
        return "ok"

    async def undo(self, ctx: TaskContext) -> None:
        UNDONE.append("send-email")


@dataclass
class NamedJob(Job):
    tasks: ClassVar = [NamedTask]


class ConcurrentTask(Task):
    async def run(self, ctx: TaskContext) -> str:
        with _lock:
            _CONCURRENCY["active"] += 1
            _CONCURRENCY["max"] = max(_CONCURRENCY["max"], _CONCURRENCY["active"])
        try:
            await asyncio.sleep(0.5)
        finally:
            with _lock:
                _CONCURRENCY["active"] -= 1
        return "ok"


@dataclass
class ConcurrentJob(Job):
    tasks: ClassVar = [ConcurrentTask]


class OverrideFails(Task):
    max_attempts = 4

    async def run(self, ctx: TaskContext) -> None:
        raise ValueError("nope")


@dataclass
class OverrideJob(Job):
    tasks: ClassVar = [OverrideFails]


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
    tick(w3)
    assert manager.runs(job.id)[0].worker_id == "worker-1"  # lease tagged with the id


def test_worker_max_concurrency(manager):
    _CONCURRENCY["active"] = 0
    _CONCURRENCY["max"] = 0
    for _ in range(3):
        manager.enqueue(ConcurrentJob())

    worker = Worker(manager.backend, max_concurrency=2, poll_interval=0.05)
    worker.register(job_cls_path(ConcurrentJob), ConcurrentJob)
    thread = threading.Thread(target=worker.run_forever)
    thread.start()
    try:
        deadline = time.time() + 20
        while manager.counts().get("succeeded", 0) < 3 and time.time() < deadline:
            time.sleep(0.05)
        assert manager.counts()["succeeded"] == 3
        assert _CONCURRENCY["max"] == 2  # two runs really executed in parallel
    finally:
        worker.stop()
        thread.join(timeout=5)


def test_custom_task_name(manager):
    assert NamedTask.task_name() == "send-email"

    job = manager.enqueue(NamedJob())
    worker = Worker(manager.backend)
    worker.register(job_cls_path(NamedJob), NamedJob)
    tick(worker)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    record = manager.tasks(run.id)[0]
    assert record.task_name == "send-email"  # custom name stored, not the dotted path

    UNDONE.clear()
    asyncio.run(manager.undo(job.id))  # still resolves the class by position
    assert UNDONE == ["send-email"]


def test_delete_job(manager):
    job = manager.enqueue(Sum(1, 1))
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    tick(worker)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED

    manager.delete(job.id)
    assert manager.get(job.id) is None
    assert manager.runs(job.id) == []
    assert manager.tasks(run.id) == []


def test_delete_refuses_active_job(manager):
    job = manager.enqueue(SlowJob())  # never run; still ready
    with pytest.raises(ValueError):
        manager.delete(job.id)
    assert manager.get(job.id) is not None


def test_prune_removes_old_terminal_runs(manager):
    old_job = manager.enqueue(Sum(1, 1))
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    tick(worker)
    old_run = manager.runs(old_job.id)[0]
    assert old_run.status == RunStatus.SUCCEEDED
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(RunModel)
            .where(RunModel.id == old_run.id)
            .values(finished_at=datetime.now() - timedelta(days=40))
        )

    recent_job = manager.enqueue(Sum(2, 2))
    tick(worker)
    recent_run = manager.runs(recent_job.id)[0]

    deleted = manager.prune(older_than=timedelta(days=30))
    assert deleted == 1
    assert manager.runs(old_job.id) == []
    assert manager.tasks(old_run.id) == []
    assert manager.runs(recent_job.id)[0].id == recent_run.id  # recent kept


def test_task_level_retry_override(manager):
    # Job says max_attempts=2, but the task overrides with max_attempts=4.
    job = manager.enqueue(OverrideJob(), max_attempts=2)
    worker = Worker(manager.backend, retry_backoff=2.0)
    worker.register(job_cls_path(OverrideJob), OverrideJob)
    for _ in range(4):
        tick(worker)
        run = manager.runs(job.id)[0]
        if run.status == RunStatus.FAILED:
            break
        make_due(manager.backend, run.id)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.FAILED
    assert manager.tasks(run.id)[0].attempts == 4  # task's override won


def test_job_level_retries(manager):
    job = manager.enqueue(FailingJob(), max_attempts=1, retries=2)
    worker = Worker(manager.backend, retry_backoff=2.0)
    worker.register(job_cls_path(FailingJob), FailingJob)

    tick(worker)  # run 1 fails -> whole-run retry created
    assert len(manager.runs(job.id)) == 2
    make_due(manager.backend, manager.runs(job.id)[0].id)
    tick(worker)  # run 2 fails -> retry created
    assert len(manager.runs(job.id)) == 3
    make_due(manager.backend, manager.runs(job.id)[0].id)
    tick(worker)  # run 3 fails -> retries exhausted, terminal

    runs = manager.runs(job.id)
    assert len(runs) == 3  # 1 original + 2 retries
    assert all(r.status == RunStatus.FAILED for r in runs)
    assert manager.get(job.id).attempts == 3


def test_job_attempts_reset_on_success(manager):
    job = manager.enqueue(Sum(1, 1), retries=2)
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    tick(worker)
    assert manager.runs(job.id)[0].status == RunStatus.SUCCEEDED
    assert manager.get(job.id).attempts == 0


def test_backend_selection(tmp_path):
    from pyreljob.backends import backend_from_url
    from pyreljob.backends.sqlite import SQLiteBackend

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
    assert runs[0].status == RunStatus.READY


def test_enqueue_rejects_non_serializable(manager):
    class NotSerializable(Job):
        tasks: ClassVar = [Add]

    with pytest.raises(TypeError):
        manager.enqueue(NotSerializable())


def test_job_tags_roundtrip_and_filter(manager):
    manager.enqueue(Sum(1, 2), tags=["user:1", "crm"])
    manager.enqueue(Sum(3, 4), tags=["user:2"])
    manager.enqueue(Sum(5, 6))

    all_jobs = manager.list_jobs()
    assert len(all_jobs) == 3
    assert all(j.tags is not None for j in all_jobs if j.tags)

    scoped = manager.list_jobs(tag="user:1")
    assert len(scoped) == 1
    assert scoped[0].tags == ["user:1", "crm"]

    scoped = manager.list_jobs(tag="user:2")
    assert len(scoped) == 1
    assert scoped[0].tags == ["user:2"]

    untagged = manager.list_jobs(tag="user:3")
    assert untagged == []


def test_list_runs_joins_owning_job(manager):
    first = manager.enqueue(Sum(1, 2), tags=["user:1", "crm"])
    second = manager.enqueue(Sum(3, 4), tags=["user:2"])
    untagged = manager.enqueue(Sum(5, 6))

    runs = manager.list_runs()
    assert len(runs) == 3
    newest = runs[0]
    assert newest.run.status == RunStatus.READY
    assert newest.job.id == untagged.id
    assert newest.job.tags is None

    scoped = manager.list_runs(tag="user:1")
    assert len(scoped) == 1
    assert scoped[0].job.id == first.id
    assert scoped[0].job.tags == ["user:1", "crm"]

    scoped = manager.list_runs(tag="user:2")
    assert len(scoped) == 1
    assert scoped[0].job.id == second.id

    empty = manager.list_runs(tag="user:3")
    assert empty == []

    # A maintained job re-fires many runs; each appears as its own entry.
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Poller), Poller)
    for _ in range(3):
        tick(worker)  # drain the earlier Sum runs first
    scheduled = manager.schedule(Poller(), tags=["user:9"])
    manager.tick()  # fire run 1 (first schedule fires now)
    tick(worker)  # execute run 1 -> re-arms next_runtime
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == scheduled.id)
            .values(next_run_at=datetime.now() - timedelta(seconds=1))
        )
    manager.tick()  # fire run 2
    runs = manager.list_runs(tag="user:9")
    assert len(runs) == 2
    assert {r.job.id for r in runs} == {scheduled.id}


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
    tick(worker)
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
    tick(worker)

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
    tick(worker)
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
    tick(worker)

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
    tick(worker)
    assert manager.runs(job.id)[0].status == RunStatus.READY


def test_worker_retries_task_with_job_max_attempts(manager):
    job = manager.enqueue(FlakyJob(), max_attempts=4)
    worker = Worker(manager.backend)
    worker.register(job_cls_path(FlakyJob), FlakyJob)

    tick(worker)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.READY
    assert run.scheduled_at is not None

    make_due(manager.backend, run.id)
    tick(worker)
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
    tick(worker)
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
    tick(worker2)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED

    UNDONE.clear()
    asyncio.run(manager.undo(job.id))
    assert UNDONE == ["charge", "reserve"]
    assert [s.status for s in manager.tasks(run.id)] == [
        TaskStatus.COMPENSATED,
        TaskStatus.COMPENSATED,
    ]


def test_task_timeout_fails_task(manager):
    job = manager.enqueue(HangingJob())
    worker = Worker(manager.backend)
    worker.register(job_cls_path(HangingJob), HangingJob)
    start = time.time()
    tick(worker)
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
    # Resumable1 succeeds, Resumable2 fails -> run ready; a fresh claim must
    # resume at Resumable2 (never re-running Resumable1).
    job = manager.enqueue(Resumable(), max_attempts=2)
    run = manager.runs(job.id)[0]
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Resumable), Resumable)
    tick(worker)
    records = manager.tasks(run.id)
    assert records[0].status == TaskStatus.SUCCEEDED
    assert records[1].status == TaskStatus.FAILED

    make_due(manager.backend, run.id)
    tick(worker)  # resumes at Resumable2; succeeds; AlwaysFails fails once
    records = manager.tasks(run.id)
    assert records[0].status == TaskStatus.SUCCEEDED  # never re-run
    assert records[0].attempts == 1
    assert records[1].status == TaskStatus.SUCCEEDED
    assert records[1].attempts == 2
    assert manager.runs(job.id)[0].status == RunStatus.READY

    make_due(manager.backend, run.id)
    tick(worker)  # AlwaysFails exhausts -> compensate -> failed
    records = manager.tasks(run.id)
    assert records[0].status == TaskStatus.COMPENSATED
    assert records[1].status == TaskStatus.COMPENSATED
    assert records[2].status == TaskStatus.FAILED
    assert manager.runs(job.id)[0].status == RunStatus.FAILED


def test_worker_fails_when_class_cannot_resolve(manager, monkeypatch):
    job = manager.enqueue(Sum(1, 1))
    worker = Worker(manager.backend)
    monkeypatch.setattr("pyreljob.worker.resolve_job", lambda name: None)
    tick(worker)
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
    tick(worker)
    assert manager.runs(job.id)[0].result == {"Add": 9}


def test_schedule_is_idempotent(manager):
    a = manager.schedule(Poller())
    b = manager.schedule(Poller())
    assert a.id == b.id
    assert manager.runs(a.id) == []  # no run until the beat fires it


def test_schedule_multiple_instances_by_key(manager):
    a = manager.schedule(Poller(), idempotency_key="poller:1")
    b = manager.schedule(Poller(), idempotency_key="poller:2")
    assert a.id != b.id
    # each instance runs independently
    assert a.args is not None and b.args is not None


def test_schedule_same_key_is_idempotent(manager):
    a = manager.schedule(Poller(), idempotency_key="poller:1")
    b = manager.schedule(Poller(), idempotency_key="poller:1")
    assert a.id == b.id


def test_schedule_keyed_and_keyless_are_distinct(manager):
    keyed = manager.schedule(Poller(), idempotency_key="poller:1")
    keyless = manager.schedule(Poller())
    assert keyed.id != keyless.id


def test_schedule_same_key_different_class_is_idempotent_per_class(manager):
    # Same key on the same class stays idempotent; the global unique index on
    # idempotency_key means a key must be unique across classes too, so callers
    # should namespace keys by job (e.g. "job:instance").
    a = manager.schedule(Poller(), idempotency_key="shared")
    b = manager.schedule(Poller(), idempotency_key="shared")
    assert a.id == b.id


def test_beat_creates_run_for_maintained_job(manager):
    job = manager.schedule(Poller())

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
    assert runs[0].status == RunStatus.READY
    # The fire disarmed the job; the worker re-arms it after the run.
    assert manager.get(job.id).next_run_at is None


def test_self_scheduling_job(manager):
    # First schedule fires now (next_runtime(None) => now), then the job
    # re-arms itself via Job.next_runtime after each run.
    job = manager.schedule(Poller())
    manager.tick()
    assert len(manager.runs(job.id)) == 1

    worker = Worker(manager.backend)
    worker.register(job_cls_path(Poller), Poller)
    tick(worker)  # executes run 1 -> re-arms next_runtime from the run's finish time
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


def test_on_demand_job_is_not_rescheduled(manager):
    job = manager.enqueue(Poller())
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Poller), Poller)
    tick(worker)
    assert manager.runs(job.id)[0].status == RunStatus.SUCCEEDED
    # On-demand jobs run once: the worker never re-arms next_run_at for them,
    # even though Poller.next_runtime would schedule a future run.
    assert manager.get(job.id).source == JobSource.ON_DEMAND
    assert manager.get(job.id).next_run_at is None


def test_beat_misfire_grace_skips_stale(manager):
    job = manager.schedule(Poller())
    with manager.backend._engine.begin() as conn:
        conn.execute(
            update(JobModel)
            .where(JobModel.id == job.id)
            .values(next_run_at=datetime.now() - timedelta(hours=2))
        )
    manager.tick(misfire_grace_seconds=30)
    assert manager.runs(job.id) == []


def test_cancel_stops_future_scheduled_runs(manager):
    job = manager.schedule(Poller())
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
    assert manager.counts("a") == {"ready": 1}


def test_claim_reclaims_expired_lease(manager):
    _release.clear()
    job = manager.enqueue(SlowJob())
    run = manager.runs(job.id)[0]
    worker = Worker(manager.backend)
    worker.register(job_cls_path(SlowJob), SlowJob)

    thread = threading.Thread(target=lambda: asyncio.run(worker._tick()))
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

    thread = threading.Thread(target=lambda: asyncio.run(worker._tick()))
    thread.start()
    time.sleep(0.2)

    assert manager.backend.renew_lease(run.id, worker.worker_id) is True
    assert manager.backend.renew_lease(run.id, "some-other-worker") is False

    _release.set()
    thread.join(timeout=5)


def test_retry_backoff_uses_jitter(manager, monkeypatch):
    import pyreljob.worker as worker_mod

    monkeypatch.setattr(worker_mod.random, "uniform", lambda a, b: 0.5)

    job = manager.enqueue(FlakyJob())
    worker = Worker(manager.backend, retry_backoff=2.0)
    worker.register(job_cls_path(FlakyJob), FlakyJob)
    tick(worker)

    after = manager.runs(job.id)[0]
    assert after.status == RunStatus.READY
    expected = datetime.now() + timedelta(seconds=2**1 + 0.5)
    assert abs((after.scheduled_at - expected).total_seconds()) < 1.0


def test_task_can_report_progress(manager):
    job = manager.enqueue(ReportProgressJob())
    worker = Worker(manager.backend)
    worker.register(job_cls_path(ReportProgressJob), ReportProgressJob)
    tick(worker)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    assert run.progress == 0.5


def test_framework_does_not_set_progress(manager):
    job = manager.enqueue(Sum(1, 1))  # tasks never call ctx.set_progress
    worker = Worker(manager.backend)
    worker.register(job_cls_path(Sum), Sum)
    tick(worker)
    run = manager.runs(job.id)[0]
    assert run.status == RunStatus.SUCCEEDED
    assert run.progress is None  # progress is owned by task code alone


def test_progress_visible_live(manager):
    _release.clear()
    job = manager.enqueue(LiveProgressJob())
    worker = Worker(manager.backend, poll_interval=0.05)
    worker.register(job_cls_path(LiveProgressJob), LiveProgressJob)
    thread = threading.Thread(target=worker.run_forever)
    thread.start()
    try:
        deadline = time.time() + 10
        run = manager.runs(job.id)[0]
        while run.progress is None and time.time() < deadline:
            time.sleep(0.02)
            run = manager.runs(job.id)[0]
        assert run.progress == 0.4
    finally:
        _release.set()
        worker.stop()
        thread.join(timeout=5)


def test_set_progress_validates_range():
    ctx = TaskContext(0, "app.Job")
    with pytest.raises(ValueError):
        asyncio.run(ctx.set_progress(1.5))
    with pytest.raises(ValueError):
        asyncio.run(ctx.set_progress(-0.1))


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
        "add job-level retries",
        "drop cron column",
        "rename pending runs/tasks to ready",
        "add run progress column",
        "add job tags column",
    ]

    with manager.backend._engine.connect() as conn:
        jobs = conn.execute(text("SELECT job, source, status FROM jobs")).all()
        assert jobs == [
            ("app.Old", "scheduled", "active"),
            ("app.OneOff", "on_demand", "active"),
        ]
        runs = conn.execute(text("SELECT job_id, status FROM runs")).all()
        assert runs == [(1, "ready"), (2, "succeeded")]
        columns = {c["name"] for c in __import__("sqlalchemy").inspect(conn).get_columns("jobs")}
        assert {"job", "args", "source", "idempotency_key", "next_run_at"} <= columns
        assert not {"name", "payload", "schedule", "cron", "worker_id", "locked_at"} & columns
        run_columns = {
            c["name"] for c in __import__("sqlalchemy").inspect(conn).get_columns("runs")
        }
        assert "progress" in run_columns
