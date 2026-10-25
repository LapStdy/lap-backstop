"""场景：核心重启后按持久化状态与最后事件重建运行。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import RunState
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from core.worker.executor import WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)

_STABLE_DISK_FREE = 100 * 1024**3
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
SCHEDULED_AT = T0 + timedelta(minutes=30)


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": "instant"}),
            json.dumps(
                {
                    "id": "fake-policy",
                    "verifier_plugin_id": "fake",
                    "required_evidence": ["log"],
                    "params": {},
                }
            ),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


def make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    return registry


def create_run(
    runs: JobRunService,
    *,
    key: str = "run-key",
    scheduled_at: datetime = T0,
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    return record.id


def prepare_run(
    runs: JobRunService,
    *,
    key: str = "run-key",
    scheduled_at: datetime = T0,
) -> str:
    run_id = create_run(runs, key=key, scheduled_at=scheduled_at)
    runs.enqueue(run_id)
    return run_id


def supervisor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
) -> Supervisor:
    return Supervisor(
        connection, clock, make_registry(), data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )


def event_types(runs: JobRunService, run_id: str) -> list[str]:
    return [event.type for event in runs.events(run_id)]


def queued_event_count(runs: JobRunService, run_id: str) -> int:
    return sum(1 for event_type in event_types(runs, run_id) if event_type == "run.queued")


def state_changes(runs: JobRunService, run_id: str) -> list[tuple[str, str, str]]:
    return [
        (
            str(event.payload.get("from_state")),
            str(event.payload.get("to_state")),
            str(event.payload.get("reason")),
        )
        for event in runs.events(run_id)
        if event.type == "run.state.changed"
    ]


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    insert_template(connection)
    clock = VirtualClock(T0)
    try:
        yield connection, clock, JobRunService(connection, clock), tmp_path / "data"
    finally:
        connection.close()


def test_startup_enqueues_scheduled_run_and_executes(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = create_run(runs)
    assert runs.get(run_id).state is RunState.SCHEDULED

    supervisor = supervisor_for(connection, clock, data_root)
    supervisor.startup()
    assert runs.get(run_id).state is RunState.QUEUED
    assert "run.queued" in event_types(runs, run_id)

    supervisor.tick()
    assert runs.get(run_id).state is RunState.SUCCEEDED


def test_startup_keeps_queued_run_in_queue(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(runs)
    supervisor = supervisor_for(connection, clock, data_root)
    supervisor.startup()
    assert runs.get(run_id).state is RunState.QUEUED

    supervisor.tick()
    assert runs.get(run_id).state is RunState.SUCCEEDED


def test_startup_keeps_waiting_run_and_its_lease(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(runs, scheduled_at=SCHEDULED_AT)
    executor = WorkerExecutor(connection, clock, make_registry(), data_root=data_root)
    assert executor.execute(run_id).state is RunState.WAITING
    assert LeaseService(connection, clock).active_for_run(run_id)

    supervisor = supervisor_for(connection, clock, data_root)
    supervisor.startup()
    assert runs.get(run_id).state is RunState.WAITING
    assert LeaseService(connection, clock).active_for_run(run_id)

    for minute in range(5, 30, 5):
        clock.jump_to(T0 + timedelta(minutes=minute))
        supervisor.tick()
    assert runs.get(run_id).state is RunState.WAITING

    clock.jump_to(SCHEDULED_AT)
    supervisor.tick()
    finished = runs.get(run_id)
    assert finished.state is RunState.SUCCEEDED
    assert finished.started_at == SCHEDULED_AT


def test_startup_requeues_due_recovering_run(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(runs)
    runs.transition(run_id, RunState.PREPARING)
    runs.mark_recovering(
        run_id,
        next_attempt=1,
        retry_at=T0 - timedelta(seconds=1),
        error_code="E_SCRIPT_TASK_FAILED",
    )
    supervisor = supervisor_for(connection, clock, data_root)
    supervisor.startup()
    assert runs.get(run_id).state is RunState.QUEUED
    assert queued_event_count(runs, run_id) == 2

    supervisor.tick()
    recovered = runs.get(run_id)
    assert recovered.state is RunState.SUCCEEDED
    assert recovered.attempt == 1


def test_startup_holds_recovering_run_until_retry_time(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(runs)
    runs.transition(run_id, RunState.PREPARING)
    runs.mark_recovering(
        run_id,
        next_attempt=1,
        retry_at=T0 + timedelta(seconds=60),
        error_code="E_SCRIPT_TASK_FAILED",
    )
    supervisor = supervisor_for(connection, clock, data_root)
    supervisor.startup()
    assert runs.get(run_id).state is RunState.RECOVERING
    assert queued_event_count(runs, run_id) == 1

    clock.jump_to(T0 + timedelta(seconds=30))
    supervisor.tick()
    assert runs.get(run_id).state is RunState.RECOVERING

    clock.jump_to(T0 + timedelta(seconds=60))
    supervisor.tick()
    assert runs.get(run_id).state is RunState.SUCCEEDED


def test_startup_marks_orphan_executing_run_unknown_and_releases_lease(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(runs)
    runs.transition(run_id, RunState.PREPARING)
    ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
    LeaseService(connection, clock).acquire(run_id, (ENV_ID,))
    runs.transition(run_id, RunState.EXECUTING)

    supervisor_for(connection, clock, data_root).startup()
    recovered = runs.get(run_id)
    assert recovered.state is RunState.UNKNOWN
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
    assert state_changes(runs, run_id) == [
        ("scheduled", "queued", "enqueue"),
        ("queued", "preparing", ""),
        ("preparing", "executing", ""),
        ("executing", "unknown", "startup_recovery"),
    ]
    assert "run.finished" in event_types(runs, run_id)
