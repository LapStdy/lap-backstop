"""场景：启动恢复按事件流回放重建运行状态，流损坏时按表状态兜底。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from contracts import RunState
from core.clock import VirtualClock
from core.events.replay import replay_run
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"


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


def create_run(runs: JobRunService) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key="run-key",
            priority=0,
            deadline_at=None,
        )
    )
    return record.id


def drive_to_executing(runs: JobRunService, run_id: str) -> None:
    runs.enqueue(run_id)
    runs.transition(run_id, RunState.PREPARING)
    runs.transition(run_id, RunState.EXECUTING)


def supervisor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
) -> Supervisor:
    return Supervisor(connection, clock, make_registry(), data_root=data_root)


def last_state_change(runs: JobRunService, run_id: str) -> tuple[str, str, str] | None:
    for event in reversed(runs.events(run_id)):
        if event.type == "run.state.changed":
            return (
                str(event.payload.get("from_state")),
                str(event.payload.get("to_state")),
                str(event.payload.get("reason")),
            )
    return None


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


def test_replay_rebuilds_completed_run_from_stream(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = create_run(runs)
    supervisor = supervisor_for(connection, clock, data_root)
    supervisor.startup()
    supervisor.tick()

    assert runs.get(run_id).state is RunState.SUCCEEDED
    snapshot = replay_run(runs.events(run_id))
    assert snapshot.state is RunState.SUCCEEDED
    assert snapshot.phases == {
        "prepare": "ok",
        "execute": "ok",
        "verify": "ok",
        "cleanup": "ok",
    }
    assert snapshot.verdict_status == "completed"
    assert snapshot.finished is True
    assert snapshot.attempt == runs.get(run_id).attempt


def test_startup_decides_from_replayed_stream_when_table_lags(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = create_run(runs)
    drive_to_executing(runs, run_id)
    connection.execute("UPDATE job_runs SET state = 'queued' WHERE id = ?", (run_id,))

    supervisor_for(connection, clock, data_root).startup()

    assert runs.get(run_id).state is RunState.QUEUED
    assert last_state_change(runs, run_id) == ("preparing", "executing", "")
    assert replay_run(runs.events(run_id)).state is RunState.EXECUTING


def test_startup_leaves_stream_intact_when_enqueue_path_torn(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = create_run(runs)
    drive_to_executing(runs, run_id)
    connection.execute("DELETE FROM run_events WHERE run_id = ? AND seq > 1", (run_id,))

    supervisor_for(connection, clock, data_root).startup()

    assert runs.get(run_id).state is RunState.EXECUTING
    assert replay_run(runs.events(run_id)).state is RunState.SCHEDULED


def test_startup_falls_back_to_table_state_when_stream_broken(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = create_run(runs)
    drive_to_executing(runs, run_id)
    connection.execute("DELETE FROM run_events WHERE run_id = ? AND seq = 2", (run_id,))

    supervisor_for(connection, clock, data_root).startup()

    assert runs.get(run_id).state is RunState.UNKNOWN
