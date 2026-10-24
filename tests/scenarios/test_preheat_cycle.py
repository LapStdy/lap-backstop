"""场景：预热把环境准备移出关键路径，执行在计划时间准点开始。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import EnvironmentState, RunState
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
SCHEDULED_AT = T0 + timedelta(hours=1)
PREHEAT = timedelta(minutes=10)
KEEP_WARM = timedelta(minutes=30)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
SCHEDULE_ID = "schedule-1"


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": "slow", "slow_probes": 2}),
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


def insert_schedule(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, window_minutes, "
        "coalesce, priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
        "job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每小时', 'interval', '60', NULL, 'UTC', 'run_once', 120, 1, 0, 180, "
        "?, ?, ?, 1, ?, ?, ?)",
        (
            SCHEDULE_ID,
            int(PREHEAT.total_seconds() // 60),
            int(KEEP_WARM.total_seconds() // 60),
            TEMPLATE_ID,
            SCHEDULED_AT.isoformat(),
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


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    insert_template(connection)
    insert_schedule(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    try:
        yield connection, clock, runs, tmp_path / "data"
    finally:
        connection.close()


def test_preheat_creates_run_only_when_trigger_time_arrives(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    supervisor = Supervisor(connection, clock, make_registry(), data_root=data_root)
    clock.jump_to(SCHEDULED_AT - PREHEAT - timedelta(seconds=1))
    supervisor.tick()
    assert runs.non_terminal() == ()

    clock.jump_to(SCHEDULED_AT - PREHEAT)
    supervisor.tick()
    created = runs.non_terminal()
    assert [run.scheduled_at for run in created] == [SCHEDULED_AT]


def test_environment_is_ready_before_scheduled_time_and_executes_on_time(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    supervisor = Supervisor(connection, clock, make_registry(), data_root=data_root)
    clock.jump_to(SCHEDULED_AT - PREHEAT)
    supervisor.tick()

    (waiting,) = runs.non_terminal()
    assert waiting.state is RunState.WAITING
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.READY
    assert LeaseService(connection, clock).active_for_run(waiting.id)

    clock.jump_to(SCHEDULED_AT - timedelta(seconds=1))
    supervisor.tick()
    assert runs.get(waiting.id).state is RunState.WAITING

    clock.jump_to(SCHEDULED_AT)
    supervisor.tick()
    finished = runs.get(waiting.id)
    assert finished.state is RunState.SUCCEEDED
    assert finished.started_at == SCHEDULED_AT
    assert LeaseService(connection, clock).active_for_run(waiting.id) == ()

    environment = EnvironmentService(connection, clock).get(ENV_ID)
    assert environment.state is EnvironmentState.READY
    assert environment.warm_until == SCHEDULED_AT + KEEP_WARM

    executing = [
        event.at
        for event in runs.events(waiting.id)
        if event.type == "run.state.changed" and event.payload.get("to_state") == "executing"
    ]
    assert executing == [SCHEDULED_AT]
