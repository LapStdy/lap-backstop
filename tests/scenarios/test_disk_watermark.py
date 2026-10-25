"""场景：Supervisor tick 上的三级磁盘水位与 serve 循环的退出码 5（docs/08）。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import RunState
from core.__main__ import EXIT_DISK_LOW
from core.clock import VirtualClock
from core.hostops.serve import HeartbeatWriter, ServeLoop, ServeOptions
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import (
    DISK_CRITICAL_EVENT,
    DISK_FULL_GUARD_EVENT,
    DISK_LOW_EVENT,
    Supervisor,
)
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
GIB = 1024**3
MIB = 1024**2
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
SCHEDULE_ID = "schedule-1"


@pytest.fixture
def env(tmp_path: Path) -> Iterator[tuple[sqlite3.Connection, VirtualClock, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    try:
        yield connection, clock, tmp_path / "data"
    finally:
        connection.close()


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


def insert_schedule(connection: sqlite3.Connection, *, next_run_at: str) -> None:
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, window_minutes, "
        "coalesce, priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
        "job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每小时', 'interval', '60', NULL, 'UTC', 'run_once', 120, 1, 0, 180, 0, 0, "
        "?, 1, ?, ?, ?)",
        (SCHEDULE_ID, TEMPLATE_ID, next_run_at, T0.isoformat(), T0.isoformat()),
    )


def supervisor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    free: dict[str, int],
) -> Supervisor:
    return Supervisor(
        connection,
        clock,
        make_registry(),
        data_root=data_root,
        disk_free=lambda: free["bytes"],
    )


def disk_events(connection: sqlite3.Connection, name: str) -> list[dict[str, object]]:
    """全局事件里指定事件名的载荷列表，按 seq 顺序。"""
    rows = connection.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    found: list[dict[str, object]] = []
    for row in rows:
        record = json.loads(str(row[0]))
        if record.get("event") == name:
            payload = record["payload"]
            assert isinstance(payload, dict)
            found.append(payload)
    return found


def run_count(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) FROM job_runs").fetchone()
    assert row is not None
    return int(row[0])


def test_warn_emits_disk_low_once_until_recovery(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    free = {"bytes": 4 * GIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    assert disk_events(connection, DISK_LOW_EVENT) == [
        {"free_bytes": 4 * GIB, "threshold_bytes": 5 * GIB}
    ]
    supervisor.tick()
    assert len(disk_events(connection, DISK_LOW_EVENT)) == 1
    free["bytes"] = 6 * GIB
    supervisor.tick()
    free["bytes"] = 4 * GIB
    supervisor.tick()
    assert len(disk_events(connection, DISK_LOW_EVENT)) == 2


def test_warn_boundary_at_five_gib_exactly_is_quiet(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    free = {"bytes": 5 * GIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    assert disk_events(connection, DISK_LOW_EVENT) == []
    free["bytes"] = 5 * GIB - 1
    supervisor.tick()
    assert len(disk_events(connection, DISK_LOW_EVENT)) == 1


def test_guard_emits_disk_full_guard_once_with_error_code(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    free = {"bytes": 1500 * MIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    assert disk_events(connection, DISK_FULL_GUARD_EVENT) == [
        {
            "free_bytes": 1500 * MIB,
            "threshold_bytes": 2 * GIB,
            "error_code": "E_DISK",
        }
    ]
    supervisor.tick()
    assert len(disk_events(connection, DISK_FULL_GUARD_EVENT)) == 1
    free["bytes"] = 2500 * MIB
    supervisor.tick()
    # 回到 [2 GB, 5 GB) 区间属于「低于 5 GB」：首次进入告警档，warn 边沿补记一次
    assert disk_events(connection, DISK_LOW_EVENT) == [
        {"free_bytes": 2500 * MIB, "threshold_bytes": 5 * GIB}
    ]
    free["bytes"] = 1500 * MIB
    supervisor.tick()
    assert len(disk_events(connection, DISK_FULL_GUARD_EVENT)) == 2
    assert len(disk_events(connection, DISK_LOW_EVENT)) == 1


def test_guard_boundary_at_two_gib_exactly_is_allowed(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    free = {"bytes": 2 * GIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    assert disk_events(connection, DISK_FULL_GUARD_EVENT) == []


def test_critical_boundary_at_500_mib_is_guard_level(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    free = {"bytes": 500 * MIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    assert not supervisor.is_disk_critical
    assert disk_events(connection, DISK_CRITICAL_EVENT) == []
    assert len(disk_events(connection, DISK_FULL_GUARD_EVENT)) == 1
    free["bytes"] = 500 * MIB - 1
    supervisor.tick()
    assert supervisor.is_disk_critical
    assert disk_events(connection, DISK_CRITICAL_EVENT) == [
        {"free_bytes": 500 * MIB - 1, "threshold_bytes": 500 * MIB}
    ]


def test_critical_ticks_emit_once_and_skip_scheduling(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    insert_template(connection)
    insert_schedule(connection, next_run_at=T0.isoformat())
    free = {"bytes": 400 * MIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    supervisor.tick()
    assert run_count(connection) == 0
    assert len(disk_events(connection, DISK_CRITICAL_EVENT)) == 1


def test_unreadable_volume_is_ignored(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    supervisor = Supervisor(
        connection,
        clock,
        make_registry(),
        data_root=data_root,
        disk_free=lambda: None,
    )
    supervisor.tick()
    assert not supervisor.is_disk_critical
    assert disk_events(connection, DISK_LOW_EVENT) == []
    assert disk_events(connection, DISK_FULL_GUARD_EVENT) == []


def test_disk_guard_blocks_new_run_until_recovered(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    insert_template(connection)
    insert_schedule(connection, next_run_at=T0.isoformat())
    free = {"bytes": 1500 * MIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    assert run_count(connection) == 0
    supervisor.tick()
    assert run_count(connection) == 0
    assert len(disk_events(connection, DISK_FULL_GUARD_EVENT)) == 1
    free["bytes"] = 100 * GIB
    supervisor.tick()
    assert run_count(connection) == 1


def test_running_run_finishes_while_disk_guarded(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    insert_template(connection)
    runs = JobRunService(connection, clock)
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key="guard-survivor",
            priority=0,
            deadline_at=T0 + timedelta(minutes=180),
        )
    )
    free = {"bytes": 1500 * MIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    supervisor.tick()
    supervisor.tick()
    assert runs.get(record.id).state is RunState.SUCCEEDED
    assert len(disk_events(connection, DISK_FULL_GUARD_EVENT)) == 1


def test_serve_loop_returns_disk_low_exit_code(
    env: tuple[sqlite3.Connection, VirtualClock, Path],
) -> None:
    connection, clock, data_root = env
    free = {"bytes": 400 * MIB}
    supervisor = supervisor_for(connection, clock, data_root, free)
    writer = HeartbeatWriter(
        path=data_root / "run" / "heartbeat.json",
        started_at=T0,
        pid=1,
        version="dev",
    )
    loop = ServeLoop(
        supervisor,
        JobRunService(connection, clock),
        writer,
        ServeOptions(clock=clock, max_ticks=10, tick_interval_seconds=0.0),
    )
    assert loop.run() == EXIT_DISK_LOW
    assert EXIT_DISK_LOW == 5
