"""场景：虚拟时钟下 fake 插件把同一台机器连续推进 7 天的浸泡演练。

每天按固定 cron 触发一次作业，覆盖终态收敛、租约回收、事件流自洽、保留清理
30 天口径、保温复用与单日失败恢复；推进只用虚拟时钟，不碰真实墙钟与 sleep。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import TERMINAL_RUN_STATES, EnvironmentState, RunState, VerdictStatus
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.events.replay import replay_run
from core.hostops.retention import EVIDENCE_DIRNAME, RetentionOutcome, RetentionRunner
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.iso_time import format_at
from tests.supervisor_support import tick_and_settle

T0 = datetime(2026, 10, 23, 0, 0, tzinfo=UTC)
DAYS = 7
DAILY_HOUR = 4
KEEP_WARM_MINUTES = 240
FAILURE_DAY = 3
RETRY_BACKOFF = timedelta(seconds=30)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
SCHEDULE_ID = "soak-daily"
DISK_FREE = 100 * 1024**3

INSTANT_CONFIG: Mapping[str, object] = {"scenario": "instant"}
FAIL_CONFIG: Mapping[str, object] = {"scenario": "fail", "fail_times": 1}
VERIFICATION_POLICY = {
    "id": "fake-policy",
    "verifier_plugin_id": "fake",
    "required_evidence": ["log"],
    "params": {},
}
RECOVERY_POLICY = {"max_attempts": 3, "backoff_seconds": [30]}
RUN_BASE_EVENTS = {"run.created", "run.state.changed", "run.finished"}
LEASE_BASE_EVENTS = {"resource.leased", "resource.released"}


def _day_moment(day: int) -> datetime:
    """第 ``day`` 天的每日触发时刻。"""
    return T0 + timedelta(days=day - 1, hours=DAILY_HOUR)


def _insert_template(connection: sqlite3.Connection, config: Mapping[str, object]) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, ?, 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps(dict(config)),
            json.dumps(VERIFICATION_POLICY),
            json.dumps(RECOVERY_POLICY),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


def _set_template_config(connection: sqlite3.Connection, config: Mapping[str, object]) -> None:
    """改写模板的插件配置：失败天次的注入与复原都走这一处。"""
    connection.execute(
        "UPDATE job_templates SET config = ?, updated_at = ? WHERE id = ?",
        (json.dumps(dict(config)), T0.isoformat(), TEMPLATE_ID),
    )


def _insert_daily_schedule(
    connection: sqlite3.Connection,
    schedule_id: str,
    first_at: datetime,
) -> None:
    """按首日触发时刻插入一条每天同一时刻的 cron 计划。"""
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, window_minutes, "
        "coalesce, priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
        "job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每日', 'cron', ?, NULL, 'UTC', 'run_once', 120, 1, 0, 180, 0, ?, ?, "
        "1, ?, ?, ?)",
        (
            schedule_id,
            f"{first_at.minute} {first_at.hour} * * *",
            KEEP_WARM_MINUTES,
            TEMPLATE_ID,
            first_at.isoformat(),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


def _make_registry() -> tuple[PluginRegistry, FakeEnvironmentProvider, FakeScriptAdapter]:
    environment = FakeEnvironmentProvider()
    script = FakeScriptAdapter()
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=environment,
            script=script,
            verifier=FakeVerifier(),
        )
    )
    return registry, environment, script


@dataclass(slots=True)
class SoakRig:
    """一台被浸泡的机器：库、时钟、执行循环、插件实例与数据根。"""

    connection: sqlite3.Connection
    clock: VirtualClock
    runs: JobRunService
    supervisor: Supervisor
    provider: FakeEnvironmentProvider
    script: FakeScriptAdapter
    data_root: Path


@pytest.fixture
def soak(tmp_path: Path) -> Iterator[SoakRig]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    _insert_template(connection, INSTANT_CONFIG)
    clock = VirtualClock(T0)
    registry, environment, script = _make_registry()
    data_root = tmp_path / "data"
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: DISK_FREE
    )
    try:
        yield SoakRig(
            connection=connection,
            clock=clock,
            runs=JobRunService(connection, clock),
            supervisor=supervisor,
            provider=environment,
            script=script,
            data_root=data_root,
        )
    finally:
        connection.close()


def _advance(rig: SoakRig, moment: datetime) -> None:
    rig.clock.jump_to(moment)
    tick_and_settle(rig.supervisor)


def _run_seven_days(rig: SoakRig) -> None:
    _insert_daily_schedule(rig.connection, SCHEDULE_ID, _day_moment(1))
    for day in range(1, DAYS + 1):
        _advance(rig, _day_moment(day))


def _run_ids(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute("SELECT id FROM job_runs ORDER BY created_at, id").fetchall()
    return [str(row[0]) for row in rows]


def _run_states(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute("SELECT id, state FROM job_runs").fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def _run_event_count(connection: sqlite3.Connection, run_id: str) -> int:
    row = connection.execute(
        "SELECT COUNT(*) FROM run_events WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert row is not None
    return int(row[0])


def _unfinished_runs(connection: sqlite3.Connection) -> int:
    marks = ",".join("?" for _ in TERMINAL_RUN_STATES)
    row = connection.execute(
        f"SELECT COUNT(*) FROM job_runs WHERE state NOT IN ({marks})",
        tuple(state.value for state in TERMINAL_RUN_STATES),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _active_lease_rows(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) FROM resource_leases WHERE state != 'released'"
    ).fetchone()
    assert row is not None
    return int(row[0])


def _stale_lease_rows(connection: sqlite3.Connection, now: datetime) -> int:
    """仍占着租约状态的行里，到期时刻已过的行数；正常收尾应恒为 0。"""
    row = connection.execute(
        "SELECT COUNT(*) FROM resource_leases WHERE state != 'released' AND expires_at <= ?",
        (format_at(now),),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _evidence_files(rig: SoakRig, run_id: str) -> int:
    rows = rig.connection.execute(
        "SELECT path FROM evidence WHERE run_id = ?", (run_id,)
    ).fetchall()
    return sum(1 for row in rows if (rig.data_root / EVIDENCE_DIRNAME / str(row[0])).is_file())


def _run_retention(rig: SoakRig, day: int) -> RetentionOutcome:
    rig.clock.jump_to(_day_moment(day))
    runner = RetentionRunner(rig.connection, rig.clock, data_root=rig.data_root)
    outcome = runner.run_once()
    assert outcome is not None
    return outcome


def _assert_run_consistent(rig: SoakRig, run_id: str) -> None:
    events = rig.runs.events(run_id)
    snapshot = replay_run(events)
    record = rig.runs.get(run_id)
    assert snapshot.state is record.state
    assert snapshot.finished
    assert snapshot.last_seq == len(events)
    assert snapshot.verdict_status == VerdictStatus.COMPLETED.value
    kinds = {event.type for event in events}
    assert kinds >= RUN_BASE_EVENTS
    assert kinds >= LEASE_BASE_EVENTS
    evidence_events = [event for event in events if event.type == "evidence"]
    row = rig.connection.execute(
        "SELECT COUNT(*) FROM evidence WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert row is not None
    registered = int(row[0])
    assert len(evidence_events) == registered
    assert _evidence_files(rig, run_id) == registered


def test_seven_days_all_runs_reach_terminal_state(soak: SoakRig) -> None:
    _run_seven_days(soak)
    states = _run_states(soak.connection)
    assert len(states) == DAYS
    assert set(states.values()) == {RunState.SUCCEEDED.value}
    assert _unfinished_runs(soak.connection) == 0


def test_soak_seven_days_releases_all_leases(soak: SoakRig) -> None:
    _insert_daily_schedule(soak.connection, SCHEDULE_ID, _day_moment(1))
    for day in range(1, DAYS + 1):
        _advance(soak, _day_moment(day))
        assert _active_lease_rows(soak.connection) == 0
        assert _stale_lease_rows(soak.connection, soak.clock.utc_now()) == 0
    assert _active_lease_rows(soak.connection) == 0


def test_events_match_replayed_snapshot_each_run(soak: SoakRig) -> None:
    _run_seven_days(soak)
    for run_id in _run_ids(soak.connection):
        _assert_run_consistent(soak, run_id)


def test_retention_keeps_recent_and_purges_old_after_40_days(soak: SoakRig) -> None:
    _run_seven_days(soak)
    kept = _run_retention(soak, DAYS + 1)
    assert kept.evidence_files_deleted == 0
    assert kept.run_events_deleted == 0
    assert kept.run_phases_deleted == 0
    assert kept.runs_deleted == 0
    _advance(soak, _day_moment(39))
    recent = _run_ids(soak.connection)[-1]
    recent_events = _run_event_count(soak.connection, recent)
    recent_files = _evidence_files(soak, recent)
    assert recent_events > 0
    assert recent_files > 0
    purged = _run_retention(soak, 40)
    assert purged.evidence_files_deleted > 0
    assert purged.run_events_deleted > 0
    assert purged.runs_deleted == 0
    assert len(_run_ids(soak.connection)) == DAYS + 1
    expired = _run_ids(soak.connection)[:DAYS]
    assert sum(_run_event_count(soak.connection, run_id) for run_id in expired) == 0
    assert _run_event_count(soak.connection, recent) == recent_events
    assert _evidence_files(soak, recent) == recent_files


def test_keep_warm_reuses_ready_environment_next_day(soak: SoakRig) -> None:
    _insert_daily_schedule(soak.connection, "soak-evening", T0 + timedelta(hours=23, minutes=50))
    _insert_daily_schedule(soak.connection, "soak-morning", T0 + timedelta(days=1))
    _advance(soak, T0 + timedelta(hours=23, minutes=50))
    _advance(soak, T0 + timedelta(days=1))
    states = _run_states(soak.connection)
    assert len(states) == 2
    assert set(states.values()) == {RunState.SUCCEEDED.value}
    assert soak.provider.calls.count("prepare") == 1
    assert soak.provider.calls.count("warm") == 1
    environment = EnvironmentService(soak.connection, soak.clock).get(ENV_ID)
    assert environment.state is EnvironmentState.READY


def test_single_failure_day_recovers_and_later_days_succeed(soak: SoakRig) -> None:
    _insert_daily_schedule(soak.connection, SCHEDULE_ID, _day_moment(1))
    for day in range(1, DAYS + 1):
        if day == FAILURE_DAY:
            _set_template_config(soak.connection, FAIL_CONFIG)
        _advance(soak, _day_moment(day))
        if day == FAILURE_DAY:
            soak.clock.advance(RETRY_BACKOFF)
            tick_and_settle(soak.supervisor)
            _set_template_config(soak.connection, INSTANT_CONFIG)
    states = _run_states(soak.connection)
    assert len(states) == DAYS
    assert set(states.values()) == {RunState.SUCCEEDED.value}
    recovered = [
        run_id for run_id in _run_ids(soak.connection) if soak.runs.get(run_id).attempt > 0
    ]
    assert len(recovered) == 1
    events = soak.runs.events(recovered[0])
    signals = [event.payload.get("kind") for event in events if event.type == "run.signal"]
    assert "task_failed" in signals
    started = [event for event in events if event.type == "recovery.started"]
    finished = [event for event in events if event.type == "recovery.finished"]
    assert len(started) == 1
    assert len(finished) == 1
    assert started[0].payload.get("attempt") == 1
