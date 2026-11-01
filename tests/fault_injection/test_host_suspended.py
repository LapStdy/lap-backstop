"""故障注入：运行中墙钟相对单调钟跳变，按休眠恢复处置（docs/12 故障注入表）。

判据是「119 秒不算跳变；121 秒记 host.suspended、全环境置 unknown 且 probe 恰好一次」：
跳变量取两次 tick 之间墙钟增量与单调增量之差，休眠恢复才拉得出大差值。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

from contracts import EnvironmentState
from core.environments.service import EnvironmentService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider
from storage import migrate, open_database
from tests.supervisor_support import tick_and_settle

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
ENV_ID = "env-1"
STABLE_DISK_FREE = 100 * 1024**3
TICK_MONO_SECONDS: Final[float] = 1.0


class DivergentClock:
    """墙钟与单调钟分离的测试时钟：单调按秒推进，墙钟可整体前跳。"""

    def __init__(self, start: datetime) -> None:
        self._utc = start
        self._mono = 0.0

    def monotonic(self) -> float:
        return self._mono

    def utc_now(self) -> datetime:
        return self._utc

    def advance_mono(self, seconds: float) -> None:
        self._mono += seconds

    def jump_wall(self, delta: timedelta) -> None:
        self._utc += delta


def make_supervisor(
    connection: sqlite3.Connection,
    clock: DivergentClock,
    provider: FakeEnvironmentProvider,
    tmp_path: Path,
) -> Supervisor:
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    return Supervisor(connection, clock, registry, data_root=tmp_path, disk_free=lambda: 100)


def global_payloads(connection: sqlite3.Connection) -> list[dict[str, object]]:
    rows = connection.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    return [dict(json.loads(str(row[0]))) for row in rows]


def suspended_payloads(connection: sqlite3.Connection) -> list[dict[str, object]]:
    return [
        payload
        for payload in global_payloads(connection)
        if payload.get("event") == "host.suspended"
    ]


def env_state(connection: sqlite3.Connection) -> str:
    row = connection.execute("SELECT state FROM environments WHERE id = ?", (ENV_ID,)).fetchone()
    assert row is not None
    return str(row[0])


def tick_with_jump(supervisor: Supervisor, clock: DivergentClock, wall_seconds: float) -> None:
    """一轮正常 tick（定基线）加一轮带跳变的 tick；墙钟前跳、单调只走一秒。"""
    tick_and_settle(supervisor)
    clock.advance_mono(TICK_MONO_SECONDS)
    clock.jump_wall(timedelta(seconds=wall_seconds))
    tick_and_settle(supervisor)


def test_wall_jump_under_threshold_changes_nothing(tmp_path: Path) -> None:
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        EnvironmentService(connection, DivergentClock(T0)).register(
            ENV_ID, provider_id="fake", name="方舟环境"
        )
        provider = FakeEnvironmentProvider()
        clock = DivergentClock(T0)
        supervisor = make_supervisor(connection, clock, provider, tmp_path)
        supervisor.prepare_environment(ENV_ID)
        assert env_state(connection) == EnvironmentState.READY.value

        tick_with_jump(supervisor, clock, wall_seconds=120.0)

        assert suspended_payloads(connection) == []
        assert env_state(connection) == EnvironmentState.READY.value
        assert provider.calls == ["prepare"]
    finally:
        connection.close()


def test_wall_jump_over_threshold_suspends_environments_and_probes_once(
    tmp_path: Path,
) -> None:
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        EnvironmentService(connection, DivergentClock(T0)).register(
            ENV_ID, provider_id="fake", name="方舟环境"
        )
        provider = FakeEnvironmentProvider()
        clock = DivergentClock(T0)
        supervisor = make_supervisor(connection, clock, provider, tmp_path)
        supervisor.prepare_environment(ENV_ID)

        tick_with_jump(supervisor, clock, wall_seconds=122.0)

        payloads = suspended_payloads(connection)
        assert len(payloads) == 1
        assert payloads[0]["jump_seconds"] == 121.0
        assert "墙钟跳变" in str(payloads[0]["detail"])
        assert env_state(connection) == EnvironmentState.UNKNOWN.value
        changed = [
            payload
            for payload in global_payloads(connection)
            if payload.get("environment_id") == ENV_ID and payload.get("to_state") == "unknown"
        ]
        assert len(changed) == 1
        assert provider.calls == ["prepare", "probe"]
    finally:
        connection.close()
