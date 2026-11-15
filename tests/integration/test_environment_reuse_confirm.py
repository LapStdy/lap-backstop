"""保温窗口内的复用确认：已 ready 的环境在 prepare 时先 warm(0) 确认一次。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import pytest

from contracts import EnvironmentState
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from storage import migrate, open_database
from tests.integration.test_environments import (
    ENV_ID,
    T0,
    StubOptions,
    StubProvider,
    make_call,
    state_events,
)


@pytest.fixture
def env(tmp_path: Path) -> Iterator[tuple[sqlite3.Connection, VirtualClock, EnvironmentService]]:
    """一个已注册的环境；库是空的，用例自行准备。"""
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    service = EnvironmentService(connection, clock)
    service.register(ENV_ID, provider_id="fake", name="模拟器")
    try:
        yield connection, clock, service
    finally:
        connection.close()


def test_prepare_ready_confirms_with_zero_minutes_warm(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """复用确认调 warm(0)：只确认不延长，结果说明带上确认摘要。"""
    _connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    provider = StubProvider()
    result = service.prepare(make_call(provider, clock))
    assert result.ok
    assert provider.calls == ["warm"]
    assert result.detail == "环境已就绪（warm 0）"
    assert service.get(ENV_ID).state is EnvironmentState.READY


def test_reuse_confirm_failure_marks_environment_failed(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """确认失败按 prepare 失败处置：环境置 failed，状态事件来自 ready。"""
    connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    provider = StubProvider(options=StubOptions(warm_ok=False))
    result = service.prepare(make_call(provider, clock))
    assert not result.ok
    assert service.get(ENV_ID).state is EnvironmentState.FAILED
    last = state_events(connection)[-1]
    assert (last["from_state"], last["to_state"]) == ("ready", "failed")


def test_reuse_confirm_keeps_warm_until_unchanged(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """复用确认不延长保温窗口：warm_until 仍是上次收尾算出的时刻。"""
    _connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.cleanup(make_call(StubProvider(), clock), keep_warm=True, warm_minutes=240)
    clock.advance(timedelta(minutes=10))
    assert service.prepare(make_call(StubProvider(), clock)).ok
    assert service.get(ENV_ID).warm_until == T0 + timedelta(minutes=240)
