"""环境动作仲裁：cleanup 与 warm 的先提交者赢语义（docs/09 保温、docs/11 并发与超时）。

并发窗口用同一库上的两条真实连接构造：插件回调里由 B 连接先提交，A 连接的
动作返回后应丢弃本次写入，只记 core.env_action_conflict，按失败结果收口。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path

import pytest

from contracts import EnvironmentRef, EnvironmentState, PhaseResult, RunContext
from core.clock import VirtualClock
from core.environments.errors import EnvironmentStateError
from core.environments.service import EnvironmentService
from storage import migrate, open_database
from tests.integration.test_environments import (
    ENV_ID,
    T0,
    StubProvider,
    conflict_events,
    make_call,
    state_events,
)

REQS = ("A-104",)

TwoWriters = tuple[sqlite3.Connection, sqlite3.Connection, VirtualClock, EnvironmentService]


@pytest.fixture
def arbitration_env(tmp_path: Path) -> Iterator[TwoWriters]:
    """同一库上的两条真实连接：service 挂 A 连接，B 连接代表另一个写者。"""
    connection_a = open_database(tmp_path / "state.db")
    migrate(connection_a)
    connection_b = open_database(tmp_path / "state.db")
    clock = VirtualClock(T0)
    service = EnvironmentService(connection_a, clock)
    service.register(ENV_ID, provider_id="fake", name="模拟器")
    try:
        yield connection_a, connection_b, clock, service
    finally:
        connection_a.close()
        connection_b.close()


class CleanupWarmProvider(StubProvider):
    """cleanup/warm 返回前先执行注入动作，模拟调用期间另一连接先提交。"""

    def __init__(
        self,
        *,
        on_cleanup: Callable[[], None] | None = None,
        on_warm: Callable[[], None] | None = None,
    ) -> None:
        super().__init__()
        self._on_cleanup = on_cleanup
        self._on_warm = on_warm

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        if self._on_cleanup is not None:
            self._on_cleanup()
        return super().cleanup(env, ctx, keep_warm)

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        if self._on_warm is not None:
            self._on_warm()
        return super().warm(env, ctx, minutes)


def rewrite_state_to(connection: sqlite3.Connection, state: str) -> Callable[[], None]:
    """注入动作：由 B 连接把环境改到指定状态，抢在 A 连接之前提交。"""

    def rewrite() -> None:
        connection.execute("UPDATE environments SET state = ? WHERE id = ?", (state, ENV_ID))

    return rewrite


def test_cleanup_conflict_discards_write_and_records_event(arbitration_env: TwoWriters) -> None:
    """cleanup 调用期间环境被改成 off：丢弃本次写，只记一条冲突事件并失败收口。"""
    connection_a, connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))
    assert service.get(ENV_ID).state is EnvironmentState.READY

    provider = CleanupWarmProvider(on_cleanup=rewrite_state_to(connection_b, "off"))
    result = service.cleanup(make_call(provider, clock), keep_warm=True, warm_minutes=240)

    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    assert service.get(ENV_ID).state is EnvironmentState.OFF
    assert service.get(ENV_ID).warm_until is None
    assert conflict_events(connection_a) == [
        {
            "environment_id": ENV_ID,
            "action": "cleanup",
            "expected_state": "ready",
            "actual_state": "off",
        }
    ]
    assert not connection_a.in_transaction


def test_cleanup_conflict_writes_no_state_event(arbitration_env: TwoWriters) -> None:
    """cleanup 冲突丢弃时不新增 env.state.changed，也不清空已有 warm_until。"""
    connection_a, connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))
    service.cleanup(make_call(StubProvider(), clock), keep_warm=True, warm_minutes=30)
    before = state_events(connection_a)
    warm_before = service.get(ENV_ID).warm_until
    assert warm_before is not None

    provider = CleanupWarmProvider(on_cleanup=rewrite_state_to(connection_b, "off"))
    result = service.cleanup(make_call(provider, clock), keep_warm=False)

    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    assert state_events(connection_a) == before
    assert service.get(ENV_ID).warm_until == warm_before


def test_cleanup_busy_to_ready_unchanged(arbitration_env: TwoWriters) -> None:
    """前置 busy、keep_warm=False：先 busy→ready 再 ready→off，与现状一致。"""
    connection_a, _connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))
    service.mark_busy(make_call(StubProvider(), clock))

    result = service.cleanup(make_call(StubProvider(), clock), keep_warm=False)

    assert result.ok is True
    row = service.get(ENV_ID)
    assert row.state is EnvironmentState.OFF
    assert row.warm_until is None
    assert [(item["from_state"], item["to_state"]) for item in state_events(connection_a)] == [
        ("unknown", "booting"),
        ("booting", "ready"),
        ("ready", "busy"),
        ("busy", "ready"),
        ("ready", "off"),
    ]


def test_cleanup_busy_changed_to_off_conflicts(arbitration_env: TwoWriters) -> None:
    """前置 busy、调用期间被改成 off：丢弃 busy→ready 与关闭写入，记冲突并失败收口。"""
    connection_a, connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))
    service.mark_busy(make_call(StubProvider(), clock))
    before = state_events(connection_a)

    provider = CleanupWarmProvider(on_cleanup=rewrite_state_to(connection_b, "off"))
    result = service.cleanup(make_call(provider, clock), keep_warm=False)

    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    assert service.get(ENV_ID).state is EnvironmentState.OFF
    assert state_events(connection_a) == before
    assert conflict_events(connection_a) == [
        {
            "environment_id": ENV_ID,
            "action": "cleanup",
            "expected_state": "busy",
            "actual_state": "off",
        }
    ]


def test_warm_conflict_discards_warm_until(arbitration_env: TwoWriters) -> None:
    """warm 调用期间环境被改成 off：丢弃 warm_until 写入，记冲突并失败收口。"""
    connection_a, connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))
    before = state_events(connection_a)

    provider = CleanupWarmProvider(on_warm=rewrite_state_to(connection_b, "off"))
    result = service.warm(make_call(provider, clock), minutes=30)

    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    row = service.get(ENV_ID)
    assert row.state is EnvironmentState.OFF
    assert row.warm_until is None
    assert state_events(connection_a) == before
    assert conflict_events(connection_a) == [
        {
            "environment_id": ENV_ID,
            "action": "warm",
            "expected_state": "ready",
            "actual_state": "off",
        }
    ]
    assert not connection_a.in_transaction


def test_cleanup_rejected_transition_still_raises(arbitration_env: TwoWriters) -> None:
    """无冲突的状态机拒绝（off 上要求保温回到 ready）仍按异常上抛，与现状一致。"""
    connection_a, _connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))
    service.cleanup(make_call(StubProvider(), clock), keep_warm=False)
    assert service.get(ENV_ID).state is EnvironmentState.OFF

    with pytest.raises(EnvironmentStateError):
        service.cleanup(make_call(StubProvider(), clock), keep_warm=True)

    assert service.get(ENV_ID).state is EnvironmentState.OFF
    assert conflict_events(connection_a) == []


def test_cleanup_without_conflict_keeps_current_behaviour(arbitration_env: TwoWriters) -> None:
    """无并发干扰：keep_warm 真假两种迁移、warm_until 与事件序列与现状一致。"""
    connection_a, _connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))

    result = service.cleanup(make_call(StubProvider(), clock), keep_warm=True, warm_minutes=240)
    assert result.ok is True
    row = service.get(ENV_ID)
    assert row.state is EnvironmentState.READY
    assert row.warm_until == T0 + timedelta(minutes=240)

    result = service.cleanup(make_call(StubProvider(), clock), keep_warm=False)
    assert result.ok is True
    row = service.get(ENV_ID)
    assert row.state is EnvironmentState.OFF
    assert row.warm_until is None
    assert [(item["from_state"], item["to_state"]) for item in state_events(connection_a)] == [
        ("unknown", "booting"),
        ("booting", "ready"),
        ("ready", "off"),
    ]


def test_warm_without_conflict_keeps_current_behaviour(arbitration_env: TwoWriters) -> None:
    """无并发干扰：warm 返回成功并按分钟写入 warm_until。"""
    _connection_a, _connection_b, clock, service = arbitration_env
    service.prepare(make_call(StubProvider(), clock))

    result = service.warm(make_call(StubProvider(), clock), minutes=30)

    assert result.ok is True
    row = service.get(ENV_ID)
    assert row.state is EnvironmentState.READY
    assert row.warm_until == T0 + timedelta(minutes=30)
