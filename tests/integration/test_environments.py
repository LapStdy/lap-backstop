"""环境服务：探针、准备、复位、清理、保温。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    EnvironmentProbe,
    EnvironmentRef,
    EnvironmentState,
    PhaseResult,
    RunContext,
)
from core.clock import VirtualClock
from core.environments.errors import EnvironmentNotFoundError, EnvironmentStateError
from core.environments.service import EnvironmentCall, EnvironmentService
from storage import migrate, open_database

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
ENV_ID = "env-1"


class _CancelFlag:
    def is_cancelled(self) -> bool:
        return False


class _Logger:
    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


def make_context(clock: VirtualClock) -> RunContext:
    now = clock.utc_now()
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="fake",
        environment_id=ENV_ID,
        config={},
        work_dir=Path("."),
        evidence_dir=Path("."),
        deadline_at=now + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=clock,
        secrets={},
        log=_Logger(),
        emit=lambda event, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


class StubProvider:
    """按预设状态序列返回探针结果，其余动作由开关控制。"""

    def __init__(
        self,
        states: list[EnvironmentState] | None = None,
        *,
        prepare_ok: bool = True,
        reset_ok: bool = True,
        cleanup_ok: bool = True,
        warm_ok: bool = True,
        probe_error: Exception | None = None,
    ) -> None:
        self._states = list(states or [])
        self.prepare_ok = prepare_ok
        self.reset_ok = reset_ok
        self.cleanup_ok = cleanup_ok
        self.warm_ok = warm_ok
        self.probe_error = probe_error
        self.calls: list[str] = []

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        self.calls.append("probe")
        if self.probe_error is not None:
            raise self.probe_error
        state = self._states.pop(0) if self._states else EnvironmentState.READY
        return EnvironmentProbe(state=state, detail="stub", observed_at=ctx.clock.utc_now())

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        self.calls.append("prepare")
        now = ctx.clock.utc_now()
        return PhaseResult(ok=self.prepare_ok, detail="prepared", started_at=now, ended_at=now)

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        self.calls.append("reset")
        return PhaseResult(ok=self.reset_ok, detail="reset")

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        self.calls.append("cleanup")
        return PhaseResult(ok=self.cleanup_ok, detail=f"cleanup keep_warm={keep_warm}")

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        self.calls.append("warm")
        return PhaseResult(ok=self.warm_ok, detail=f"warm {minutes}")


class HookedProvider(StubProvider):
    """在插件回调里先执行注入动作再返回结果，模拟调用期间另一连接先提交。"""

    def __init__(
        self,
        states: list[EnvironmentState] | None = None,
        *,
        on_probe: Callable[[], None] | None = None,
        on_prepare: Callable[[], None] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(states, **kwargs)
        self._on_probe = on_probe
        self._on_prepare = on_prepare

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        if self._on_probe is not None:
            self._on_probe()
        return super().probe(env, ctx)

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        if self._on_prepare is not None:
            self._on_prepare()
        return super().prepare(env, ctx)


@pytest.fixture
def env(tmp_path: Path) -> Iterator[tuple[sqlite3.Connection, VirtualClock, EnvironmentService]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    service = EnvironmentService(connection, clock)
    service.register(ENV_ID, provider_id="fake", name="模拟器")
    try:
        yield connection, clock, service
    finally:
        connection.close()


def make_call(provider: StubProvider, clock: VirtualClock) -> EnvironmentCall:
    return EnvironmentCall(
        environment_id=ENV_ID,
        provider=provider,
        ref=EnvironmentRef(id=ENV_ID, provider_id="fake"),
        ctx=make_context(clock),
    )


def state_events(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT payload FROM events_global WHERE type = 'env.state.changed' ORDER BY seq"
    ).fetchall()
    return [json.loads(str(row[0])) for row in rows]


def test_register_starts_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, _clock, service = env
    assert service.get(ENV_ID).state is EnvironmentState.UNKNOWN


def test_probe_moves_through_booting_to_ready(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    connection, clock, service = env
    provider = StubProvider([EnvironmentState.BOOTING, EnvironmentState.READY])
    service.probe(make_call(provider, clock))
    service.probe(make_call(provider, clock))
    assert service.get(ENV_ID).state is EnvironmentState.READY
    payloads = state_events(connection)
    assert [(item["from_state"], item["to_state"]) for item in payloads] == [
        ("unknown", "booting"),
        ("booting", "ready"),
    ]


def test_probe_same_state_only_refreshes_observation(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    connection, clock, service = env
    provider = StubProvider([EnvironmentState.BOOTING, EnvironmentState.BOOTING])
    service.probe(make_call(provider, clock))
    clock.advance(timedelta(minutes=1))
    service.probe(make_call(provider, clock))
    assert len(state_events(connection)) == 1
    assert service.get(ENV_ID).last_probe_at == T0 + timedelta(minutes=1)


def test_illegal_probe_transition_marks_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.cleanup(make_call(StubProvider(), clock), keep_warm=False, warm_minutes=0)
    probe = service.probe(make_call(StubProvider([EnvironmentState.READY]), clock))
    assert probe is not None and probe.state is EnvironmentState.READY
    assert service.get(ENV_ID).state is EnvironmentState.UNKNOWN
    last = state_events(_connection)[-1]
    assert (last["from_state"], last["to_state"]) == ("off", "unknown")


def test_probe_failure_marks_unknown_and_returns_none(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    probe = service.probe(make_call(StubProvider(probe_error=TimeoutError("探针超时")), clock))
    assert probe is None
    assert service.get(ENV_ID).state is EnvironmentState.UNKNOWN
    last = state_events(_connection)[-1]
    assert (last["from_state"], last["to_state"]) == ("ready", "unknown")


def test_prepare_moves_unknown_to_ready(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    __connection, clock, service = env
    provider = StubProvider()
    result = service.prepare(make_call(provider, clock))
    assert result.ok
    assert provider.calls == ["prepare"]
    assert service.get(ENV_ID).state is EnvironmentState.READY


def test_prepare_is_idempotent_when_ready(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    __connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    provider = StubProvider()
    result = service.prepare(make_call(provider, clock))
    assert result.ok
    assert provider.calls == []


def test_prepare_failure_marks_failed(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    __connection, clock, service = env
    result = service.prepare(make_call(StubProvider(prepare_ok=False), clock))
    assert not result.ok
    assert service.get(ENV_ID).state is EnvironmentState.FAILED


def test_reset_returns_to_booting(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    __connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    provider = StubProvider()
    service.reset(make_call(provider, clock))
    assert provider.calls == ["reset"]
    assert service.get(ENV_ID).state is EnvironmentState.BOOTING


def test_cleanup_with_keep_warm_sets_warm_until(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    __connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.cleanup(make_call(StubProvider(), clock), keep_warm=True, warm_minutes=240)
    current = service.get(ENV_ID)
    assert current.state is EnvironmentState.READY
    assert current.warm_until == T0 + timedelta(minutes=240)


def test_cleanup_without_keep_warm_turns_off(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    __connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.cleanup(make_call(StubProvider(), clock), keep_warm=False)
    current = service.get(ENV_ID)
    assert current.state is EnvironmentState.OFF
    assert current.warm_until is None


def test_warm_expired_lists_due_environments(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    __connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.warm(make_call(StubProvider(), clock), minutes=30)
    assert service.warm_expired(T0 + timedelta(minutes=29)) == ()
    due = service.warm_expired(T0 + timedelta(minutes=31))
    assert [row.id for row in due] == [ENV_ID]


def test_warm_requires_ready_environment(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    provider = StubProvider()
    with pytest.raises(EnvironmentStateError):
        service.warm(make_call(provider, clock), minutes=30)
    assert provider.calls == []


def test_missing_environment_raises(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, _clock, service = env
    with pytest.raises(EnvironmentNotFoundError):
        service.get("missing")


@pytest.fixture
def two_writers(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, sqlite3.Connection, VirtualClock, EnvironmentService]]:
    """同一库上的两条真实连接：service 挂 A 连接，B 连接代表另一个写者线程。"""
    database = tmp_path / "state.db"
    connection_a = open_database(database)
    migrate(connection_a)
    connection_b = open_database(database)
    clock = VirtualClock(T0)
    service = EnvironmentService(connection_a, clock)
    service.register(ENV_ID, provider_id="fake", name="模拟器")
    try:
        yield connection_a, connection_b, clock, service
    finally:
        connection_a.close()
        connection_b.close()


def conflict_events(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """events_global 里 event 名为 core.env_action_conflict 的载荷（内层 payload）。"""
    rows = connection.execute(
        "SELECT payload FROM events_global WHERE type = 'core.event' ORDER BY seq"
    ).fetchall()
    frames = (json.loads(str(row[0])) for row in rows)
    return [item["payload"] for item in frames if item.get("event") == "core.env_action_conflict"]


def test_probe_write_discarded_when_other_connection_committed_first(
    two_writers: tuple[sqlite3.Connection, sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """A 按 unknown 前置写入时 B 已提交 booting：A 丢弃，最终状态与事件归 B。"""
    connection_a, connection_b, clock, service = two_writers
    other = EnvironmentService(connection_b, clock)

    def commit_booting() -> None:
        other.probe(make_call(StubProvider([EnvironmentState.BOOTING]), clock))

    provider = HookedProvider([EnvironmentState.OFF], on_probe=commit_booting)
    service.probe(make_call(provider, clock))

    assert service.get(ENV_ID).state is EnvironmentState.BOOTING
    payloads = state_events(connection_a)
    assert [(item["from_state"], item["to_state"]) for item in payloads] == [("unknown", "booting")]
    assert conflict_events(connection_a) == [
        {
            "environment_id": ENV_ID,
            "action": "probe",
            "expected_state": "unknown",
            "actual_state": "booting",
        }
    ]
    assert not connection_a.in_transaction
    assert not connection_b.in_transaction


def test_prepare_discards_result_when_environment_off_during_plugin_call(
    two_writers: tuple[sqlite3.Connection, sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """prepare 调用期间环境被另一连接改回 off：结果不应用，抛状态冲突并留证据。"""
    connection_a, connection_b, clock, service = two_writers

    def rewrite_off() -> None:
        connection_b.execute("UPDATE environments SET state = 'off' WHERE id = ?", (ENV_ID,))

    provider = HookedProvider(on_prepare=rewrite_off)
    with pytest.raises(EnvironmentStateError):
        service.prepare(make_call(provider, clock))

    assert service.get(ENV_ID).state is EnvironmentState.OFF
    payloads = state_events(connection_a)
    assert [(item["from_state"], item["to_state"]) for item in payloads] == [("unknown", "booting")]
    assert conflict_events(connection_a) == [
        {
            "environment_id": ENV_ID,
            "action": "prepare",
            "expected_state": "booting",
            "actual_state": "off",
        }
    ]
    assert not connection_a.in_transaction
    assert not connection_b.in_transaction


def test_prepare_raises_conflict_when_environment_ready_during_plugin_call(
    two_writers: tuple[sqlite3.Connection, sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """prepare 调用期间环境已被改成 ready：不静默吞成成功，抛冲突并留证据。"""
    connection_a, connection_b, clock, service = two_writers

    def rewrite_ready() -> None:
        connection_b.execute("UPDATE environments SET state = 'ready' WHERE id = ?", (ENV_ID,))

    provider = HookedProvider(on_prepare=rewrite_ready)
    with pytest.raises(EnvironmentStateError):
        service.prepare(make_call(provider, clock))

    assert service.get(ENV_ID).state is EnvironmentState.READY
    payloads = state_events(connection_a)
    assert [(item["from_state"], item["to_state"]) for item in payloads] == [("unknown", "booting")]
    assert conflict_events(connection_a) == [
        {
            "environment_id": ENV_ID,
            "action": "prepare",
            "expected_state": "booting",
            "actual_state": "ready",
        }
    ]
    assert not connection_a.in_transaction
    assert not connection_b.in_transaction


def test_sequential_prepare_records_true_precondition_per_transition(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """无并发时每次成功迁移恰一条事件，from_state 为写入前数据库里的真实状态。"""
    connection, _clock, service = env
    service.prepare(make_call(StubProvider(), clock=_clock))
    assert service.get(ENV_ID).state is EnvironmentState.READY
    payloads = state_events(connection)
    assert [(item["from_state"], item["to_state"]) for item in payloads] == [
        ("unknown", "booting"),
        ("booting", "ready"),
    ]
