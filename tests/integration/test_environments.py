"""环境服务：探针、准备、复位、清理、保温。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
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
    ) -> None:
        self._states = list(states or [])
        self.prepare_ok = prepare_ok
        self.reset_ok = reset_ok
        self.cleanup_ok = cleanup_ok
        self.warm_ok = warm_ok
        self.calls: list[str] = []

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        self.calls.append("probe")
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


def test_illegal_probe_transition_is_rejected(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.cleanup(make_call(StubProvider(), clock), keep_warm=False, warm_minutes=0)
    with pytest.raises(EnvironmentStateError):
        service.probe(make_call(StubProvider([EnvironmentState.READY]), clock))
    assert service.get(ENV_ID).state is EnvironmentState.OFF


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
