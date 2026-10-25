"""场景：fake 插件驱动环境状态机走过就绪、失败与保温。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import EnvironmentRef, EnvironmentState, RunContext
from core.clock import VirtualClock
from core.environments.service import EnvironmentCall, EnvironmentService
from plugins.fake.plugin import FakeEnvironmentProvider
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


def make_context(clock: VirtualClock, scenario: str, **options: object) -> RunContext:
    config: dict[str, Any] = {"scenario": scenario, **options}
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="fake",
        environment_id=ENV_ID,
        config=config,
        work_dir=Path("."),
        evidence_dir=Path("."),
        deadline_at=clock.utc_now() + timedelta(minutes=10),
        cancel=_CancelFlag(),
        clock=clock,
        secrets={},
        log=_Logger(),
        emit=lambda event, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, EnvironmentService]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    service = EnvironmentService(connection, clock)
    service.register(ENV_ID, provider_id="fake", name="模拟器")
    try:
        yield connection, clock, service
    finally:
        connection.close()


def make_call(
    provider: FakeEnvironmentProvider,
    clock: VirtualClock,
    scenario: str,
    **options: object,
) -> EnvironmentCall:
    return EnvironmentCall(
        environment_id=ENV_ID,
        provider=provider,
        ref=EnvironmentRef(id=ENV_ID, provider_id="fake"),
        ctx=make_context(clock, scenario, **options),
    )


def state_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) FROM events_global WHERE type = 'env.state.changed'"
    ).fetchone()
    return 0 if row is None else int(row[0])


def test_instant_prepare_reaches_ready(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    result = service.prepare(make_call(FakeEnvironmentProvider(), clock, "instant"))
    assert result.ok
    assert service.get(ENV_ID).state is EnvironmentState.READY


def test_never_ready_prepare_marks_failed(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    result = service.prepare(make_call(FakeEnvironmentProvider(), clock, "never-ready"))
    assert not result.ok
    assert result.error_code == "E_ENV_WAKE_TIMEOUT"
    assert service.get(ENV_ID).state is EnvironmentState.FAILED


def test_slow_probe_advances_to_ready(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    connection, clock, service = env
    provider = FakeEnvironmentProvider()
    call = make_call(provider, clock, "slow", slow_probes=2)
    states: list[EnvironmentState] = []
    for _ in range(3):
        probe = service.probe(call)
        assert probe is not None
        states.append(probe.state)
    assert states == [
        EnvironmentState.BOOTING,
        EnvironmentState.BOOTING,
        EnvironmentState.READY,
    ]
    assert service.get(ENV_ID).state is EnvironmentState.READY
    assert state_count(connection) == 2


def test_cleanup_after_ready_turns_off(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    service.prepare(make_call(FakeEnvironmentProvider(), clock, "instant"))
    service.cleanup(make_call(FakeEnvironmentProvider(), clock, "instant"), keep_warm=False)
    assert service.get(ENV_ID).state is EnvironmentState.OFF


def test_keep_warm_records_warm_until(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    _connection, clock, service = env
    service.prepare(make_call(FakeEnvironmentProvider(), clock, "instant"))
    service.cleanup(
        make_call(FakeEnvironmentProvider(), clock, "instant"),
        keep_warm=True,
        warm_minutes=120,
    )
    current = service.get(ENV_ID)
    assert current.state is EnvironmentState.READY
    assert current.warm_until == T0 + timedelta(minutes=120)
