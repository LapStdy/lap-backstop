"""场景：supervisor 的手动环境动作：prepare 与 reset 的状态迁移与错误拒绝。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from contracts import EnvironmentState
from core.clock import VirtualClock
from core.environments.errors import EnvironmentNotFoundError, EnvironmentStateError
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.errors import PluginNotFoundError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider
from storage.repositories.environments import EnvironmentRepository
from tests.scenarios.supervisor_scenario_support import (
    _STABLE_DISK_FREE,
    ENV_ID,
    T0,
    supervisor_for,
)


def test_prepare_environment_moves_unknown_to_ready(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """手动预热与 tick 调度无关：provider 被调到，环境按 service 规则迁到 ready。"""
    connection, clock, _runs, data_root = env
    provider = FakeEnvironmentProvider()
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    EnvironmentRepository(connection).ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    result = supervisor.prepare_environment(ENV_ID)
    assert result.ok
    assert provider.calls == ["prepare"]
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.READY


def test_reset_environment_stays_booting_until_probe(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/02：reset 复位成功后不直接 ready，等下一次探针确认。"""
    connection, clock, _runs, data_root = env
    provider = FakeEnvironmentProvider()
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    repository = EnvironmentRepository(connection)
    repository.ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    repository.set_state(ENV_ID, EnvironmentState.READY, at=T0, detail={"action": "test"})
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    result = supervisor.reset_environment(ENV_ID)
    assert result.ok
    assert provider.calls == ["reset"]
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.BOOTING


def test_prepare_environment_rejects_busy_state(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = env
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=FakeEnvironmentProvider()))
    repository = EnvironmentRepository(connection)
    repository.ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    repository.set_state(ENV_ID, EnvironmentState.BUSY, at=T0, detail={"action": "test"})
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    with pytest.raises(EnvironmentStateError):
        supervisor.prepare_environment(ENV_ID)
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.BUSY


def test_prepare_environment_raises_for_missing_provider(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = env
    EnvironmentRepository(connection).ensure(ENV_ID, provider_id="ghost", name=ENV_ID)
    supervisor = supervisor_for(connection, clock, data_root)
    with pytest.raises(PluginNotFoundError):
        supervisor.prepare_environment(ENV_ID)


def test_reset_environment_raises_for_missing_environment(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = env
    supervisor = supervisor_for(connection, clock, data_root)
    with pytest.raises(EnvironmentNotFoundError):
        supervisor.reset_environment("no-such-env")
