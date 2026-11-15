"""场景：supervisor 的环境动作：手动 prepare/reset 的状态迁移与错误拒绝。

保温关闭的失败记录与跨轮重试也归本文件。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import EnvironmentRef, EnvironmentState, ErrorCode, PhaseResult, RunContext
from core.clock import VirtualClock
from core.environments.errors import EnvironmentNotFoundError, EnvironmentStateError
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.errors import PluginNotFoundError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider
from storage import open_database
from storage.repositories.environments import EnvironmentRepository
from tests.scenarios.supervisor_scenario_support import (
    _STABLE_DISK_FREE,
    ENV_ID,
    T0,
    supervisor_for,
)
from tests.supervisor_support import tick_and_settle

ENV_ID_2 = "env-2"


class _RecordingLogger:
    """结构化日志记录桩：攒下 warn 字段字典，供断言读取。"""

    def __init__(self) -> None:
        self.warns: list[dict[str, Any]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        self.warns.append(dict(fields))

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


class _CleanupFailureProvider(FakeEnvironmentProvider):
    """cleanup 按预设返回失败结果的假 provider：钉 ok=False 失败记录的载荷。"""

    def __init__(self, *, error_code: str, detail: str) -> None:
        super().__init__()
        self._error_code = error_code
        self._detail = detail

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        self.calls.append("cleanup")
        return PhaseResult(ok=False, error_code=self._error_code, detail=self._detail)


class _CleanupCrashProvider(FakeEnvironmentProvider):
    """cleanup 一律抛错的假 provider：钉住异常路径的 warn 记录形状。"""

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        self.calls.append("cleanup")
        raise RuntimeError("fake 保温关闭失败")


class _RaceDuringCleanupProvider(FakeEnvironmentProvider):
    """cleanup 返回前先执行注入动作，模拟调用期间另一连接先提交的并发窗口。"""

    def __init__(self) -> None:
        super().__init__()
        self.on_cleanup: Callable[[], None] | None = None

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        if self.on_cleanup is not None:
            self.on_cleanup()
        return super().cleanup(env, ctx, keep_warm)


def _seed_ready_with_expired_warm(
    repository: EnvironmentRepository,
    environment_id: str,
    *,
    provider_id: str = "fake",
) -> None:
    """种一行 ready 且保温已到期的环境：保温关闭的入场条件。"""
    repository.ensure(environment_id, provider_id=provider_id, name=environment_id)
    repository.set_state(environment_id, EnvironmentState.READY, at=T0, detail={"action": "test"})
    repository.set_warm_until(environment_id, T0 - timedelta(minutes=1))


def _rewrite_state_to(
    connection: sqlite3.Connection,
    environment_id: str,
    state: str,
) -> Callable[[], None]:
    """注入动作：由另一连接把环境改到指定状态，抢在保温关闭的写入之前提交。"""

    def rewrite() -> None:
        connection.execute(
            "UPDATE environments SET state = ? WHERE id = ?", (state, environment_id)
        )

    return rewrite


def _warm_close_supervisor(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    registry: PluginRegistry,
    logger: _RecordingLogger,
) -> Supervisor:
    """保温关闭用例的装配：日志挂记录桩、磁盘水位稳定，registry 由用例按 provider 组装。"""
    connection, clock, _runs, data_root = supervisor_env
    return Supervisor(
        connection,
        clock,
        registry,
        data_root=data_root,
        disk_free=lambda: _STABLE_DISK_FREE,
        log=logger,
    )


def test_prepare_environment_moves_unknown_to_ready(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """手动预热与 tick 调度无关：provider 被调到，环境按 service 规则迁到 ready。"""
    connection, clock, _runs, data_root = supervisor_env
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
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/02：reset 复位成功后不直接 ready，等下一次探针确认。"""
    connection, clock, _runs, data_root = supervisor_env
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
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = supervisor_env
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
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = supervisor_env
    EnvironmentRepository(connection).ensure(ENV_ID, provider_id="ghost", name=ENV_ID)
    supervisor = supervisor_for(connection, clock, data_root)
    with pytest.raises(PluginNotFoundError):
        supervisor.prepare_environment(ENV_ID)


def test_reset_environment_raises_for_missing_environment(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = supervisor_env
    supervisor = supervisor_for(connection, clock, data_root)
    with pytest.raises(EnvironmentNotFoundError):
        supervisor.reset_environment("no-such-env")


def test_close_warm_logs_failure_result(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """cleanup 返回 ok=False：恰一条失败记录，载荷含环境、错误码与详情，其余环境继续关闭。"""
    connection, clock, _runs, _data_root = supervisor_env
    provider = _CleanupFailureProvider(
        error_code=ErrorCode.ENV_UNREACHABLE.value, detail="fake 清理失败"
    )
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    registry.register(PluginCapabilities(plugin_id="fake2", environment=FakeEnvironmentProvider()))
    repository = EnvironmentRepository(connection)
    _seed_ready_with_expired_warm(repository, ENV_ID)
    _seed_ready_with_expired_warm(repository, ENV_ID_2, provider_id="fake2")
    logger = _RecordingLogger()
    supervisor = _warm_close_supervisor(supervisor_env, registry, logger)
    tick_and_settle(supervisor)
    assert provider.calls == ["cleanup"]
    assert logger.warns == [
        {
            "event": "supervisor.warm_close_failed",
            "environment_id": ENV_ID,
            "error_code": ErrorCode.ENV_UNREACHABLE.value,
            "detail": "fake 清理失败",
        }
    ]
    service = EnvironmentService(connection, clock)
    assert service.get(ENV_ID).state is EnvironmentState.FAILED
    assert service.get(ENV_ID_2).state is EnvironmentState.OFF


def test_close_warm_retries_next_tick_after_failure(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """冲突丢弃不清保温到期：行回到 ready 后下一轮 tick 再关，失败逐轮各记一条。

    对手是手动复位（调用期间先提交 booting，保温关闭按失败结果收口）；复位成功
    探针确认 ready 后行重新满足 warm_expired，这一步由测试编排。
    """
    connection, clock, _runs, data_root = supervisor_env
    provider = _RaceDuringCleanupProvider()
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    repository = EnvironmentRepository(connection)
    _seed_ready_with_expired_warm(repository, ENV_ID)
    logger = _RecordingLogger()
    supervisor = _warm_close_supervisor(supervisor_env, registry, logger)
    other = open_database(data_root / "state.db")
    try:
        provider.on_cleanup = _rewrite_state_to(other, ENV_ID, EnvironmentState.BOOTING.value)
        tick_and_settle(supervisor)
        assert provider.calls == ["cleanup"]
        row_after_failure = repository.get(ENV_ID)
        assert row_after_failure is not None
        assert row_after_failure.warm_until == T0 - timedelta(minutes=1)
        assert [record["error_code"] for record in logger.warns] == [ErrorCode.ENV_NOT_READY.value]
        repository.set_state(ENV_ID, EnvironmentState.READY, at=T0, detail={"action": "test"})

        tick_and_settle(supervisor)
        assert provider.calls == ["cleanup", "cleanup"]
        assert [record["error_code"] for record in logger.warns] == [
            ErrorCode.ENV_NOT_READY.value,
            ErrorCode.ENV_NOT_READY.value,
        ]
        repository.set_state(ENV_ID, EnvironmentState.READY, at=T0, detail={"action": "test"})

        provider.on_cleanup = None
        tick_and_settle(supervisor)
        assert provider.calls == ["cleanup", "cleanup", "cleanup"]
        assert len(logger.warns) == 2
        record = EnvironmentService(connection, clock).get(ENV_ID)
        assert record.state is EnvironmentState.OFF
        assert record.warm_until is None
    finally:
        other.close()


def test_close_warm_exception_warns_with_error_field(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """cleanup 抛错走既有捕获：一条带 error 字段的 warn，状态与保温到期不动。"""
    connection, clock, _runs, _data_root = supervisor_env
    provider = _CleanupCrashProvider()
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    repository = EnvironmentRepository(connection)
    _seed_ready_with_expired_warm(repository, ENV_ID)
    logger = _RecordingLogger()
    supervisor = _warm_close_supervisor(supervisor_env, registry, logger)
    tick_and_settle(supervisor)
    assert provider.calls == ["cleanup"]
    assert logger.warns == [
        {
            "event": "supervisor.warm_close_failed",
            "environment_id": ENV_ID,
            "error": "fake 保温关闭失败",
        }
    ]
    record = EnvironmentService(connection, clock).get(ENV_ID)
    assert record.state is EnvironmentState.READY
    assert record.warm_until == T0 - timedelta(minutes=1)
