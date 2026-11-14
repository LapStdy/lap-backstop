"""场景：清理失败与恢复预算的收尾口径：provider 清理至多一次、失败码与重试预算。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any

from contracts import EnvironmentRef, PhaseResult, RunContext, RunState
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.worker.executor import WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from tests.scenarios.worker_lifecycle_support import (
    TEMPLATE_ID,
    _registry_with,
    prepare_run,
)


class _RecordingLogger:
    """把级别与字段留在内存里，供断言 WARN 用。"""

    def __init__(self) -> None:
        self.entries: list[tuple[str, dict[str, Any]]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("DEBUG", dict(fields)))

    def info(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("INFO", dict(fields)))

    def warn(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("WARNING", dict(fields)))

    def error(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("ERROR", dict(fields)))


class _FailingCleanupProvider(FakeEnvironmentProvider):
    """清理必定抛异常，验证收尾异常只降级成 WARN、不打断恢复。"""

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        self.calls.append("cleanup")
        raise RuntimeError("清理失败")


class _FailedResultCleanupProvider(FakeEnvironmentProvider):
    """清理返回失败结果，验证失败口径沿用 provider 报告的错误码。"""

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        self.calls.append("cleanup")
        now = ctx.clock.utc_now()
        return PhaseResult(
            ok=False,
            error_code="E_ENV_UNREACHABLE",
            detail="fake 清理失败",
            started_at=now,
            ended_at=now,
        )


def set_recovery_policy(connection: sqlite3.Connection, policy: Mapping[str, Any]) -> None:
    """改写模板的恢复策略；场景用它收缩恢复预算。"""
    connection.execute(
        "UPDATE job_templates SET recovery_policy = ? WHERE id = ?",
        (json.dumps(policy), TEMPLATE_ID),
    )


def test_cleanup_failure_logs_warning_and_recovery_continues(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "fail")
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=_FailingCleanupProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    logger = _RecordingLogger()
    result = WorkerExecutor(connection, clock, registry, data_root=data_root, log=logger).execute(
        run_id
    )
    assert result.state is RunState.RECOVERING
    assert result.error_code == "E_SCRIPT_TASK_FAILED"
    assert any(
        level == "WARNING" and fields.get("event") == "executor.cleanup_failed"
        for level, fields in logger.entries
    )


def test_run_succeeds_with_single_cleanup_call(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/09：每次执行尝试的 provider cleanup 至多调用一次。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    provider = FakeEnvironmentProvider()
    result = WorkerExecutor(
        connection, clock, _registry_with(environment=provider), data_root=data_root
    ).execute(run_id)
    assert result.state is RunState.SUCCEEDED
    assert provider.calls.count("cleanup") == 1


def test_cleanup_failure_fails_run_with_single_cleanup_call(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """清理失败按既有口径落 failed；同轮收尾不再对 provider 二次清理。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    provider = _FailedResultCleanupProvider()
    result = WorkerExecutor(
        connection, clock, _registry_with(environment=provider), data_root=data_root
    ).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_ENV_UNREACHABLE"
    assert provider.calls.count("cleanup") == 1
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_cleanup_exception_ends_failed_with_single_cleanup_call(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """清理抛异常不触发第二轮收尾：计数为 1，恢复预算耗尽按停手落终态。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    set_recovery_policy(connection, {"max_attempts": 1})
    provider = _FailingCleanupProvider()
    result = WorkerExecutor(
        connection, clock, _registry_with(environment=provider), data_root=data_root
    ).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_RECOVERY_EXHAUSTED"
    assert provider.calls.count("cleanup") == 1
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_recovery_exhaustion_fails_run_with_single_cleanup_call(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """恢复耗尽停手清理一次，租约释放，运行落 failed。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "fail")
    set_recovery_policy(connection, {"max_attempts": 1})
    provider = FakeEnvironmentProvider()
    result = WorkerExecutor(
        connection, clock, _registry_with(environment=provider), data_root=data_root
    ).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_RECOVERY_EXHAUSTED"
    assert provider.calls.count("cleanup") == 1
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_cleanup_exception_aborts_with_plugin_crash_when_budget_remains(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """清理异常且恢复预算未耗尽：cleaning 中不重试，按停手落 failed。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    provider = _FailingCleanupProvider()
    result = WorkerExecutor(
        connection, clock, _registry_with(environment=provider), data_root=data_root
    ).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_PLUGIN_CRASH"
    assert provider.calls.count("cleanup") == 1
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_recovery_retry_cleans_once_per_attempt(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """恢复重试是新执行尝试：两次尝试各清理一次，合计两次。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "fail")
    provider = FakeEnvironmentProvider()
    executor = WorkerExecutor(
        connection, clock, _registry_with(environment=provider), data_root=data_root
    )
    first = executor.execute(run_id)
    assert first.state is RunState.RECOVERING
    assert first.attempt == 1
    clock.advance(timedelta(seconds=120))
    second = executor.execute(run_id)
    assert second.state is RunState.RECOVERING
    assert second.attempt == 2
    assert provider.calls.count("cleanup") == 2
