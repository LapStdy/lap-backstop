"""场景：执行窗口内的取消与竞争迁出：出队叫停、各阶段取消退让与状态机拒绝。"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest

from contracts import (
    EnvironmentRef,
    EnvironmentState,
    PhaseResult,
    PhaseState,
    RunContext,
    RunState,
    ScriptHandle,
    ScriptObservation,
    ScriptState,
)
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.errors import IllegalRunTransitionError
from core.jobs.run_service import JobRunService
from core.plugins.errors import WorkerCancelledError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.worker.executor import CancelFlag, WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage.repositories.leases import LeaseRow
from tests.scenarios.worker_lifecycle_support import (
    ENV_ID,
    ExecutorOptions,
    _RaisingVerifier,
    _registry_with,
    executor_for,
    phases,
    prepare_run,
)


class _CancellingScriptAdapter(FakeScriptAdapter):
    """观察期间发出取消请求，模拟用户在执行阶段按下停止。"""

    def __init__(self, on_observe: Callable[[str], None]) -> None:
        super().__init__()
        self._on_observe = on_observe

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self._on_observe(ctx.run_id)
        return ScriptObservation(state=ScriptState.RUNNING, since=ctx.clock.utc_now())


class _CancellingCleanupProvider(FakeEnvironmentProvider):
    """清理期间发出取消请求，模拟用户在 cleaning 阶段按下停止。"""

    def __init__(self, on_cleanup: Callable[[str], None]) -> None:
        super().__init__()
        self._on_cleanup = on_cleanup
        self.cleanup_calls = 0

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        self.cleanup_calls += 1
        self._on_cleanup(ctx.run_id)
        return super().cleanup(env, ctx, keep_warm)


class _RacingLeaseService(LeaseService):
    """获取租约后立即执行注入动作：模拟请求线程在出队窗口内抢先改动运行。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: VirtualClock,
        on_acquire: Callable[[str], None],
    ) -> None:
        super().__init__(connection, clock)
        self._on_acquire = on_acquire

    def acquire(
        self,
        run_id: str,
        resource_ids: tuple[str, ...],
        *,
        ttl: timedelta | None = None,
    ) -> tuple[LeaseRow, ...]:
        leases = super().acquire(run_id, resource_ids, ttl=ttl)
        self._on_acquire(run_id)
        return leases


def test_cancel_flag_prevents_start(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    flag = CancelFlag()
    flag.cancel()
    result = executor_for(connection, clock, data_root, ExecutorOptions(cancel=flag)).execute(
        run_id
    )
    assert result.state is RunState.CANCELLED
    assert phases(connection, run_id) == {}


def test_cancel_during_dequeue_yields_cancelled_without_lease(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """docs/09 出队窗口内的叫停：叫停抢先落库，执行器退让、不留活跃租约。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    executor = executor_for(connection, clock, data_root)

    def cancel_while_dequeuing(dequeuing_run: str) -> None:
        runs.request_cancel(dequeuing_run, reason="user_stop")

    monkeypatch.setattr(
        executor,
        "_leases",
        _RacingLeaseService(connection, clock, cancel_while_dequeuing),
    )
    result = executor.execute(run_id)
    assert result.state is RunState.CANCELLED
    assert result.finished_at is not None
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
    assert phases(connection, run_id) == {}
    events = runs.events(run_id)
    assert [event.type for event in events].count("run.interrupted") == 1
    interrupted = next(event for event in events if event.type == "run.interrupted")
    assert interrupted.payload["reason"] == "user_stop"
    finished = next(event for event in events if event.type == "run.finished")
    assert finished.payload["status"] == "cancelled"


def test_execute_after_dequeue_cancel_is_idempotent(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """退让收尾后用干净执行器再推进同一运行：不抛异常，状态与事件都不变。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    executor = executor_for(connection, clock, data_root)

    def cancel_while_dequeuing(dequeuing_run: str) -> None:
        runs.request_cancel(dequeuing_run, reason="user_stop")

    monkeypatch.setattr(
        executor,
        "_leases",
        _RacingLeaseService(connection, clock, cancel_while_dequeuing),
    )
    executor.execute(run_id)
    before = runs.events(run_id)
    again = executor_for(connection, clock, data_root).execute(run_id)
    assert again.state is RunState.CANCELLED
    assert runs.events(run_id) == before


def test_transition_from_cancelled_to_preparing_is_rejected(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """退让不放宽状态机：终态运行推进 preparing 仍抛 IllegalRunTransitionError。"""
    connection, _clock, runs, _ = env
    run_id = prepare_run(connection, runs, "instant")
    runs.request_cancel(run_id, reason="user_stop")
    with pytest.raises(IllegalRunTransitionError):
        runs.transition(run_id, RunState.PREPARING)


def test_non_cancel_conflict_during_dequeue_still_raises(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """出队窗口内非叫停的并发迁出（到期失败）不走退让：非法迁移照常抛出。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    executor = executor_for(connection, clock, data_root)

    def fail_while_dequeuing(dequeuing_run: str) -> None:
        runs.transition(dequeuing_run, RunState.FAILED, reason="deadline", error_code="E_DEADLINE")

    monkeypatch.setattr(
        executor,
        "_leases",
        _RacingLeaseService(connection, clock, fail_while_dequeuing),
    )
    with pytest.raises(IllegalRunTransitionError):
        executor.execute(run_id)
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_cancel_during_cleanup_records_cancelled_after_one_cleanup(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/09「cleaning 阶段取消：等 cleanup 完成，再记 cancelled」。

    docs/02 允许 cleaning → cancelled；cleanup 只执行一次，取消不再触发一轮收尾。
    """
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    flag = CancelFlag()

    def request_cancel(during_run: str) -> None:
        runs.request_cancel(during_run, reason="user_stop")
        flag.cancel()

    provider = _CancellingCleanupProvider(request_cancel)
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=provider,
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    result = WorkerExecutor(
        connection,
        clock,
        registry,
        data_root=data_root,
        cancel=flag,
    ).execute(run_id)
    assert result.state is RunState.CANCELLED
    assert result.finished_at is not None
    assert provider.cleanup_calls == 1
    assert phases(connection, run_id)["cleanup"] is PhaseState.OK
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF
    events = runs.events(run_id)
    assert [event.type for event in events].count("run.interrupted") == 1
    changed = [event for event in events if event.type == "run.state.changed"]
    assert (changed[-1].payload["from_state"], changed[-1].payload["to_state"]) == (
        "cleaning",
        "cancelled",
    )
    assert events[-1].type == "run.finished"
    assert events[-1].payload["status"] == "cancelled"


def test_cancel_during_verify_cleans_once_and_ends_cancelled(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """未进入 cleaning 的取消收尾清理一次，随后记 cancelled。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    provider = FakeEnvironmentProvider()
    verifier = _RaisingVerifier(WorkerCancelledError("已叫停"))
    result = WorkerExecutor(
        connection,
        clock,
        _registry_with(environment=provider, verifier=verifier),
        data_root=data_root,
    ).execute(run_id)
    assert result.state is RunState.CANCELLED
    assert provider.calls.count("cleanup") == 1


def test_cancel_during_execution_cleans_once_and_ends_cancelled(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """执行阶段取消先停脚本再清理一次，随后记 cancelled。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    flag = CancelFlag()

    def request_cancel(during_run: str) -> None:
        runs.request_cancel(during_run, reason="user_stop")
        flag.cancel()

    provider = FakeEnvironmentProvider()
    result = WorkerExecutor(
        connection,
        clock,
        _registry_with(
            environment=provider,
            script=_CancellingScriptAdapter(request_cancel),
        ),
        data_root=data_root,
        cancel=flag,
    ).execute(run_id)
    assert result.state is RunState.CANCELLED
    assert provider.calls.count("cleanup") == 1
