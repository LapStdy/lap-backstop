"""场景：worker 执行器跑完一条 JobRun 生命周期。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    EnvironmentRef,
    EnvironmentState,
    EventType,
    Logger,
    PhaseResult,
    PhaseState,
    RunContext,
    RunState,
    ScriptHandle,
    ScriptObservation,
    ScriptState,
    Verdict,
    VerdictStatus,
)
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.errors import IllegalRunTransitionError
from core.jobs.run_service import JobRunService
from core.plugins.errors import (
    WorkerCallError,
    WorkerCancelledError,
    WorkerCrashError,
    WorkerTimeoutError,
)
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.worker.executor import CancelFlag, WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.events import EventLog
from storage.repositories.job_runs import NewRun
from storage.repositories.leases import LeaseRow
from storage.repositories.run_phases import RunPhaseRepository
from storage.repositories.verdicts import VerdictRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"


DEFAULT_POLICY: Mapping[str, Any] = {
    "id": "fake-policy",
    "verifier_plugin_id": "fake",
    "required_evidence": ["log"],
    "params": {},
}


def insert_template(
    connection: sqlite3.Connection,
    scenario: str,
    *,
    policy: Mapping[str, Any] | None = None,
) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": scenario}),
            json.dumps(DEFAULT_POLICY if policy is None else policy),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


def make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    return registry


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


class _RunningScriptAdapter(FakeScriptAdapter):
    """永远报告运行中，模拟迟迟不给结论的脚本。"""

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        return ScriptObservation(state=ScriptState.RUNNING, since=ctx.clock.utc_now())


class _CancellingScriptAdapter(FakeScriptAdapter):
    """观察期间发出取消请求，模拟用户在执行阶段按下停止。"""

    def __init__(self, on_observe: Callable[[str], None]) -> None:
        super().__init__()
        self._on_observe = on_observe

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self._on_observe(ctx.run_id)
        return ScriptObservation(state=ScriptState.RUNNING, since=ctx.clock.utc_now())


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


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    data_root = tmp_path / "data"
    try:
        yield connection, clock, runs, data_root
    finally:
        connection.close()


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    scenario: str,
    *,
    scheduled_at: datetime = T0,
    deadline_at: datetime | None = None,
    key: str = "schedule-1:2026-10-23T12:00:00+00:00",
    policy: Mapping[str, Any] | None = None,
) -> str:
    insert_template(connection, scenario, policy=policy)
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=deadline_at,
        )
    )
    runs.enqueue(record.id)
    return record.id


def executor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    *,
    cancel: CancelFlag | None = None,
    log: Logger | None = None,
) -> WorkerExecutor:
    return WorkerExecutor(
        connection,
        clock,
        make_registry(),
        data_root=data_root,
        cancel=cancel,
        log=log,
    )


def set_recovery_policy(connection: sqlite3.Connection, policy: Mapping[str, Any]) -> None:
    """改写模板的恢复策略；场景用它收缩恢复预算。"""
    connection.execute(
        "UPDATE job_templates SET recovery_policy = ? WHERE id = ?",
        (json.dumps(policy), TEMPLATE_ID),
    )


def phases(connection: sqlite3.Connection, run_id: str) -> dict[str, PhaseState]:
    rows = RunPhaseRepository(connection).for_run(run_id)
    return {row.phase: row.state for row in rows}


def test_instant_scenario_succeeds_end_to_end(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.SUCCEEDED
    assert result.finished_at is not None
    verdicts = VerdictRepository(connection).for_run(run_id)
    assert [item.status for item in verdicts] == [VerdictStatus.COMPLETED]
    evidence = connection.execute(
        "SELECT COUNT(*) FROM evidence WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert evidence is not None
    assert int(evidence[0]) == 1
    assert phases(connection, run_id) == {
        "prepare": PhaseState.OK,
        "execute": PhaseState.OK,
        "verify": PhaseState.OK,
        "cleanup": PhaseState.OK,
    }
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_executor_waits_for_scheduled_time(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(
        connection,
        runs,
        "instant",
        scheduled_at=T0 + timedelta(hours=1),
    )
    executor = executor_for(connection, clock, data_root)
    waiting = executor.execute(run_id)
    assert waiting.state is RunState.WAITING
    clock.advance(timedelta(hours=1))
    finished = executor.execute(run_id)
    assert finished.state is RunState.SUCCEEDED


def test_fail_scenario_enters_recovery(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "fail")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.RECOVERING
    assert result.attempt == 1


def test_partial_scenario_ends_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "partial")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert [item.status for item in VerdictRepository(connection).for_run(run_id)] == [
        VerdictStatus.UNKNOWN
    ]


def test_hang_scenario_stops_script_and_ends_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "hang")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert phases(connection, run_id)["execute"] is PhaseState.TIMED_OUT


def unknown_reason(connection: sqlite3.Connection, run_id: str) -> str | None:
    """UNKNOWN 收尾那次状态迁移的 reason；没有返回 None。"""
    for event in EventLog(connection).events_for_run(run_id):
        if event.type != EventType.RUN_STATE_CHANGED.value:
            continue
        if event.payload.get("to_state") != RunState.UNKNOWN.value:
            continue
        return str(event.payload["reason"])
    return None


def test_verify_without_policy_id_blames_template(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """历史模板策略缺 id：unknown 收尾，原因指向模板缺策略而不是缺证据。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant", policy={})
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert result.error_code == "E_CONFIG"
    assert phases(connection, run_id)["verify"] is PhaseState.FAILED
    assert unknown_reason(connection, run_id) == "verify_no_policy"


def test_verify_with_missing_evidence_blames_evidence(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_MissingEvidenceVerifier())
    result = WorkerExecutor(connection, clock, registry, data_root=data_root).execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert result.error_code == "E_VERIFY_NO_EVIDENCE"
    assert unknown_reason(connection, run_id) == "verify_no_evidence"


def test_crash_scenario_enters_recovery_with_plugin_crash(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "crash")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.RECOVERING
    assert result.error_code == "E_PLUGIN_CRASH"
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_missed_deadline_fails_before_execution(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(
        connection,
        runs,
        "instant",
        deadline_at=T0 + timedelta(minutes=30),
    )
    clock.advance(timedelta(hours=1))
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"


def test_cancel_flag_prevents_start(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    flag = CancelFlag()
    flag.cancel()
    result = executor_for(connection, clock, data_root, cancel=flag).execute(run_id)
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


class _NoEvidenceVerifier:
    def verify(self, ctx: RunContext) -> Verdict:
        return Verdict(
            status=VerdictStatus.COMPLETED,
            policy_id="fake-policy",
            confidence=1.0,
            reason="没有证据",
            evidence_ids=(),
            created_at=ctx.clock.utc_now(),
        )


def test_verifier_without_evidence_ends_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=_NoEvidenceVerifier(),
        )
    )
    executor = WorkerExecutor(connection, clock, registry, data_root=data_root)
    result = executor.execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert result.error_code == "E_VERIFY_NO_EVIDENCE"


def test_missing_template_marks_run_failed_with_config_error(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    # 模板可能在排队后被外部清掉；外键只在该连接上临时关掉才能造出这种状态。
    connection.execute("PRAGMA foreign_keys = OFF")
    connection.execute("DELETE FROM job_templates WHERE id = ?", (TEMPLATE_ID,))
    connection.execute("PRAGMA foreign_keys = ON")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_CONFIG"
    assert phases(connection, run_id) == {}
    events = runs.events(run_id)
    assert events[-2].payload["reason"] == "template_missing"
    assert events[-1].type == "run.finished"


def test_unfinished_script_ends_with_deadline_not_hung(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=_RunningScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    result = WorkerExecutor(connection, clock, registry, data_root=data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"
    execute_phase = next(
        row for row in RunPhaseRepository(connection).for_run(run_id) if row.phase == "execute"
    )
    assert execute_phase.state is PhaseState.TIMED_OUT
    assert execute_phase.detail["detail"] == "deadline"


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


class _RaisingVerifier:
    """验证阶段抛出指定异常，用来覆盖跨进程错误到运行终态的映射。"""

    def __init__(
        self,
        error: WorkerTimeoutError | WorkerCancelledError | WorkerCrashError | WorkerCallError,
    ) -> None:
        self._error = error

    def verify(self, ctx: RunContext) -> Verdict:
        raise self._error


class _MissingEvidenceVerifier(FakeVerifier):
    """结论引用不存在的证据，用来核对缺证据与缺策略的收尾原因区分。"""

    def verify(self, ctx: RunContext) -> Verdict:
        verdict = super().verify(ctx)
        return replace(verdict, evidence_ids=("missing-1",))


def _registry_with(
    *,
    environment: FakeEnvironmentProvider | None = None,
    script: FakeScriptAdapter | None = None,
    verifier: Any | None = None,
) -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider() if environment is None else environment,
            script=FakeScriptAdapter() if script is None else script,
            verifier=FakeVerifier() if verifier is None else verifier,
        )
    )
    return registry


def test_worker_timeout_ends_with_deadline(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerTimeoutError("调用超时")))
    result = WorkerExecutor(connection, clock, registry, data_root=data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_worker_cancel_ends_run_cancelled(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerCancelledError("已叫停")))
    result = WorkerExecutor(connection, clock, registry, data_root=data_root).execute(run_id)
    assert result.state is RunState.CANCELLED
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_worker_crash_keeps_plugin_crash_code(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerCrashError("进程退出")))
    result = WorkerExecutor(connection, clock, registry, data_root=data_root).execute(run_id)
    assert result.state is RunState.RECOVERING
    assert result.error_code == "E_PLUGIN_CRASH"


def test_worker_call_error_keeps_plugin_code(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerCallError("插件方法抛异常")))
    result = WorkerExecutor(connection, clock, registry, data_root=data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_PLUGIN"
