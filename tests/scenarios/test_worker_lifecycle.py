"""场景：worker 执行器跑完一条 JobRun 生命周期。"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from contracts import (
    EnvironmentState,
    EventType,
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
from core.jobs.run_service import JobRunService
from core.plugins.errors import (
    WorkerCallError,
    WorkerCancelledError,
    WorkerCrashError,
    WorkerTimeoutError,
)
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.worker.executor import ExecutorOptions, WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage.repositories.events import EventLog
from storage.repositories.run_phases import RunPhaseRepository
from storage.repositories.verdicts import VerdictRepository
from tests.scenarios.worker_lifecycle_support import (
    ENV_ID,
    T0,
    TEMPLATE_ID,
    RunOverrides,
    _RaisingVerifier,
    _registry_with,
    executor_for,
    phases,
    prepare_run,
)


class _RunningScriptAdapter(FakeScriptAdapter):
    """永远报告运行中，模拟迟迟不给结论的脚本。"""

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        return ScriptObservation(state=ScriptState.RUNNING, since=ctx.clock.utc_now())


def unknown_reason(connection: sqlite3.Connection, run_id: str) -> str | None:
    """UNKNOWN 收尾那次状态迁移的 reason；没有返回 None。"""
    for event in EventLog(connection).events_for_run(run_id):
        if event.type != EventType.RUN_STATE_CHANGED.value:
            continue
        if event.payload.get("to_state") != RunState.UNKNOWN.value:
            continue
        return str(event.payload["reason"])
    return None


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


class _MissingEvidenceVerifier(FakeVerifier):
    """结论引用不存在的证据，用来核对缺证据与缺策略的收尾原因区分。"""

    def verify(self, ctx: RunContext) -> Verdict:
        verdict = super().verify(ctx)
        return replace(verdict, evidence_ids=("missing-1",))


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
        connection, runs, "instant", RunOverrides(scheduled_at=T0 + timedelta(hours=1))
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


def test_verify_without_policy_id_blames_template(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """历史模板策略缺 id：unknown 收尾，原因指向模板缺策略而不是缺证据。"""
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant", RunOverrides(policy={}))
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
    result = WorkerExecutor(
        connection, clock, registry, ExecutorOptions(data_root=data_root)
    ).execute(run_id)
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
        connection, runs, "instant", RunOverrides(deadline_at=T0 + timedelta(minutes=30))
    )
    clock.advance(timedelta(hours=1))
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"


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
    executor = WorkerExecutor(connection, clock, registry, ExecutorOptions(data_root=data_root))
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
    result = WorkerExecutor(
        connection, clock, registry, ExecutorOptions(data_root=data_root)
    ).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"
    execute_phase = next(
        row for row in RunPhaseRepository(connection).for_run(run_id) if row.phase == "execute"
    )
    assert execute_phase.state is PhaseState.TIMED_OUT
    assert execute_phase.detail["detail"] == "deadline"


def test_worker_timeout_ends_with_deadline(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerTimeoutError("调用超时")))
    result = WorkerExecutor(
        connection, clock, registry, ExecutorOptions(data_root=data_root)
    ).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_worker_cancel_ends_run_cancelled(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerCancelledError("已叫停")))
    result = WorkerExecutor(
        connection, clock, registry, ExecutorOptions(data_root=data_root)
    ).execute(run_id)
    assert result.state is RunState.CANCELLED
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_worker_crash_keeps_plugin_crash_code(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerCrashError("进程退出")))
    result = WorkerExecutor(
        connection, clock, registry, ExecutorOptions(data_root=data_root)
    ).execute(run_id)
    assert result.state is RunState.RECOVERING
    assert result.error_code == "E_PLUGIN_CRASH"


def test_worker_call_error_keeps_plugin_code(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = _registry_with(verifier=_RaisingVerifier(WorkerCallError("插件方法抛异常")))
    result = WorkerExecutor(
        connection, clock, registry, ExecutorOptions(data_root=data_root)
    ).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_PLUGIN"
