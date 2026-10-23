"""worker 执行器：把排队运行推过 prepare → execute → verify → cleanup。

执行器跑在 worker 侧；本实现先在处理进程内同步调用插件，超时与取消靠注入的时钟
和阶段边界检查，后续换成子进程 RPC 时生命周期不变。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import (
    CancelToken,
    Clock,
    EnvironmentRef,
    EventType,
    EvidenceKind,
    Logger,
    PhaseState,
    RecoveryAction,
    RunContext,
    RunState,
    ScriptAdapter,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    StopMode,
    Verdict,
    VerdictStatus,
    Verifier,
)
from core.environments.service import EnvironmentCall, EnvironmentService
from core.events.evidence import EvidenceService
from core.events.verdicts import VerdictError, VerdictService
from core.jobs.phases import PhaseTracker
from core.jobs.run_service import JobRunService
from core.logging import NullLogger
from core.plugins.registry import PluginRegistry
from core.recovery.policy import RecoveryPolicy
from core.resources.service import LeaseService
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRepository, JobTemplateRow
from storage.repositories.resources import ResourceRepository

_MAX_OBSERVATIONS = 10
_DEFAULT_DEADLINE = timedelta(minutes=30)
_TERMINAL_BY_VERDICT: Mapping[VerdictStatus, RunState] = {
    VerdictStatus.COMPLETED: RunState.SUCCEEDED,
    VerdictStatus.FAILED: RunState.FAILED,
    VerdictStatus.UNKNOWN: RunState.UNKNOWN,
}

Emit = Callable[[str, Mapping[str, Any]], None]
RecordEvidence = Callable[[str, Path, str], str]


class CancelFlag:
    """内核持有的取消标记；worker 在阶段边界检查。"""

    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        """置位取消标记。"""
        self._cancelled = True

    def is_cancelled(self) -> bool:
        """是否已请求取消。"""
        return self._cancelled


@dataclass(slots=True)
class RunScope:
    """一次执行共享的状态：阶段方法只拿这一份，不再传宽参数表。"""

    run: JobRunRecord
    template: JobTemplateRow
    call: EnvironmentCall
    script: ScriptAdapter
    verifier: Verifier
    context: RunContext
    handle: ScriptHandle | None = None
    verdict: Verdict | None = None
    verify_error: str | None = None

    @property
    def run_id(self) -> str:
        """运行 id 的短别名；日志与事件用。"""
        return self.run.id


class WorkerExecutor:
    """一次 JobRun 的四阶段执行。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        registry: PluginRegistry,
        *,
        data_root: Path,
        log: Logger | None = None,
        keep_warm_minutes: int = 0,
        cancel: CancelToken | None = None,
    ) -> None:
        self._connection = connection
        self._clock = clock
        self._registry = registry
        self._data_root = data_root
        self._keep_warm_minutes = keep_warm_minutes
        self._cancel: CancelToken = CancelFlag() if cancel is None else cancel
        self._log: Logger = NullLogger() if log is None else log
        self._runs = JobRunService(connection, clock)
        self._templates = JobTemplateRepository(connection)
        self._environments = EnvironmentService(connection, clock)
        self._resources = ResourceRepository(connection)
        self._leases = LeaseService(connection, clock)
        self._phases = PhaseTracker(connection, clock)
        self._evidence = EvidenceService(connection, data_root / "evidence", clock)
        self._verdicts = VerdictService(connection, clock)
        self._events = EventLog(connection)

    def execute(self, run_id: str) -> JobRunRecord:
        """推进一步：恢复等待、准备、执行、验证、清理，直到终态或等待时机。

        Args:
            run_id: 要推进的运行 id。

        Returns:
            推进后的运行记录；未到 scheduled_at 或不在可推进状态时原样返回。

        Raises:
            RunNotFoundError: 运行不存在。
            PluginError: 模板声明的插件或能力没有注册。
        """
        run = self._runs.get(run_id)
        if run.state is RunState.RECOVERING:
            run = self.resume(run_id)
        if run.state not in (RunState.QUEUED, RunState.WAITING):
            return run
        template = self._templates.get(run.template_id)
        if template is None:
            return self._fail_missing_template(run)
        scope = self._begin(run, template)
        if self._cancel.is_cancelled():
            return self._cancel_run(scope, started=False)
        if scope.run.state is RunState.QUEUED:
            self._leases.acquire(scope.run_id, (scope.call.environment_id,))
            scope.run = self._runs.transition(scope.run.id, RunState.PREPARING)
        try:
            return (
                self._prepare(scope)
                or self._run_script(scope)
                or self._verify(scope)
                or self._clean(scope)
            )
        except Exception as error:  # 插件异常不能拖垮内核：按崩溃收尾
            self._log.error(
                {"event": "executor.error", "run_id": scope.run_id, "error": str(error)}
            )
            return self._recover(scope, "E_PLUGIN_CRASH", str(error))

    def _begin(self, run: JobRunRecord, template: JobTemplateRow) -> RunScope:
        """组装一次执行用的插件、环境引用与运行上下文。"""
        provider = self._registry.environment(template.plugin_id)
        script = self._registry.script(template.plugin_id)
        verifier = self._registry.verifier(template.plugin_id)
        environment_id = template.environment_id
        self._resources.ensure(
            environment_id,
            resource_type="environment",
            name=environment_id,
        )
        self._environments.register(
            environment_id,
            provider_id=template.plugin_id,
            name=environment_id,
        )
        context = self._build_context(run, template)
        call = EnvironmentCall(
            environment_id=environment_id,
            provider=provider,
            ref=EnvironmentRef(id=environment_id, provider_id=template.plugin_id),
            ctx=context,
        )
        return RunScope(
            run=run,
            template=template,
            call=call,
            script=script,
            verifier=verifier,
            context=context,
        )

    def _prepare(self, scope: RunScope) -> JobRunRecord | None:
        """准备阶段：让插件把环境带到 ready，失败按恢复策略收尾。"""
        if scope.run.state is not RunState.PREPARING:
            return None
        phase = self._phases.start(scope.run.id, "prepare", attempt=scope.run.attempt)
        prepared = self._environments.prepare(scope.call)
        self._phases.finish(
            phase,
            state=PhaseState.OK if prepared.ok else PhaseState.FAILED,
            detail=prepared.detail,
        )
        if not prepared.ok:
            return self._recover(scope, prepared.error_code or "E_ENV_WAKE_TIMEOUT", "prepare")
        scope.run = self._runs.transition(scope.run.id, RunState.WAITING)
        return None

    def _run_script(self, scope: RunScope) -> JobRunRecord | None:
        """执行阶段：到点后启动脚本、采集信号，超时与失败按错误码收尾。"""
        if self._cancel.is_cancelled():
            return self._cancel_run(scope, started=True)
        now = self._clock.utc_now()
        if scope.run.scheduled_at is not None and now < scope.run.scheduled_at:
            return scope.run
        if scope.run.deadline_at is not None and now >= scope.run.deadline_at:
            return self._recover(scope, "E_DEADLINE", "deadline")
        scope.run = self._runs.transition(scope.run.id, RunState.EXECUTING)
        self._environments.mark_busy(scope.call)
        phase = self._phases.start(scope.run.id, "execute", attempt=scope.run.attempt)
        handle = scope.script.launch(
            ScriptRef(
                id=scope.template.id,
                plugin_id=scope.template.plugin_id,
                config=dict(scope.template.config),
            ),
            scope.context,
        )
        scope.handle = handle
        observation = self._wait_for_script(scope.script, handle, scope.context)
        if observation is None:
            self._phases.finish(phase, state=PhaseState.TIMED_OUT, detail="deadline")
            self._stop_script(scope)
            return self._recover(scope, "E_DEADLINE", "deadline")
        for signal in observation.signals:
            self._events.append_run(
                scope.run.id,
                EventType.RUN_SIGNAL,
                {"run_id": scope.run.id, "kind": signal.kind, "payload": dict(signal.payload)},
                at=self._clock.utc_now(),
            )
        hung = observation.state is ScriptState.HUNG
        script_failed = (
            observation.last_signal is not None and observation.last_signal.kind == "task_failed"
        )
        self._phases.finish(
            phase,
            state=(
                PhaseState.TIMED_OUT
                if hung
                else PhaseState.FAILED
                if script_failed
                else PhaseState.OK
            ),
            detail=observation.state.value,
        )
        if hung:
            self._stop_script(scope)
        if script_failed:
            return self._recover(scope, "E_SCRIPT_TASK_FAILED", "script")
        if self._cancel.is_cancelled():
            return self._cancel_run(scope, started=True)
        return None

    def _verify(self, scope: RunScope) -> JobRunRecord | None:
        """验证阶段：插件结论先与证据核对再落库，缺证据留给清理阶段收尾。"""
        scope.run = self._runs.transition(scope.run.id, RunState.VERIFYING)
        phase = self._phases.start(scope.run.id, "verify", attempt=scope.run.attempt)
        verdict = scope.verifier.verify(scope.context)
        if not isinstance(verdict, Verdict):
            raise TypeError("验证器返回的不是 Verdict")
        try:
            _, effective = self._verdicts.record(scope.run.id, verdict)
        except VerdictError as error:
            scope.verify_error = error.code
        else:
            scope.verdict = effective
        self._phases.finish(
            phase,
            state=PhaseState.FAILED if scope.verify_error is not None else PhaseState.OK,
            detail=scope.verify_error or "",
        )
        return None

    def _clean(self, scope: RunScope) -> JobRunRecord:
        """清理阶段：停脚本、清环境、释放租约，再按结论落终态。"""
        scope.run = self._runs.transition(scope.run.id, RunState.CLEANING)
        phase = self._phases.start(scope.run.id, "cleanup", attempt=scope.run.attempt)
        self._stop_script(scope)
        cleaned = self._environments.cleanup(
            scope.call,
            keep_warm=self._keep_warm_minutes > 0,
            warm_minutes=self._keep_warm_minutes,
        )
        self._phases.finish(
            phase,
            state=PhaseState.OK if cleaned.ok else PhaseState.FAILED,
            detail=cleaned.detail,
        )
        self._leases.release(scope.run.id, reason="cleanup")
        if not cleaned.ok:
            return self._fail(scope, cleaned.error_code or "E_ENV_UNREACHABLE", "cleanup")
        if scope.verify_error is not None:
            return self._runs.transition(
                scope.run.id,
                RunState.UNKNOWN,
                reason="verify_no_evidence",
                error_code=scope.verify_error,
            )
        target = (
            RunState.UNKNOWN
            if scope.verdict is None
            else _TERMINAL_BY_VERDICT[scope.verdict.status]
        )
        return self._runs.transition(scope.run.id, target, reason="verdict")

    def _wait_for_script(
        self,
        script: ScriptAdapter,
        handle: ScriptHandle,
        context: RunContext,
    ) -> ScriptObservation | None:
        """观察脚本直到结束；观察次数用尽或到截止时间返回 None，由调用方按超时收尾。"""
        for _ in range(_MAX_OBSERVATIONS):
            observation = script.observe(handle, context)
            if observation.state is not ScriptState.RUNNING:
                return observation
            if self._clock.utc_now() >= context.deadline_at:
                return None
        return None

    def resume(self, run_id: str) -> JobRunRecord:
        """恢复等待到点时把运行放回队列；未到点或不在 recovering 时原样返回。"""
        run = self._runs.get(run_id)
        if run.state is not RunState.RECOVERING:
            return run
        retry_at = _retry_at(run.detail)
        if retry_at is not None and self._clock.utc_now() < retry_at:
            return run
        return self._runs.transition(run_id, RunState.QUEUED, reason="recovery")

    def _fail_missing_template(self, run: JobRunRecord) -> JobRunRecord:
        """模板缺失是配置错误：直接记失败，不让运行卡在排队里等一个不会来的模板。"""
        self._log.error(
            {
                "event": "executor.template_missing",
                "run_id": run.id,
                "template_id": run.template_id,
                "error_code": "E_CONFIG",
            }
        )
        current = self._runs.get(run.id)
        if current.state in (RunState.EXECUTING, RunState.VERIFYING):
            current = self._runs.transition(run.id, RunState.CLEANING, reason="template_missing")
        return self._runs.transition(
            run.id,
            RunState.FAILED,
            reason="template_missing",
            error_code="E_CONFIG",
        )

    def _recover(self, scope: RunScope, error_code: str, reason: str) -> JobRunRecord:
        """按模板策略决定恢复动作；退避等待由 resume 按 retry_at 控制。"""
        try:
            policy = RecoveryPolicy.from_mapping(scope.template.recovery_policy)
        except ValueError:
            return self._fail(scope, "E_CONFIG", reason)
        run = self._runs.get(scope.run_id)
        decision = policy.decide(error_code=error_code, attempt=run.attempt)
        now = self._clock.utc_now()
        self._events.append_run(
            scope.run_id,
            EventType.RECOVERY_STARTED,
            {
                "run_id": scope.run_id,
                "action": decision.action.value,
                "attempt": decision.next_attempt,
            },
            at=now,
        )
        if (
            decision.exhausted
            or decision.action is RecoveryAction.ABORT
            or run.state is RunState.CLEANING
        ):
            return self._abort_recovery(scope, error_code, exhausted=decision.exhausted)
        applied = self._apply_action(scope, decision.action)
        self._events.append_run(
            scope.run_id,
            EventType.RECOVERY_FINISHED,
            {
                "run_id": scope.run_id,
                "action": decision.action.value,
                "result": "ok" if applied else "failed",
            },
            at=now,
        )
        if not applied:
            return self._abort_recovery(scope, error_code, exhausted=False)
        self._stop_script(scope)
        if decision.action is not RecoveryAction.RESET_ENVIRONMENT:
            self._safe_cleanup(scope.call, run_id=scope.run_id, reason="recovering")
        self._safe_release(scope.run_id, reason="recovering")
        retry_at = now + timedelta(seconds=decision.delay_seconds)
        return self._runs.mark_recovering(
            scope.run_id,
            next_attempt=decision.next_attempt,
            retry_at=retry_at,
            error_code=error_code,
        )

    def _abort_recovery(self, scope: RunScope, error_code: str, *, exhausted: bool) -> JobRunRecord:
        """停手：停脚本、清理、释放租约，把运行记成 failed。"""
        self._stop_script(scope)
        self._safe_cleanup(scope.call, run_id=scope.run_id, reason="recovery")
        self._safe_release(scope.run_id, reason="recovery")
        current = self._runs.get(scope.run_id)
        if current.state in (RunState.EXECUTING, RunState.VERIFYING):
            current = self._runs.transition(scope.run_id, RunState.CLEANING, reason="recovery")
        result = "exhausted" if exhausted else "aborted"
        code = "E_RECOVERY_EXHAUSTED" if exhausted else error_code
        finished = self._runs.transition(
            scope.run_id,
            RunState.FAILED,
            reason=result,
            error_code=code,
        )
        self._events.append_run(
            scope.run_id,
            EventType.RECOVERY_FINISHED,
            {
                "run_id": scope.run_id,
                "action": RecoveryAction.ABORT.value,
                "result": result,
            },
            at=self._clock.utc_now(),
        )
        return finished

    def _apply_action(self, scope: RunScope, action: RecoveryAction) -> bool:
        """执行恢复动作；插件异常只算动作失败，由调用方转停手。"""
        try:
            if action is RecoveryAction.RESET_ENVIRONMENT:
                return self._environments.reset(scope.call).ok
            if action is RecoveryAction.RESTART_SCRIPT:
                if scope.handle is None:
                    return True
                return scope.script.recover(scope.handle, scope.context, action).ok
            return True
        except Exception:
            return False

    def _cancel_run(self, scope: RunScope, *, started: bool) -> JobRunRecord:
        """取消运行；已开始的先停脚本与清环境，再记 cancelled。"""
        if started:
            self._stop_script(scope)
            self._safe_cleanup(scope.call, run_id=scope.run_id, reason="cancelled")
        self._safe_release(scope.run_id, reason="cancelled")
        current = self._runs.get(scope.run_id)
        if current.state in (RunState.EXECUTING, RunState.VERIFYING):
            current = self._runs.transition(scope.run_id, RunState.CLEANING, reason="cancelled")
        return self._runs.transition(scope.run_id, RunState.CANCELLED, reason="cancelled")

    def _fail(self, scope: RunScope, error_code: str, reason: str) -> JobRunRecord:
        """收尾失败：停脚本、清理、释放租约，把运行记成 failed。"""
        self._stop_script(scope)
        self._safe_cleanup(scope.call, run_id=scope.run_id, reason=reason)
        self._safe_release(scope.run_id, reason=reason)
        current = self._runs.get(scope.run_id)
        if current.state in (RunState.EXECUTING, RunState.VERIFYING):
            current = self._runs.transition(scope.run_id, RunState.CLEANING, reason=reason)
        return self._runs.transition(
            scope.run_id,
            RunState.FAILED,
            reason=reason,
            error_code=error_code,
        )

    def _stop_script(self, scope: RunScope) -> None:
        """停止脚本并清空句柄；失败只记 WARN，收尾不能因为插件拒绝而卡住。"""
        handle = scope.handle
        if handle is None:
            return
        scope.handle = None
        try:
            scope.script.stop(handle, scope.context, StopMode.FORCE)
        except Exception as error:
            self._log.warn(
                {
                    "event": "executor.script_stop_failed",
                    "run_id": scope.run_id,
                    "error": str(error),
                }
            )

    def _safe_cleanup(self, call: EnvironmentCall, *, run_id: str, reason: str) -> None:
        """清理环境；失败只记 WARN，让已经确定的结局继续写入。"""
        try:
            self._environments.cleanup(call, keep_warm=False, warm_minutes=0)
        except Exception as error:
            self._log.warn(
                {
                    "event": "executor.cleanup_failed",
                    "run_id": run_id,
                    "reason": reason,
                    "error": str(error),
                }
            )

    def _safe_release(self, run_id: str, *, reason: str) -> None:
        """释放租约；失败只记 WARN，过期租约由回收扫描兜底。"""
        try:
            self._leases.release(run_id, reason=reason)
        except Exception as error:
            self._log.warn(
                {
                    "event": "executor.lease_release_failed",
                    "run_id": run_id,
                    "reason": reason,
                    "error": str(error),
                }
            )

    def _build_context(self, run: JobRunRecord, template: JobTemplateRow) -> RunContext:
        work_dir = self._data_root / "work" / run.id
        evidence_dir = self._data_root / "evidence" / run.id
        work_dir.mkdir(parents=True, exist_ok=True)
        evidence_dir.mkdir(parents=True, exist_ok=True)
        now = self._clock.utc_now()
        deadline = run.deadline_at if run.deadline_at is not None else now + _DEFAULT_DEADLINE
        return RunContext(
            run_id=run.id,
            attempt=run.attempt,
            template_id=template.id,
            plugin_id=template.plugin_id,
            environment_id=template.environment_id,
            config=dict(template.config),
            work_dir=work_dir,
            evidence_dir=evidence_dir,
            deadline_at=deadline,
            cancel=self._cancel,
            clock=self._clock,
            secrets={},
            log=self._log,
            emit=self._emitter(run.id),
            record_evidence=self._evidence_recorder(run.id, evidence_dir),
        )

    def _emitter(self, run_id: str) -> Emit:
        def emit(event: str, payload: Mapping[str, Any]) -> None:
            self._events.append_run(
                run_id,
                EventType(event),
                dict(payload),
                at=self._clock.utc_now(),
            )

        return emit

    def _evidence_recorder(self, run_id: str, evidence_dir: Path) -> RecordEvidence:
        def record(kind: str, path: Path, summary: str) -> str:
            target = path if path.is_absolute() else evidence_dir / path
            return self._evidence.record(
                run_id,
                kind=EvidenceKind(kind),
                path=target,
                summary=summary,
            )

        return record


__all__ = ["CancelFlag", "RunScope", "WorkerExecutor"]


def _retry_at(detail: Mapping[str, Any]) -> datetime | None:
    """从运行摘要里读出恢复重试时刻。"""
    value = detail.get("retry_at")
    if not isinstance(value, str):
        return None
    moment = datetime.fromisoformat(value)
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)
