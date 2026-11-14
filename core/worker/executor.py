"""worker 执行器：把排队运行推过 prepare → execute → verify → cleanup。

阶段编排在核心进程，插件能力调用经 core/plugins 的 worker 子进程通道（ADR 0010）；
调用超时、取消、插件异常与崩溃都转成运行状态，不把异常抛给 supervisor。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import (
    ApplyResult,
    CancelToken,
    Clock,
    ConfigProjection,
    EnvironmentRef,
    EventType,
    EvidenceKind,
    Logger,
    PhaseName,
    PhaseState,
    RecoveryAction,
    RunContext,
    RunState,
    ScriptObservation,
    ScriptState,
    Verdict,
    VerdictStatus,
)
from core.environments.service import EnvironmentCall, EnvironmentService
from core.events.evidence import EvidenceDraft, EvidenceService
from core.events.verdicts import VerdictError, VerdictService
from core.jobs.errors import IllegalRunTransitionError, RunNotFoundError
from core.jobs.phases import PhaseTracker
from core.jobs.run_service import SHUTDOWN_REASON, JobRunService
from core.logging import NullLogger
from core.plugins import rpc
from core.plugins.errors import PluginError, WorkerCancelledError, WorkerTimeoutError
from core.plugins.registry import PluginRegistry
from core.recovery.policy import RecoveryPolicy
from core.resources.service import LeaseService
from core.worker.run_scope import CancelFlag, RunScope
from core.worker.script_run import ScriptRun
from storage.defaults import default
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRepository, JobTemplateRow
from storage.repositories.resources import ResourceRepository, ResourceSpec

_DEFAULT_DEADLINE = timedelta(minutes=int(default("worker", "deadline_minutes")))
_TERMINAL_BY_VERDICT: Mapping[VerdictStatus, RunState] = {
    VerdictStatus.COMPLETED: RunState.SUCCEEDED,
    VerdictStatus.FAILED: RunState.FAILED,
    VerdictStatus.UNKNOWN: RunState.UNKNOWN,
}

Emit = Callable[[str, Mapping[str, Any]], None]
RecordEvidence = Callable[[str, Path, str], str]


@dataclass(frozen=True, slots=True)
class ExecutorOptions:
    """执行器的注入点：数据根、日志出口、保温窗口、取消标记与停机信号。"""

    data_root: Path
    log: Logger | None = None
    keep_warm_minutes: int = 0
    cancel: CancelToken | None = None
    shutdown: Callable[[], bool] | None = None


@dataclass(frozen=True, slots=True)
class ExecutorServices:
    """执行器协作件共享的服务句柄：全部建立在同一条连接与同一时钟上。"""

    runs: JobRunService
    templates: JobTemplateRepository
    phases: PhaseTracker
    environments: EnvironmentService
    resources: ResourceRepository
    leases: LeaseService
    evidence: EvidenceService
    verdicts: VerdictService
    events: EventLog
    clock: Clock
    log: Logger

    @classmethod
    def build(
        cls,
        connection: sqlite3.Connection,
        clock: Clock,
        data_root: Path,
        log: Logger,
    ) -> ExecutorServices:
        """在同一条连接上装配全部服务；证据目录落在数据根下。"""
        return cls(
            runs=JobRunService(connection, clock),
            templates=JobTemplateRepository(connection),
            phases=PhaseTracker(connection, clock),
            environments=EnvironmentService(connection, clock),
            resources=ResourceRepository(connection),
            leases=LeaseService(connection, clock),
            evidence=EvidenceService(connection, data_root / "evidence", clock),
            verdicts=VerdictService(connection, clock),
            events=EventLog(connection),
            clock=clock,
            log=log,
        )


def _never_shutdown() -> bool:
    """未注入停机信号时的缺省信号：永不请求停机。"""
    return False


class WorkerExecutor:
    """一次 JobRun 的四阶段执行。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        registry: PluginRegistry,
        options: ExecutorOptions,
    ) -> None:
        log = NullLogger() if options.log is None else options.log
        cancel = CancelFlag() if options.cancel is None else options.cancel
        shutdown = _never_shutdown if options.shutdown is None else options.shutdown
        services = ExecutorServices.build(connection, clock, options.data_root, log)
        self._registry = registry
        self._data_root = options.data_root
        self._keep_warm_minutes = options.keep_warm_minutes
        self._log = log
        self._cancel: CancelToken = cancel
        self._shutdown: Callable[[], bool] = shutdown
        self._clock = clock
        self._runs = services.runs
        self._templates = services.templates
        self._environments = services.environments
        self._resources = services.resources
        self._leases = services.leases
        self._phases = services.phases
        self._evidence = services.evidence
        self._verdicts = services.verdicts
        self._events = services.events

    def execute(self, run_id: str) -> JobRunRecord:
        """推进一步：恢复等待、准备、执行、验证、清理，直到终态或等待时机。

        Args:
            run_id: 要推进的运行 id。

        Returns:
            推进后的运行记录；未到 scheduled_at 或不在可推进状态时原样返回。

        Raises:
            RunNotFoundError: 运行不存在。
            PluginError: 模板声明的插件或能力没有注册。
            IllegalRunTransitionError: 出队窗口内运行被并发迁出队列且不是叫停抢先。
        """
        run = self._runs.get(run_id)
        if run.state is RunState.RECOVERING:
            # 停机不推进：恢复等待不再入队，留给下次启动扫描按持久状态处置。
            if self._drain_requested():
                return self._drain(run)
            run = self.resume(run_id)
        if run.state not in (RunState.QUEUED, RunState.WAITING):
            return run
        if self._drain_requested():
            return self._drain(run)
        template = self._templates.get(run.template_id)
        if template is None:
            return self._fail_missing_template(run)
        scope = self._begin(run, template)
        if self._cancel.is_cancelled():
            return self._cancel_run(scope, started=False)
        if self._shutdown():
            return self._drain(scope.run)
        if scope.run.state is RunState.QUEUED:
            claimed = self._claim_queued(scope)
            if claimed is not None:
                return claimed
        try:
            return self._advance_or_drain(scope)
        except WorkerCancelledError:
            self._log.info({"event": "executor.worker_cancelled", "run_id": scope.run_id})
            return self._cancel_run(scope, started=True)
        except WorkerTimeoutError as error:
            self._log.warn(
                {"event": "executor.worker_timeout", "run_id": scope.run_id, "error": str(error)}
            )
            return self._recover(scope, "E_DEADLINE", "worker_timeout")
        except PluginError as error:
            self._log.error(
                {
                    "event": "executor.plugin_error",
                    "run_id": scope.run_id,
                    "error_code": error.code,
                    "error": str(error),
                }
            )
            return self._recover(scope, error.code, str(error))
        except Exception as error:  # 插件异常不能拖垮内核：按崩溃收尾
            self._log.error(
                {"event": "executor.error", "run_id": scope.run_id, "error": str(error)}
            )
            return self._recover(scope, "E_PLUGIN_CRASH", str(error))

    def _drain_requested(self) -> bool:
        """停机已请求且用户取消没有同时置位：取消优先于停机（ADR 0007）。"""
        return self._shutdown() and not self._cancel.is_cancelled()

    def _advance(self, scope: RunScope) -> JobRunRecord:
        """按阶段推进；阶段之间是取消与停机的检查点（ADR 0007）。

        取消优先于停机：用户取消仍按既有路径落终态。停机在阶段边界收尾：当前阶段
        先跑完写清阶段结论，再写 run.interrupted，之后不再开新阶段；运行状态与租约
        保持原样，交下次启动扫描收尾（docs/08）。
        """
        for step in (self._prepare, self._run_script, self._verify, self._clean):
            result = step(scope)
            if result is not None:
                return result
            if self._cancel.is_cancelled():
                continue
            if self._shutdown():
                return self._drain(scope.run)
        return self._runs.get(scope.run_id)

    def _advance_or_drain(self, scope: RunScope) -> JobRunRecord:
        """推进一次运行；推进中失败而停机已请求时按停机收尾，不走恢复。

        停机宽限到点被强制终止的调用以失败返回：失败发生在收尾路径上，再走恢复会
        改状态、动租约，还会为收尾重新拉起 worker（docs/08 停机与阶段边界）；停机
        收尾只写 run.interrupted，状态与租约留给下次启动扫描。取消优先于停机：取消
        已置位时异常照旧交回调用方按既有口径收尾。
        """
        try:
            return self._advance(scope)
        except Exception:
            if self._drain_requested():
                return self._drain(scope.run)
            raise

    def _drain(self, run: JobRunRecord) -> JobRunRecord:
        """停机收尾：写一条 run.interrupted，不改状态、不动租约。

        进行中的运行由下次启动扫描置 unknown 并回收孤儿租约，等待中的运行继续持租约、
        重启后接着跑；强杀 worker 与阶段收尾也不在这里做，执行器同步运行，停机后由
        进程退出结束 worker（docs/09 租约、docs/08 停机与阶段边界）。重复信号幂等：
        已有停机收尾事件的运行不再写第二条。
        """
        try:
            if not self._has_shutdown_interrupt(run.id):
                self._events.append_run(
                    run.id,
                    EventType.RUN_INTERRUPTED,
                    {"run_id": run.id, "reason": SHUTDOWN_REASON},
                    at=self._clock.utc_now(),
                )
        except sqlite3.Error as error:
            # 事件写不进时仍要保留停机的状态承诺：不改状态，只留日志给排障。
            self._log.error(
                {"event": "executor.drain_event_failed", "run_id": run.id, "error": str(error)}
            )
        try:
            return self._runs.get(run.id)
        except (RunNotFoundError, sqlite3.Error):
            return run

    def _has_shutdown_interrupt(self, run_id: str) -> bool:
        """该运行是否已有停机收尾的 run.interrupted：与用户取消靠 reason 区分。"""
        return any(
            event.type == EventType.RUN_INTERRUPTED.value
            and event.payload.get("reason") == SHUTDOWN_REASON
            for event in self._events.events_for_run(run_id)
        )

    def _begin(self, run: JobRunRecord, template: JobTemplateRow) -> RunScope:
        """组装一次执行用的插件、环境引用与运行上下文。"""
        provider = self._registry.environment(template.plugin_id)
        script = self._registry.script(template.plugin_id)
        verifier = self._registry.verifier(template.plugin_id)
        environment_id = template.environment_id
        self._resources.ensure(
            environment_id,
            ResourceSpec(resource_type="environment", name=environment_id),
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

    def _claim_queued(self, scope: RunScope) -> JobRunRecord | None:
        """排队运行占租约并迁到 preparing；叫停在出队窗口内抢先时执行器退让。

        出队与叫停分属 tick 与请求线程：QUEUED 读出后运行仍可能被并发取消，迁移
        抛 IllegalRunTransitionError 时只有叫停已落库（运行已 cancelled 且记过取消
        请求）才退让收尾；其余迁出照原样抛出，抛前先释放刚占的租约，不留孤儿。
        """
        self._leases.acquire(scope.run_id, (scope.call.environment_id,))
        try:
            scope.run = self._runs.transition(scope.run.id, RunState.PREPARING)
        except IllegalRunTransitionError:
            current = self._runs.get(scope.run_id)
            if current.state is RunState.CANCELLED and self._runs.cancel_requested(scope.run_id):
                return self._cancel_run(scope, started=False)
            self._safe_release(scope.run_id, reason="dequeue_conflict")
            raise
        return None

    def _prepare(self, scope: RunScope) -> JobRunRecord | None:
        """准备阶段：让插件把环境带到 ready，失败按恢复策略收尾。"""
        if scope.run.state is not RunState.PREPARING:
            return None
        phase = self._phases.start(scope.run.id, PhaseName.PREPARE, attempt=scope.run.attempt)
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
        """执行阶段：到点后先投影下发目标配置，再启动脚本并收口观察结果。"""
        if self._cancel.is_cancelled():
            return self._cancel_run(scope, started=True)
        now = self._clock.utc_now()
        if scope.run.scheduled_at is not None and now < scope.run.scheduled_at:
            return scope.run
        if scope.run.deadline_at is not None and now >= scope.run.deadline_at:
            return self._recover(scope, "E_DEADLINE", "deadline")
        settled = self._project_config(scope)
        if settled is not None:
            return settled
        scope.run = self._runs.transition(scope.run.id, RunState.EXECUTING)
        self._environments.mark_busy(scope.call)
        phase = self._phases.start(scope.run.id, PhaseName.EXECUTE, attempt=scope.run.attempt)
        runner = self._script_lifecycle(scope)
        handle = runner.launch(scope)
        observation = runner.wait(scope, handle)
        if observation is None:
            self._phases.finish(phase, state=PhaseState.TIMED_OUT, detail="deadline")
            self._stop_script(scope)
            # 阶段行已收好：停机在阶段边界排水，不再走恢复（docs/08）。
            if self._drain_requested():
                return self._drain(scope.run)
            return self._recover(scope, "E_DEADLINE", "deadline")
        return self._settle_execute(scope, phase, observation)

    def _project_config(self, scope: RunScope) -> JobRunRecord | None:
        """开跑前按插件配置构造整份目标并下发；返回 None 表示继续开跑。

        没有投影能力的插件或开关关闭时跳过；通道异常与 ok=false 走同一收口，
        不把异常抛给 supervisor。
        """
        projection = self._registry.get(scope.template.plugin_id).config_projection
        if projection is None or _projection_disabled(scope.context.config):
            return None
        try:
            target = projection.desired(scope.context, scope.context.config)
            result = projection.apply(scope.context, target)
        except PluginError as error:  # 通道超时与取消同属一族，错误码随通道走
            return self._projection_failed(scope, projection, error.code, None)
        if result.ok:
            self._emit_config_applied(scope, result)
            return None
        return self._projection_failed(
            scope, projection, result.error_code or "E_CONFIG", _revision_in(result.meta)
        )

    def _projection_failed(
        self,
        scope: RunScope,
        projection: ConfigProjection,
        error_code: str,
        revision: str | None,
    ) -> JobRunRecord:
        """投影失败收口：有合法修订先回滚一次并记事件，再交现有恢复策略处置。

        收口错误码始终取本次 apply 的码：回滚调用再遇通道异常时转成失败回执，
        不让回滚的码覆盖 apply 的码。
        """
        if revision is not None:
            rolled = _rollback_once(projection, scope.context, revision)
            self._log.warn(
                {
                    "event": "executor.config_projection_failed",
                    "run_id": scope.run_id,
                    "error_code": error_code,
                    "revision": revision,
                    "rolled_back": rolled.ok,
                }
            )
            self._events.append_run(
                scope.run_id,
                EventType.CONFIG_ROLLED_BACK,
                {
                    "run_id": scope.run_id,
                    "revision": revision,
                    "ok": rolled.ok,
                    "error_code": rolled.error_code,
                },
                at=self._clock.utc_now(),
            )
        return self._recover(scope, error_code, "config_projection")

    def _emit_config_applied(self, scope: RunScope, result: ApplyResult) -> None:
        """apply 成功记 config.applied；修订与哈希取插件回执，未写盘时为空。"""
        self._events.append_run(
            scope.run_id,
            EventType.CONFIG_APPLIED,
            {
                "run_id": scope.run_id,
                "plugin_id": scope.template.plugin_id,
                "revision": result.meta.get("revision"),
                "sha256": result.meta.get("sha256"),
                "changed": bool(result.meta.get("changed")),
            },
            at=self._clock.utc_now(),
        )

    def _settle_execute(
        self,
        scope: RunScope,
        phase: str,
        observation: ScriptObservation,
    ) -> JobRunRecord | None:
        """按观察结果落执行阶段结论：信号入事件流，失败、超时与取消各自收尾。"""
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
        cancelled = self._cancel.is_cancelled()
        state = PhaseState.FAILED if script_failed else PhaseState.OK
        if hung:
            state = PhaseState.TIMED_OUT
        if cancelled:
            state = PhaseState.FAILED
        self._phases.finish(
            phase,
            state=state,
            detail="cancelled" if cancelled else observation.state.value,
        )
        if cancelled:
            self._stop_script(scope, graceful=True)
            return self._cancel_run(scope, started=True)
        if hung:
            self._stop_script(scope)
        # 阶段行已收好：停机在阶段边界排水，不进恢复与下一阶段（docs/08）。
        if self._drain_requested():
            return self._drain(scope.run)
        if script_failed:
            return self._recover(scope, "E_SCRIPT_TASK_FAILED", "script")
        return None

    def _verify(self, scope: RunScope) -> JobRunRecord | None:
        """验证阶段：插件结论先与证据核对再落库，缺证据留给清理阶段收尾。"""
        scope.run = self._runs.transition(scope.run.id, RunState.VERIFYING)
        phase = self._phases.start(scope.run.id, PhaseName.VERIFY, attempt=scope.run.attempt)
        verdict = scope.verifier.verify(scope.context)
        if not isinstance(verdict, Verdict):
            raise TypeError("验证器返回的不是 Verdict")
        try:
            _, effective = self._verdicts.record(scope.run.id, verdict)
        except VerdictError as error:
            scope.verify_failure = error
        else:
            scope.verdict = effective
        failure = scope.verify_failure
        self._phases.finish(
            phase,
            state=PhaseState.FAILED if failure is not None else PhaseState.OK,
            detail=failure.code if failure is not None else "",
        )
        return None

    def _clean(self, scope: RunScope) -> JobRunRecord:
        """清理阶段：停脚本、清环境、释放租约，再按结论落终态。"""
        scope.run = self._runs.transition(scope.run.id, RunState.CLEANING)
        phase = self._phases.start(scope.run.id, PhaseName.CLEANUP, attempt=scope.run.attempt)
        self._stop_script(scope)
        # provider 清理不是纯操作：同一尝试只调用一次，置位放在调用前，清理抛异常
        # 后收尾路径也不再重复调用（docs/09）。
        scope.has_called_cleanup = True
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
        # docs/09：cleaning 中取消在 cleanup 完成并释放租约后生效；这里直接落终态，
        # 跳过收尾路径的停脚本与租约释放，事件序列与单次收尾保持一致。
        if self._cancel.is_cancelled():
            return self._runs.transition(
                scope.run.id,
                RunState.CANCELLED,
                reason="cancelled",
            )
        if not cleaned.ok:
            return self._fail(scope, cleaned.error_code or "E_ENV_UNREACHABLE", "cleanup")
        failure = scope.verify_failure
        if failure is not None:
            return self._runs.transition(
                scope.run.id,
                RunState.UNKNOWN,
                reason=failure.reason,
                error_code=failure.code,
            )
        target = (
            RunState.UNKNOWN
            if scope.verdict is None
            else _TERMINAL_BY_VERDICT[scope.verdict.status]
        )
        return self._runs.transition(scope.run.id, target, reason="verdict")

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
        self._hop_to_cleaning(run.id, reason="template_missing")
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
            self._safe_cleanup(scope, reason="recovering")
        self._safe_release(scope.run_id, reason="recovering")
        retry_at = now + timedelta(seconds=decision.delay_seconds)
        return self._runs.mark_recovering(
            scope.run_id,
            next_attempt=decision.next_attempt,
            retry_at=retry_at,
            error_code=error_code,
        )

    def _hop_to_cleaning(self, run_id: str, *, reason: str) -> None:
        """执行或验证中的运行落终态前先经过 cleaning（docs/02 迁移表）。"""
        current = self._runs.get(run_id)
        if current.state in (RunState.EXECUTING, RunState.VERIFYING):
            self._runs.transition(run_id, RunState.CLEANING, reason=reason)

    def _abort_recovery(self, scope: RunScope, error_code: str, *, exhausted: bool) -> JobRunRecord:
        """停手：停脚本、按需清理、释放租约，把运行记成 failed。"""
        self._stop_script(scope)
        self._safe_cleanup(scope, reason="recovery")
        self._safe_release(scope.run_id, reason="recovery")
        self._hop_to_cleaning(scope.run_id, reason="recovery")
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
        """取消运行；已开始的先停脚本、按需清理，再记 cancelled。"""
        if started:
            self._stop_script(scope)
            self._safe_cleanup(scope, reason="cancelled")
        self._safe_release(scope.run_id, reason="cancelled")
        self._hop_to_cleaning(scope.run_id, reason="cancelled")
        return self._runs.transition(scope.run_id, RunState.CANCELLED, reason="cancelled")

    def _fail(self, scope: RunScope, error_code: str, reason: str) -> JobRunRecord:
        """收尾失败：停脚本、按需清理、释放租约，把运行记成 failed。"""
        self._stop_script(scope)
        self._safe_cleanup(scope, reason=reason)
        self._safe_release(scope.run_id, reason=reason)
        self._hop_to_cleaning(scope.run_id, reason=reason)
        return self._runs.transition(
            scope.run_id,
            RunState.FAILED,
            reason=reason,
            error_code=error_code,
        )

    def _script_lifecycle(self, scope: RunScope) -> ScriptRun:
        """按运行范围组装脚本生命周期对象：它只持有引用，构造无副作用。"""
        return ScriptRun(scope.script, self._clock, self._cancel, self._log)

    def _stop_script(self, scope: RunScope, *, graceful: bool = False) -> None:
        """停止脚本并清空句柄；取消时先 graceful，插件没接受再 force。失败只记 WARN。"""
        self._script_lifecycle(scope).stop(scope, graceful=graceful)

    def _safe_cleanup(self, scope: RunScope, *, reason: str) -> None:
        """清理环境；本尝试已调用过 provider 清理就跳过，失败只记 WARN。

        provider 清理不是纯操作，重复执行可能影响复用的环境（docs/09）；跳过与
        失败都不改变已经确定的结局。
        """
        if scope.has_called_cleanup:
            return
        try:
            self._environments.cleanup(scope.call, keep_warm=False, warm_minutes=0)
        except Exception as error:
            self._log.warn(
                {
                    "event": "executor.cleanup_failed",
                    "run_id": scope.run_id,
                    "reason": reason,
                    "error": str(error),
                }
            )

    def _safe_release(self, run_id: str, *, reason: str) -> None:
        """释放租约；失败只记 WARN，过期租约由回收扫描兜底。

        重复调用安全：release 只处理仍活跃的租约，二次调用查不到活跃租约、
        不写事件（docs/09）。
        """
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
            # 插件只能写 run.signal（docs/03）：与 worker 通道共用同一白名单校验。
            checked = rpc.plugin_event(event)
            rpc.check_plugin_payload(checked, payload)
            self._events.append_run(
                run_id,
                checked,
                dict(payload),
                at=self._clock.utc_now(),
            )

        return emit

    def _evidence_recorder(self, run_id: str, evidence_dir: Path) -> RecordEvidence:
        def record(kind: str, path: Path, summary: str) -> str:
            target = path if path.is_absolute() else evidence_dir / path
            return self._evidence.record(
                run_id,
                EvidenceDraft(kind=EvidenceKind(kind), path=target, summary=summary),
            )

        return record


__all__ = ["CancelFlag", "ExecutorOptions", "RunScope", "WorkerExecutor"]


def _projection_disabled(config: Mapping[str, Any]) -> bool:
    """插件配置的 config_projection.enabled 为 false 时投影关闭；缺省开启。"""
    settings = config.get("config_projection")
    return isinstance(settings, Mapping) and settings.get("enabled") is False


def _rollback_once(
    projection: ConfigProjection,
    ctx: RunContext,
    revision: str,
) -> ApplyResult:
    """回滚一次；通道异常转成失败回执，收口错误码仍由调用方按 apply 的码定。"""
    try:
        return projection.rollback(ctx, revision)
    except PluginError as error:
        return ApplyResult(ok=False, error_code=error.code, detail=f"回滚调用失败：{error}")


def _revision_in(meta: Mapping[str, Any]) -> str | None:
    """apply 回执里的修订号；非空字符串才算合法，插件私有格式由插件自校。"""
    revision = meta.get("revision")
    return revision if isinstance(revision, str) and revision else None


def _retry_at(detail: Mapping[str, Any]) -> datetime | None:
    """从运行摘要里读出恢复重试时刻。"""
    value = detail.get("retry_at")
    if not isinstance(value, str):
        return None
    moment = datetime.fromisoformat(value)
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)
