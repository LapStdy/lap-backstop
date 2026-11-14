"""worker 执行器：把排队运行推过 prepare → execute → verify → cleanup。

阶段编排在核心进程，插件能力调用经 core/plugins 的 worker 子进程通道（ADR 0010）；
调用超时、取消、插件异常与崩溃都转成运行状态，不把异常抛给 supervisor。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
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
    RunContext,
    RunState,
    Verdict,
    VerdictStatus,
)
from core.environments.service import EnvironmentCall, EnvironmentService
from core.events.evidence import EvidenceDraft, EvidenceService
from core.events.verdicts import VerdictError, VerdictService
from core.jobs.errors import IllegalRunTransitionError
from core.jobs.phases import PhaseTracker
from core.jobs.run_service import JobRunService
from core.logging import NullLogger
from core.plugins import rpc
from core.plugins.errors import PluginError, WorkerCancelledError, WorkerTimeoutError
from core.plugins.registry import PluginRegistry
from core.resources.service import LeaseService
from core.worker.executor_settlement import RunSettlement
from core.worker.run_scope import CancelFlag, RunScope
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
        self._settlement = RunSettlement(services, cancel, shutdown)

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
            if self._settlement.drain_requested():
                return self._settlement.drain(run)
            run = self._settlement.resume(run_id)
        if run.state not in (RunState.QUEUED, RunState.WAITING):
            return run
        if self._settlement.drain_requested():
            return self._settlement.drain(run)
        template = self._templates.get(run.template_id)
        if template is None:
            return self._settlement.fail_missing_template(run)
        scope = self._begin(run, template)
        if self._cancel.is_cancelled():
            return self._settlement.cancel_run(scope, started=False)
        if self._shutdown():
            return self._settlement.drain(scope.run)
        if scope.run.state is RunState.QUEUED:
            claimed = self._claim_queued(scope)
            if claimed is not None:
                return claimed
        try:
            return self._advance_or_drain(scope)
        except WorkerCancelledError:
            self._log.info({"event": "executor.worker_cancelled", "run_id": scope.run_id})
            return self._settlement.cancel_run(scope, started=True)
        except WorkerTimeoutError as error:
            self._log.warn(
                {"event": "executor.worker_timeout", "run_id": scope.run_id, "error": str(error)}
            )
            return self._settlement.recover(scope, "E_DEADLINE", "worker_timeout")
        except PluginError as error:
            self._log.error(
                {
                    "event": "executor.plugin_error",
                    "run_id": scope.run_id,
                    "error_code": error.code,
                    "error": str(error),
                }
            )
            return self._settlement.recover(scope, error.code, str(error))
        except Exception as error:  # 插件异常不能拖垮内核：按崩溃收尾
            self._log.error(
                {"event": "executor.error", "run_id": scope.run_id, "error": str(error)}
            )
            return self._settlement.recover(scope, "E_PLUGIN_CRASH", str(error))

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
                return self._settlement.drain(scope.run)
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
            if self._settlement.drain_requested():
                return self._settlement.drain(scope.run)
            raise

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
                return self._settlement.cancel_run(scope, started=False)
            self._settlement.safe_release(scope.run_id, reason="dequeue_conflict")
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
            return self._settlement.recover(
                scope, prepared.error_code or "E_ENV_WAKE_TIMEOUT", "prepare"
            )
        scope.run = self._runs.transition(scope.run.id, RunState.WAITING)
        return None

    def _run_script(self, scope: RunScope) -> JobRunRecord | None:
        """执行阶段：到点后先投影下发目标配置，再启动脚本并收口观察结果。"""
        if self._cancel.is_cancelled():
            return self._settlement.cancel_run(scope, started=True)
        now = self._clock.utc_now()
        if scope.run.scheduled_at is not None and now < scope.run.scheduled_at:
            return scope.run
        if scope.run.deadline_at is not None and now >= scope.run.deadline_at:
            return self._settlement.recover(scope, "E_DEADLINE", "deadline")
        settled = self._project_config(scope)
        if settled is not None:
            return settled
        scope.run = self._runs.transition(scope.run.id, RunState.EXECUTING)
        self._environments.mark_busy(scope.call)
        phase = self._phases.start(scope.run.id, PhaseName.EXECUTE, attempt=scope.run.attempt)
        runner = self._settlement.script_runner(scope)
        handle = runner.launch(scope)
        observation = runner.wait(scope, handle)
        if observation is None:
            self._phases.finish(phase, state=PhaseState.TIMED_OUT, detail="deadline")
            self._settlement.stop_script(scope)
            # 阶段行已收好：停机在阶段边界排水，不再走恢复（docs/08）。
            if self._settlement.drain_requested():
                return self._settlement.drain(scope.run)
            return self._settlement.recover(scope, "E_DEADLINE", "deadline")
        return self._settlement.settle_execute(scope, phase, observation)

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
        return self._settlement.recover(scope, error_code, "config_projection")

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
        self._settlement.stop_script(scope)
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
            return self._settlement.fail(
                scope, cleaned.error_code or "E_ENV_UNREACHABLE", "cleanup"
            )
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
