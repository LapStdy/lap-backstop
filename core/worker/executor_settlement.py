"""worker 执行器的结局收口：恢复决策、取消、失败与停机排水的落库。

执行器只做阶段编排与检查点；结局动作（恢复决策、取消、失败终态、写
run.interrupted、停脚本、按需清理、释放租约）归本模块。收口动作失败只记日志，
不改已经确定的结局（docs/08 停机与阶段边界、docs/09 尝试与恢复）。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from contracts import (
    CancelToken,
    EventType,
    PhaseState,
    RecoveryAction,
    RunState,
    ScriptObservation,
    ScriptState,
)
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import SHUTDOWN_REASON
from core.recovery.policy import RecoveryPolicy
from core.worker.run_scope import RunScope
from core.worker.script_run import ScriptRun
from storage.repositories.job_runs import JobRunRecord

if TYPE_CHECKING:
    from collections.abc import Callable

    from core.worker.executor import ExecutorServices


class RunSettlement:
    """运行结局的收口：恢复决策、取消、失败终态、停机排水与收尾机械。"""

    def __init__(
        self,
        services: ExecutorServices,
        cancel: CancelToken,
        shutdown: Callable[[], bool],
    ) -> None:
        self._runs = services.runs
        self._phases = services.phases
        self._environments = services.environments
        self._leases = services.leases
        self._events = services.events
        self._clock = services.clock
        self._log = services.log
        self._cancel = cancel
        self._shutdown = shutdown

    def drain_requested(self) -> bool:
        """停机已请求且用户取消没有同时置位：取消优先于停机（ADR 0007）。"""
        return self._shutdown() and not self._cancel.is_cancelled()

    def drain(self, run: JobRunRecord) -> JobRunRecord:
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

    def script_runner(self, scope: RunScope) -> ScriptRun:
        """按运行范围组装脚本生命周期对象：它只持有引用，构造无副作用。"""
        return ScriptRun(scope.script, self._clock, self._cancel, self._log)

    def stop_script(self, scope: RunScope, *, graceful: bool = False) -> None:
        """停止脚本并清空句柄；取消时先 graceful，插件没接受再 force。失败只记 WARN。"""
        self.script_runner(scope).stop(scope, graceful=graceful)

    def safe_cleanup(self, scope: RunScope, *, reason: str) -> None:
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

    def safe_release(self, run_id: str, *, reason: str) -> None:
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

    def fail_missing_template(self, run: JobRunRecord) -> JobRunRecord:
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

    def recover(self, scope: RunScope, error_code: str, reason: str) -> JobRunRecord:
        """按模板策略决定恢复动作；退避等待由 resume 按 retry_at 控制。"""
        try:
            policy = RecoveryPolicy.from_mapping(scope.template.recovery_policy)
        except ValueError:
            return self.fail(scope, "E_CONFIG", reason)
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
        self.stop_script(scope)
        if decision.action is not RecoveryAction.RESET_ENVIRONMENT:
            self.safe_cleanup(scope, reason="recovering")
        self.safe_release(scope.run_id, reason="recovering")
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
        self.stop_script(scope)
        self.safe_cleanup(scope, reason="recovery")
        self.safe_release(scope.run_id, reason="recovery")
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

    def cancel_run(self, scope: RunScope, *, started: bool) -> JobRunRecord:
        """取消运行；已开始的先停脚本、按需清理，再记 cancelled。"""
        if started:
            self.stop_script(scope)
            self.safe_cleanup(scope, reason="cancelled")
        self.safe_release(scope.run_id, reason="cancelled")
        self._hop_to_cleaning(scope.run_id, reason="cancelled")
        return self._runs.transition(scope.run_id, RunState.CANCELLED, reason="cancelled")

    def fail(self, scope: RunScope, error_code: str, reason: str) -> JobRunRecord:
        """收尾失败：停脚本、按需清理、释放租约，把运行记成 failed。"""
        self.stop_script(scope)
        self.safe_cleanup(scope, reason=reason)
        self.safe_release(scope.run_id, reason=reason)
        self._hop_to_cleaning(scope.run_id, reason=reason)
        return self._runs.transition(
            scope.run_id,
            RunState.FAILED,
            reason=reason,
            error_code=error_code,
        )

    def settle_execute(
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
            self.stop_script(scope, graceful=True)
            return self.cancel_run(scope, started=True)
        if hung:
            self.stop_script(scope)
        # 阶段行已收好：停机在阶段边界排水，不进恢复与下一阶段（docs/08）。
        if self.drain_requested():
            return self.drain(scope.run)
        if script_failed:
            return self.recover(scope, "E_SCRIPT_TASK_FAILED", "script")
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


__all__ = ["RunSettlement"]


def _retry_at(detail: Mapping[str, Any]) -> datetime | None:
    """从运行摘要里读出恢复重试时刻。"""
    value = detail.get("retry_at")
    if not isinstance(value, str):
        return None
    moment = datetime.fromisoformat(value)
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)
