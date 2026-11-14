"""worker 执行器的结局收口：停机排水、脚本停止与安全清理释放。

执行器只做阶段编排与检查点；结局动作（写 run.interrupted、停脚本、按需清理、
释放租约）归本模块。收口动作失败只记日志，不改已经确定的结局（docs/08 停机
与阶段边界、docs/09 尝试与恢复）。
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from contracts import CancelToken, EventType
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import SHUTDOWN_REASON
from core.worker.run_scope import RunScope
from core.worker.script_run import ScriptRun
from storage.repositories.job_runs import JobRunRecord

if TYPE_CHECKING:
    from collections.abc import Callable

    from core.worker.executor import ExecutorServices


class RunSettlement:
    """运行结局的收口：停机排水、脚本停止、按需清理与租约释放。"""

    def __init__(
        self,
        services: ExecutorServices,
        cancel: CancelToken,
        shutdown: Callable[[], bool],
    ) -> None:
        self._runs = services.runs
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


__all__ = ["RunSettlement"]
