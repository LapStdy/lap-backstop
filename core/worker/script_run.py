"""脚本生命周期：启动、观察与停止插件脚本，句柄登记在运行范围上。

观察与停止都不改运行状态与租约：阶段结论仍由执行器的阶段编排落库（ADR 0010）。
"""

from __future__ import annotations

from contracts import (
    CancelToken,
    Clock,
    Logger,
    ScriptAdapter,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    StopMode,
)
from core.worker.run_scope import RunScope
from storage.defaults import default

_MAX_OBSERVATIONS = int(default("worker", "max_observations"))


class ScriptRun:
    """一次运行的脚本生命周期；只管脚本本身，不碰运行状态与租约。"""

    def __init__(
        self,
        script: ScriptAdapter,
        clock: Clock,
        cancel: CancelToken,
        log: Logger,
    ) -> None:
        self._script = script
        self._clock = clock
        self._cancel = cancel
        self._log = log

    def launch(self, scope: RunScope) -> ScriptHandle:
        """按模板配置启动脚本，并把句柄记到运行范围上供停止与恢复使用。"""
        handle = self._script.launch(
            ScriptRef(
                id=scope.template.id,
                plugin_id=scope.template.plugin_id,
                config=dict(scope.template.config),
            ),
            scope.context,
        )
        scope.handle = handle
        return handle

    def wait(self, scope: RunScope, handle: ScriptHandle) -> ScriptObservation | None:
        """观察脚本直到结束；观察次数用尽或到截止时间返回 None，由调用方按超时收尾。

        停机不提前退出：当前阶段要跑完写清阶段结论，阶段行不留半途（docs/08）；
        取消仍立即返回，取消优先于停机。
        """
        for _ in range(_MAX_OBSERVATIONS):
            observation = self._script.observe(handle, scope.context)
            if observation.state is not ScriptState.RUNNING:
                return observation
            if self._cancel.is_cancelled():
                return observation
            if self._clock.utc_now() >= scope.context.deadline_at:
                return None
        return None

    def stop(self, scope: RunScope, *, graceful: bool = False) -> None:
        """停止脚本并清空句柄；取消时先 graceful，插件没接受再 force。失败只记 WARN。"""
        handle = scope.handle
        if handle is None:
            return
        scope.handle = None
        if graceful and self._attempt(scope, handle, StopMode.GRACEFUL):
            return
        self._attempt(scope, handle, StopMode.FORCE)

    def _attempt(self, scope: RunScope, handle: ScriptHandle, mode: StopMode) -> bool:
        try:
            result = self._script.stop(handle, scope.context, mode)
        except Exception as error:
            self._log.warn(
                {
                    "event": "executor.script_stop_failed",
                    "run_id": scope.run_id,
                    "mode": mode.value,
                    "error": str(error),
                }
            )
            return False
        if result.ok:
            return True
        self._log.warn(
            {
                "event": "executor.script_stop_failed",
                "run_id": scope.run_id,
                "mode": mode.value,
                "error_code": result.error_code or "",
            }
        )
        return False


__all__ = ["ScriptRun"]
