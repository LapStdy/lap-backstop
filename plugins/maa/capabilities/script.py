"""MAA 脚本能力（docs/plugins/maa.md 脚本节，ScriptAdapter 协议见 docs/05）。

不启动 MAA 进程：``launch`` 返回标记未拉起进程的句柄，``observe`` 报告已退出
且无信号，``stop`` 因无进程可停而成功，``recover`` 返回 ``E_PLUGIN`` 失败。
"""

from __future__ import annotations

from contracts import (
    RecoveryAction,
    RecoveryResult,
    RunContext,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    StopMode,
    StopResult,
)

_RECOVERY_UNAVAILABLE = "恢复动作不可用：未执行真机操作"


class MaaScript:
    """script 能力：句柄与观察结果确定，不驱动 MAA 进程。"""

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        """返回未拉起进程的句柄；meta 由本插件解释，内核不解析。"""
        return ScriptHandle(id=f"maa-{ctx.run_id}", meta={"launched": False})

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        """报告脚本已退出且无信号；observe 只读，可重复调用。"""
        return ScriptObservation(state=ScriptState.EXITED, since=ctx.clock.utc_now())

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult:
        """报告停止成功：句柄未对应进程，无可停止对象。"""
        return StopResult(ok=True, detail="无进程可停止")

    def recover(
        self,
        handle: ScriptHandle,
        ctx: RunContext,
        action: RecoveryAction,
    ) -> RecoveryResult:
        """返回恢复失败；不执行任何内核动作的分解步骤。"""
        return RecoveryResult(ok=False, error_code="E_PLUGIN", detail=_RECOVERY_UNAVAILABLE)
