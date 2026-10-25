"""MAA 环境能力（docs/plugins/maa.md 环境节，EnvironmentProvider 协议见 docs/05）。

不连接 MuMu、adb 与游戏进程：``probe`` 一律报告状态不明，四个动作一律返回
``E_PLUGIN`` 失败且无副作用；全部结果确定、可重复。
"""

from __future__ import annotations

from contracts import (
    EnvironmentProbe,
    EnvironmentRef,
    EnvironmentState,
    PhaseResult,
    RunContext,
)

_ACTION_UNAVAILABLE = "环境动作不可用：未执行真机操作"


class MaaEnvironment:
    """environment 能力：探测状态不明，动作失败且不触碰真机。"""

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        """报告环境状态不明；不连接模拟器。"""
        return EnvironmentProbe(
            state=EnvironmentState.UNKNOWN,
            detail="环境探测不可用：状态不明",
            observed_at=ctx.clock.utc_now(),
        )

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        """返回准备失败；不启动模拟器与游戏。"""
        return _unavailable(ctx)

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        """返回复位失败；不断开也不重连 adb。"""
        return _unavailable(ctx)

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        """返回清理失败；不关闭游戏与模拟器。"""
        return _unavailable(ctx)

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        """返回保温失败；不维持任何环境状态。"""
        return _unavailable(ctx)


def _unavailable(ctx: RunContext) -> PhaseResult:
    """构造带时间戳的失败结果；起点与终点同刻，表示未发生任何等待。"""
    now = ctx.clock.utc_now()
    return PhaseResult(
        ok=False,
        error_code="E_PLUGIN",
        detail=_ACTION_UNAVAILABLE,
        started_at=now,
        ended_at=now,
    )
