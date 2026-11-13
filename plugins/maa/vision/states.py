"""连续帧确认：对判定结果流做两档门槛的连续帧计数。

规则见 docs/arknights/03：交接档连续 2 帧达标、导航档连续 3 帧；任一帧
未达本档门槛就把该档计数清零。确认状态只由本对象消费的帧决定，不缓存
跨运行状态：新实例从零开始。
"""

from __future__ import annotations

from dataclasses import dataclass

from plugins.maa.vision.judge import VisionResult

HANDOVER_CONFIRM_FRAMES = 2
NAVIGATION_CONFIRM_FRAMES = 3


@dataclass(frozen=True)
class StreakState:
    """消费一帧后的状态：两档是否确认、当前连续帧数、最近一帧判定。"""

    handover_confirmed: bool
    navigation_confirmed: bool
    handover_streak: int
    navigation_streak: int
    last_result: VisionResult | None


class FrameStreak:
    """两档门槛的连续帧计数器：每判定一帧调用一次 ``observe``，``reset`` 清空。"""

    def __init__(self) -> None:
        self._handover = 0
        self._navigation = 0
        self._last: VisionResult | None = None

    def observe(self, result: VisionResult) -> StreakState:
        """消费一帧判定结果：达标的档位计数加一，未达标的清零。"""
        self._handover = self._handover + 1 if result.handover_hit else 0
        self._navigation = self._navigation + 1 if result.navigation_hit else 0
        self._last = result
        return self._snapshot()

    def reset(self) -> None:
        """清空序列：两档计数归零、最近一帧清空，回到未确认。"""
        self._handover = 0
        self._navigation = 0
        self._last = None

    def _snapshot(self) -> StreakState:
        """当前状态：连续帧数达到本档要求才算确认。"""
        return StreakState(
            handover_confirmed=self._handover >= HANDOVER_CONFIRM_FRAMES,
            navigation_confirmed=self._navigation >= NAVIGATION_CONFIRM_FRAMES,
            handover_streak=self._handover,
            navigation_streak=self._navigation,
            last_result=self._last,
        )
