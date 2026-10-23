"""内核时钟：生产用系统时钟，测试用可推进的虚拟时钟。

调度、超时、退避、租约与看门狗都只经由 Clock 取时间（docs/02 的时钟一节）。
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta


class SystemClock:
    """生产时钟：单调读用于超时，UTC 读用于调度与记账。"""

    def monotonic(self) -> float:
        """单调秒数，不受墙钟调整影响。"""
        return time.monotonic()

    def utc_now(self) -> datetime:
        """当前 UTC 时刻，带时区。"""
        return datetime.now(UTC)


class VirtualClock:
    """测试时钟：由调用方推进，单调值不会倒退。

    墙钟可以单独跳变（``jump_to``），用来覆盖休眠恢复与校时；单调值只按差值前进。
    """

    def __init__(self, start: datetime) -> None:
        """用确定的 UTC 起点创建虚拟时钟。

        Raises:
            ValueError: 起点是 naive datetime。
        """
        self._utc = _require_aware(start, "start")
        self._elapsed = 0.0

    def monotonic(self) -> float:
        """自创建以来经过的秒数。"""
        return self._elapsed

    def utc_now(self) -> datetime:
        """当前虚拟 UTC 时刻。"""
        return self._utc

    def advance(self, delta: timedelta) -> None:
        """把虚拟时间向前推进。

        Raises:
            ValueError: 时长为负。
        """
        seconds = delta.total_seconds()
        if seconds < 0:
            raise ValueError("虚拟时钟只能向前推进")
        self._elapsed += seconds
        self._utc += delta

    def jump_to(self, moment: datetime) -> None:
        """把墙钟直接跳到目标时刻，单调值按两者差值前进。

        Raises:
            ValueError: 目标是 naive 时刻，或早于当前虚拟时刻。
        """
        target = _require_aware(moment, "moment")
        delta = target - self._utc
        if delta.total_seconds() < 0:
            raise ValueError("虚拟时钟的墙钟不能倒退")
        self._elapsed += delta.total_seconds()
        self._utc = target


def _require_aware(moment: datetime, name: str) -> datetime:
    if moment.tzinfo is None:
        raise ValueError(f"{name} 必须是带时区的 UTC 时刻")
    return moment.astimezone(UTC)
