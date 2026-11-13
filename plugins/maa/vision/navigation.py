"""主界面导航控制器：星型拓扑的有界返回循环与逐步证据。

循环与预算见 docs/arknights/03「导航」：判为主界面即成功；覆盖层、战斗中、
截不到帧与预算耗尽（默认 3 次返回）即停手。每次按返回前后各判定一次，不
连续盲按；逐步证据只进返回值，不写文件、不发事件。
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from plugins.maa.vision.frames import Frame, FrameSource

DEFAULT_BACK_BUDGET = 3
WAIT_RANGE_SECONDS = (0.4, 0.8)


class NavPageState(StrEnum):
    """导航判读的页面状态：主界面、已知覆盖层、其它页面或无法归类。"""

    HOME = "home"
    OVERLAY = "overlay"
    OTHER = "other"


class StopReason(StrEnum):
    """停手原因码：除到家之外所有停手出口的标识。"""

    BUDGET_EXHAUSTED = "budget_exhausted"
    OVERLAY = "overlay"
    IN_BATTLE = "in_battle"
    NO_FRAME = "no_frame"


@dataclass(frozen=True)
class ScreenReading:
    """一次导航判读：页面状态、总分与外观命中数，由判定端口产出。"""

    state: NavPageState
    total_score: int
    appearance_hits: int


class ScreenJudge(Protocol):
    """判定端口：输入一帧，输出一次导航判读。"""

    def judge(self, frame: Frame) -> ScreenReading:
        """判定一帧。"""


class BackSender(Protocol):
    """返回键端口：发送一次系统返回键。"""

    def send_back(self) -> None:
        """发送一次返回键。"""


class BattleGate(Protocol):
    """战斗状态端口：回答当前是否处于战斗；战斗中不发送返回键。"""

    def in_battle(self) -> bool:
        """当前是否处于战斗。"""


class NoBattleGate:
    """默认战斗端口：未接战斗判定时恒答不在战斗。"""

    def in_battle(self) -> bool:
        """恒答不在战斗。"""
        return False


@dataclass(frozen=True)
class NavigationPorts:
    """导航四端口：帧来源、判定器、返回键与战斗门。"""

    frames: FrameSource
    judge: ScreenJudge
    back: BackSender
    battle: BattleGate = field(default_factory=NoBattleGate)


@dataclass(frozen=True)
class NavigationStep:
    """逐步证据：采样序号、当次判读与当步处置（是否按键、停手原因码）。"""

    sample: int
    reading: ScreenReading
    pressed_back: bool = False
    reason: StopReason | None = None


@dataclass(frozen=True)
class NavigationResult:
    """导航结果：是否到达主界面（等价于原因码为空）与逐步证据。"""

    reached_home: bool
    reason: StopReason | None
    steps: tuple[NavigationStep, ...]


def default_wait() -> None:
    """生产等待：0.4–0.8 秒随机，给界面转场留出时间。"""
    time.sleep(random.uniform(*WAIT_RANGE_SECONDS))


def _read(ports: NavigationPorts) -> ScreenReading | None:
    """采样一帧并判定；截帧失败返回 None，不抛异常。"""
    frame = ports.frames.capture()
    return None if frame is None else ports.judge.judge(frame)


def _finish(steps: list[NavigationStep], reason: StopReason | None) -> NavigationResult:
    """收尾：原因码为空当且仅当到家；证据冻结为元组。"""
    return NavigationResult(reached_home=reason is None, reason=reason, steps=tuple(steps))


def _conclude(
    steps: list[NavigationStep], reading: ScreenReading, reason: StopReason | None
) -> NavigationResult:
    """记一条终局证据并收尾：原因码为空即到家。"""
    steps.append(NavigationStep(len(steps) + 1, reading, reason=reason))
    return _finish(steps, reason)


def _after_press(steps: list[NavigationStep], ports: NavigationPorts) -> NavigationResult | None:
    """按返回后的即时判定：终局（到家、覆盖层、无帧）给出结果，其余返回 None 继续循环。"""
    post = _read(ports)
    if post is None:
        return _finish(steps, StopReason.NO_FRAME)
    post_reason = StopReason.OVERLAY if post.state is NavPageState.OVERLAY else None
    steps.append(NavigationStep(len(steps) + 1, post, reason=post_reason))
    if post.state is NavPageState.HOME:
        return _finish(steps, None)
    if post.state is NavPageState.OVERLAY:
        return _finish(steps, StopReason.OVERLAY)
    return None


def ensure_home(
    ports: NavigationPorts,
    *,
    limit: int = DEFAULT_BACK_BUDGET,
    wait: Callable[[], None] = default_wait,
) -> NavigationResult:
    """有界返回导航到主界面，返回结果与逐步证据。

    每轮采样判定：``HOME`` 即成功；``OVERLAY``、战斗中或截帧失败立即停手；
    ``OTHER`` 在预算内按一次返回，按后即时再判定一次，仍非主界面才等待
    （时长经 ``wait`` 注入）并回到采样。预算用尽仍非主界面即停手，不再按键。
    """
    steps: list[NavigationStep] = []
    presses = 0
    while True:
        reading = _read(ports)
        if reading is None:
            return _finish(steps, StopReason.NO_FRAME)
        if reading.state is NavPageState.HOME:
            return _conclude(steps, reading, None)
        if reading.state is NavPageState.OVERLAY:
            return _conclude(steps, reading, StopReason.OVERLAY)
        # 战斗门先于预算：预算未尽也不向战斗画面发送返回键。
        if ports.battle.in_battle():
            return _conclude(steps, reading, StopReason.IN_BATTLE)
        if presses >= limit:
            return _conclude(steps, reading, StopReason.BUDGET_EXHAUSTED)
        ports.back.send_back()
        presses += 1
        steps.append(NavigationStep(len(steps) + 1, reading, pressed_back=True))
        # 按后即时判定先于等待：返回键弹出的覆盖层（如退出确认框）在等待前就能被识别。
        result = _after_press(steps, ports)
        if result is not None:
            return result
        wait()
