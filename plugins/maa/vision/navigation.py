"""主界面导航控制器：星型拓扑的有界返回循环与逐步证据。

循环与预算见 docs/arknights/03「导航」：导航档（总分 ≥ 10 且外观命中 ≥ 4）
连续 3 帧达标即到家；覆盖层、战斗中、截不到帧与预算耗尽（默认 3 次返回）
即停手。每次按返回前后各判定一次，不连续盲按；逐步证据只进返回值，不写
文件、不发事件。判定与返回键端口的生产实现（``HomeJudgeAdapter``、
``AdbBackSender``）也在本模块。
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from plugins.maa.vision.frames import (
    DEFAULT_ADDRESS,
    Frame,
    FrameSource,
    ProcessRunner,
    subprocess_runner,
)
from plugins.maa.vision.judge import HomeJudge, evaluate_gates
from plugins.maa.vision.login_vision import LoginVision
from plugins.maa.vision.readiness import LoginStage
from plugins.maa.vision.states import FrameStreak

DEFAULT_BACK_BUDGET = 3
WAIT_RANGE_SECONDS = (0.4, 0.8)
_BACK_TIMEOUT_SECONDS = 30.0


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


class AdbBackSender:
    """``BackSender`` 的 adb 实现：发送系统返回键（keyevent 4），失败抛 ``OSError``。

    执行器可注入，测试不真的按键；``None`` 返回与非零退出码都算失败，
    与 ``AdbClicker`` 同口径。
    """

    def __init__(
        self,
        adb_path: Path = Path("adb"),
        address: str = DEFAULT_ADDRESS,
        runner: ProcessRunner = subprocess_runner,
    ) -> None:
        self._adb_path = adb_path
        self._address = address
        self._runner = runner

    def send_back(self) -> None:
        """发送一次返回键；执行失败抛 ``OSError``。"""
        command = [
            str(self._adb_path),
            "-s",
            self._address,
            "shell",
            "input",
            "keyevent",
            "4",
        ]
        completed = self._runner(command, _BACK_TIMEOUT_SECONDS)
        if completed is None or completed.returncode != 0:
            code = None if completed is None else completed.returncode
            raise OSError(f"adb 返回键失败：退出码 {code}")


class HomeJudgeAdapter:
    """``ScreenJudge`` 端口的生产实现：主界面判定结果适配成导航判读。

    导航档（总分 ≥ 10 且外观命中 ≥ 4）连续 3 帧达标才判 ``home``，未满连续帧
    与只达交接档（总分 8–9 或外观 2–3）的帧都判 ``other``，总分与外观命中数
    原样带出，不改门槛、不另算分；``error_code`` 非空（如分辨率不符）判
    ``other`` 并清零连续帧。覆盖层先经 ``LoginVision`` 判读：目前只有每日签到
    有对应模板，公告、限时活动等其余覆盖层没有模板、落不到覆盖层判读，一律
    按 ``other`` 处理——返回键导航不处置覆盖层，只停手。覆盖层判读没有主界面
    计分，总分与外观命中记 0。
    """

    def __init__(self, home: HomeJudge, overlay: LoginVision | None = None) -> None:
        self._home = home
        self._overlay = overlay
        self._streak = FrameStreak()

    def judge(self, frame: Frame) -> ScreenReading:
        """判定一帧：覆盖层优先，再按导航档连帧确认给 ``home`` 或 ``other``。"""
        if self._overlay is not None:
            reading = self._overlay.judge(frame)
            if reading.stage is LoginStage.OVERLAY:
                return ScreenReading(state=NavPageState.OVERLAY, total_score=0, appearance_hits=0)
        result = self._home.judge(frame.image, frame.captured_at)
        confirmed = self._streak.observe(result).navigation_confirmed
        state = NavPageState.HOME if confirmed else NavPageState.OTHER
        return ScreenReading(
            state=state, total_score=result.total_score, appearance_hits=result.appearance_hits
        )


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


def _is_navigation_hit(reading: ScreenReading) -> bool:
    """该帧是否达导航档分数门槛：达标帧不按返回，只继续采样等连续帧确认。"""
    _, navigation = evaluate_gates(reading.total_score, reading.appearance_hits)
    return navigation


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

    每轮采样判定：``HOME``（导航档连续 3 帧达标）即成功；``OVERLAY``、战斗中
    或截帧失败立即停手；已达导航档分数但连续帧未满时不按返回，继续采样确认；
    其余 ``OTHER`` 在预算内按一次返回，按后即时再判定一次，仍非主界面才等待
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
        # 达导航档但连续帧未满：主界面按返回会弹退出确认框，达标帧不按键，继续采样确认。
        if _is_navigation_hit(reading):
            steps.append(NavigationStep(len(steps) + 1, reading))
            continue
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
