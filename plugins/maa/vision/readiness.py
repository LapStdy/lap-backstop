"""登录流程状态机：从游戏启动到主界面，覆盖层在原位处置。

规则见 docs/arknights/07。状态机只通过注入端口读帧、判读、点击；识别不确定
时停手不盲点。状态只前进：回退到更早阶段按未知画面失败。判读、点击与时钟
都可注入，整条链路在合成帧上可测，不依赖真机与模拟器。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from plugins.maa.vision.frames import Frame, FrameSource

LAUNCH_TIMEOUT = 90.0
READY_TIMEOUT = 60.0
LOGIN_TIMEOUT = 60.0
CONNECT_TIMEOUT = 30.0
LOADING_TIMEOUT = 120.0
OVERLAY_TIMEOUT = 60.0
OVERLAY_MAX_ACTIONS = 20
START_TAP = (640, 672)
WAKE_TAP = (640, 512)
CLOSE_TAP = (1190, 100)
ACTIVITY_TAP = (106, 140)


class LoginStage(StrEnum):
    """判读端口给出的登录阶段与覆盖层。"""

    START = "start"
    LOGIN = "login"
    CONNECTING = "connecting"
    LOADING = "loading"
    HOME = "home"
    OVERLAY = "overlay"
    UNKNOWN = "unknown"


class OverlayKind(StrEnum):
    """覆盖层类型：处置按类型分派，认不出即停手。"""

    DAILY_SIGNIN = "daily_signin"
    ACTIVITY = "activity"
    ANNOUNCEMENT = "announcement"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    UPDATE = "update"
    EXIT_CONFIRM = "exit_confirm"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class LoginReading:
    """一次判读：阶段、覆盖层类型与置信度（低于阈值不点）。"""

    stage: LoginStage
    overlay: OverlayKind | None = None
    confidence: float = 1.0


class LoginJudge(Protocol):
    """登录判读端口：输入一帧，输出登录阶段判读。"""

    def judge(self, frame: Frame) -> LoginReading:
        """判读一帧。"""


class Clicker(Protocol):
    """点击端口：点一次安全坐标；越界由实现拒绝。"""

    def tap(self, x: int, y: int) -> None:
        """点一次坐标。"""


class ProgressStage(StrEnum):
    """状态机内部状态：进度只前进不后退。"""

    LAUNCH = "launch"
    READY_SCREEN = "ready_screen"
    LOGIN_SCREEN = "login_screen"
    CONNECTING = "connecting"
    LOADING = "loading"
    OVERLAY = "overlay"
    HOME = "home"
    FAILED = "failed"


@dataclass(frozen=True)
class LoginStep:
    """逐步证据：当步状态、判读、动作与是否点击。"""

    stage: ProgressStage
    reading: LoginReading
    action: str
    tapped: tuple[int, int] | None = None
    reason: str | None = None


@dataclass(frozen=True)
class LoginResult:
    """登录流程结果：是否到家、失败阶段与逐步证据。"""

    reached_home: bool
    failed_stage: ProgressStage | None
    detail: str
    steps: tuple[LoginStep, ...]


@dataclass(frozen=True)
class LoginPorts:
    """登录流程四端口：帧来源、判读、点击与时钟等待。"""

    frames: FrameSource
    judge: LoginJudge
    clicker: Clicker
    clock: Callable[[], float]
    wait: Callable[[float], None]


_TIMEOUTS: dict[ProgressStage, float] = {
    ProgressStage.LAUNCH: LAUNCH_TIMEOUT,
    ProgressStage.READY_SCREEN: READY_TIMEOUT,
    ProgressStage.LOGIN_SCREEN: LOGIN_TIMEOUT,
    ProgressStage.CONNECTING: CONNECT_TIMEOUT,
    ProgressStage.LOADING: LOADING_TIMEOUT,
    ProgressStage.OVERLAY: OVERLAY_TIMEOUT,
}
# 阶段进入顺序：判读到的阶段比当前靠前按未知画面收口。
_ORDER = (
    ProgressStage.LAUNCH,
    ProgressStage.READY_SCREEN,
    ProgressStage.LOGIN_SCREEN,
    ProgressStage.CONNECTING,
    ProgressStage.LOADING,
    ProgressStage.OVERLAY,
    ProgressStage.HOME,
)


def drive_login(ports: LoginPorts, *, max_frames: int = 600) -> LoginResult:
    """跑完整登录流程，返回结果与逐步证据；任何不确定都停在 FAILED。

    每帧截一次、判一次：判读推进到更早阶段按未知画面失败；推进到后一阶段
    即记一条证据；覆盖层先用处置函数处理，认不出类型直接停下不点。
    """
    stage = ProgressStage.LAUNCH
    steps: list[LoginStep] = []
    entered = ports.clock()
    overlay_actions = 0
    for _ in range(max_frames):
        frame = ports.frames.capture()
        if frame is None:
            return _failed(steps, stage, "截帧失败")
        reading = ports.judge.judge(frame)
        if reading.stage is LoginStage.OVERLAY:
            if reading.overlay is None or reading.overlay is OverlayKind.UNKNOWN:
                return _failed(steps, stage, "未知覆盖层，停手不点")
            step = _handle_overlay(reading, ports)
            steps.append(step)
            overlay_actions += 1
            if step.reason is not None:
                return LoginResult(False, ProgressStage.OVERLAY, step.reason, tuple(steps))
            if overlay_actions > OVERLAY_MAX_ACTIONS:
                return _failed(steps, stage, "覆盖层处置次数超限")
            entered = ports.clock()
            continue
        next_stage, action, tapped, reason = _advance(stage, reading, ports)
        if reason is not None:
            steps.append(LoginStep(stage, reading, action, tapped, reason))
            return _failed(steps, stage, reason)
        if next_stage is ProgressStage.HOME:
            steps.append(LoginStep(ProgressStage.HOME, reading, "到达主界面"))
            return LoginResult(True, None, "登录流程完成：已到主界面", tuple(steps))
        if next_stage is not stage:
            steps.append(LoginStep(next_stage, reading, action, tapped))
            stage = next_stage
            entered = ports.clock()
            continue
        if ports.clock() - entered > _TIMEOUTS[stage]:
            return _failed(steps, stage, f"阶段超时：{stage.value}")
        ports.wait(0.3)
    return _failed(steps, stage, "帧数用尽仍未到主界面")


def stage_index(stage: ProgressStage) -> int:
    """阶段在进度序列中的位置；FAILED 与 HOME 之外都在序列内。"""
    return _ORDER.index(stage) if stage in _ORDER else len(_ORDER)


def _advance(
    stage: ProgressStage, reading: LoginReading, ports: LoginPorts
) -> tuple[ProgressStage, str, tuple[int, int] | None, str | None]:
    """按当前阶段与判读决定下一步：目标阶段、动作、点击与失败原因。"""
    target = _stage_of(reading.stage)
    if target is None:
        return stage, "等待可识别画面", None, None
    if reading.stage is LoginStage.HOME:
        return ProgressStage.HOME, "主界面", None, None
    if stage_index(target) < stage_index(stage):
        return stage, "回退到更早画面", None, "回退到更早画面，按未知画面停手"
    if target is ProgressStage.READY_SCREEN and stage is ProgressStage.LAUNCH:
        ports.clicker.tap(*START_TAP)
        return ProgressStage.READY_SCREEN, "点 START", START_TAP, None
    if target is ProgressStage.LOGIN_SCREEN and stage_index(stage) < stage_index(
        ProgressStage.LOGIN_SCREEN
    ):
        ports.clicker.tap(*WAKE_TAP)
        return ProgressStage.LOGIN_SCREEN, "点开始唤醒", WAKE_TAP, None
    return _forward(target, reading)


def _stage_of(login: LoginStage) -> ProgressStage | None:
    """判读阶段 → 状态机阶段；UNKNOWN 返回 None（继续等待）。"""
    return {
        LoginStage.START: ProgressStage.READY_SCREEN,
        LoginStage.LOGIN: ProgressStage.LOGIN_SCREEN,
        LoginStage.CONNECTING: ProgressStage.CONNECTING,
        LoginStage.LOADING: ProgressStage.LOADING,
        LoginStage.OVERLAY: ProgressStage.OVERLAY,
        LoginStage.HOME: ProgressStage.HOME,
    }.get(login)


def _forward(target: ProgressStage, reading: LoginReading) -> tuple[ProgressStage, str, None, None]:
    """直接前进的判读：连接中、加载屏等无动作阶段。"""
    names = {
        ProgressStage.CONNECTING: "连接中",
        ProgressStage.LOADING: "加载屏",
        ProgressStage.LOGIN_SCREEN: "登录界面",
        ProgressStage.READY_SCREEN: "准备画面",
    }
    return target, names.get(target, reading.stage.value), None, None


def _handle_overlay(reading: LoginReading, ports: LoginPorts) -> LoginStep:
    """处置覆盖层：能点的类型点一次安全坐标，其余停手。"""
    kind = reading.overlay
    if kind is OverlayKind.ANNOUNCEMENT:
        ports.clicker.tap(*CLOSE_TAP)
        return LoginStep(ProgressStage.OVERLAY, reading, "点公告关闭", CLOSE_TAP)
    if kind is OverlayKind.ACTIVITY:
        ports.clicker.tap(*ACTIVITY_TAP)
        return LoginStep(ProgressStage.OVERLAY, reading, "点限时活动签到卡", ACTIVITY_TAP)
    if kind is OverlayKind.EXIT_CONFIRM:
        return LoginStep(ProgressStage.OVERLAY, reading, "退出确认，停手不点", reason="退出确认")
    reason = kind.value if kind is not None else "unknown"
    return LoginStep(ProgressStage.OVERLAY, reading, "覆盖层类型未处置，停手", reason=reason)


def _failed(steps: list[LoginStep], stage: ProgressStage, reason: str) -> LoginResult:
    """失败收口：记一条终局证据并返回失败结果。"""
    reading = steps[-1].reading if steps else LoginReading(LoginStage.UNKNOWN)
    steps.append(LoginStep(ProgressStage.FAILED, reading, reason, reason=reason))
    return LoginResult(False, stage, reason, tuple(steps))
