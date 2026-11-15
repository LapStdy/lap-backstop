"""登录流程状态机用例：合成帧驱动，验证转移、覆盖层处置与失败收口。"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np

from plugins.maa.vision.frames import Frame, FrameOrigin
from plugins.maa.vision.readiness import (
    ACTIVITY_TAP,
    CLOSE_TAP,
    SIGNIN_CLOSE_TAP,
    START_TAP,
    WAKE_TAP,
    LoginPorts,
    LoginReading,
    LoginStage,
    OverlayKind,
    drive_login,
)


def _frame(sha: str) -> Frame:
    return Frame(
        image=np.zeros((720, 1280, 3), dtype=np.uint8),
        width=1280,
        height=720,
        captured_at=datetime.now(UTC),
        sha256=sha,
        source=FrameOrigin.ADB,
        png=b"",
    )


class _Frames:
    """有限帧源：count 帧之后返回 None，模拟截帧失败。"""

    def __init__(self, count: int = 100) -> None:
        self._left = count

    def capture(self) -> Frame | None:
        if self._left <= 0:
            return None
        self._left -= 1
        return _frame("ab" * 32)


class _Judge:
    """按序返回判读，序列用完后重复最后一条。"""

    def __init__(self, readings: list[LoginReading]) -> None:
        self._readings = readings
        self._index = 0

    def judge(self, frame: Frame) -> LoginReading:
        reading = self._readings[min(self._index, len(self._readings) - 1)]
        self._index += 1
        return reading


class _Clicker:
    """记录每次点击。"""

    def __init__(self) -> None:
        self.taps: list[tuple[int, int]] = []

    def tap(self, x: int, y: int) -> None:
        self.taps.append((x, y))


class _Clock:
    """每次读表前进固定步长，测试里用大步长快速触发超时。"""

    def __init__(self, step: float = 0.5) -> None:
        self._now = 0.0
        self._step = step

    def __call__(self) -> float:
        self._now += self._step
        return self._now


def _ports(readings: list[LoginReading], *, frames: int = 100, step: float = 0.5):
    clicker = _Clicker()
    ports = LoginPorts(
        frames=_Frames(frames),
        judge=_Judge(readings),
        clicker=clicker,
        clock=_Clock(step),
        wait=lambda _seconds: None,
    )
    return ports, clicker


def _ready() -> LoginReading:
    return LoginReading(LoginStage.START)


def _login() -> LoginReading:
    return LoginReading(LoginStage.LOGIN)


def _home() -> LoginReading:
    return LoginReading(LoginStage.HOME)


def test_normal_chain_reaches_home() -> None:
    readings = [
        LoginReading(LoginStage.UNKNOWN),
        _ready(),
        _login(),
        LoginReading(LoginStage.CONNECTING),
        LoginReading(LoginStage.LOADING),
        _home(),
    ]
    ports, clicker = _ports(readings)
    result = drive_login(ports)
    assert result.reached_home is True
    assert clicker.taps == [START_TAP, WAKE_TAP]
    assert result.steps[-1].stage.value == "home"


def test_announcement_overlay_closed() -> None:
    readings = [
        _ready(),
        _login(),
        LoginReading(LoginStage.OVERLAY, OverlayKind.ANNOUNCEMENT),
        _home(),
    ]
    ports, clicker = _ports(readings)
    result = drive_login(ports)
    assert result.reached_home is True
    assert CLOSE_TAP in clicker.taps


def test_activity_overlay_entered() -> None:
    readings = [_ready(), _login(), LoginReading(LoginStage.OVERLAY, OverlayKind.ACTIVITY), _home()]
    ports, clicker = _ports(readings)
    result = drive_login(ports)
    assert result.reached_home is True
    assert ACTIVITY_TAP in clicker.taps


def test_daily_signin_overlay_closed_to_home() -> None:
    readings = [
        _ready(),
        _login(),
        LoginReading(LoginStage.OVERLAY, OverlayKind.DAILY_SIGNIN),
        _home(),
    ]
    ports, clicker = _ports(readings)
    result = drive_login(ports)
    assert result.reached_home is True
    assert SIGNIN_CLOSE_TAP in clicker.taps


def test_unknown_overlay_stops() -> None:
    readings = [_ready(), _login(), LoginReading(LoginStage.OVERLAY, OverlayKind.UNKNOWN)]
    ports, clicker = _ports(readings)
    result = drive_login(ports)
    assert result.reached_home is False
    assert "未知覆盖层" in result.detail
    assert clicker.taps == [START_TAP, WAKE_TAP]


def test_stage_timeout_fails() -> None:
    ports, _clicker = _ports([LoginReading(LoginStage.UNKNOWN)], step=10.0)
    result = drive_login(ports)
    assert result.reached_home is False
    assert "阶段超时" in result.detail


def test_no_frame_fails_immediately() -> None:
    ports, _clicker = _ports([_ready()], frames=0)
    result = drive_login(ports)
    assert result.reached_home is False
    assert result.detail == "截帧失败"
    assert result.failed_stage is not None
    assert result.failed_stage.value == "launch"


def test_regression_to_earlier_stage_fails() -> None:
    readings = [_ready(), _login(), LoginReading(LoginStage.CONNECTING), _ready()]
    ports, clicker = _ports(readings)
    result = drive_login(ports)
    assert result.reached_home is False
    assert "回退" in result.detail
    assert clicker.taps == [START_TAP, WAKE_TAP]
