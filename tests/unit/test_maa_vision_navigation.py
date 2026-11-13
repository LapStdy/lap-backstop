"""导航控制器 ensure_home 的行为：到家、有界返回、覆盖层与战斗停手、证据与注入等待。

全部用假帧源、假判定器、假返回键与假战斗门，不触真机、不写文件、不发事件。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

import numpy as np

from plugins.maa.vision.frames import Frame, FrameOrigin
from plugins.maa.vision.navigation import (
    DEFAULT_BACK_BUDGET,
    NavigationPorts,
    NavPageState,
    NoBattleGate,
    ScreenReading,
    StopReason,
    ensure_home,
)

NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)

_FRAME = Frame(
    image=np.zeros((720, 1280, 3), dtype=np.uint8),
    width=1280,
    height=720,
    captured_at=NOW,
    sha256="fake-digest",
    source=FrameOrigin.ADB,
    png=b"fake-png-bytes",
)
_HOME = ScreenReading(state=NavPageState.HOME, total_score=11, appearance_hits=4)
_OTHER = ScreenReading(state=NavPageState.OTHER, total_score=0, appearance_hits=0)
_OVERLAY = ScreenReading(state=NavPageState.OVERLAY, total_score=0, appearance_hits=0)


class _ScriptedFrames:
    """假帧来源：按脚本逐帧返回（None 表示截帧失败），记录截帧次数。"""

    def __init__(self, frames: Sequence[Frame | None]) -> None:
        self._frames = list(frames)
        self.calls = 0

    def capture(self) -> Frame | None:
        assert self.calls < len(self._frames), "截帧次数超出脚本长度"
        frame = self._frames[self.calls]
        self.calls += 1
        return frame


class _ScriptedJudge:
    """假判定器：按脚本逐次给出判读，记录判定次数。"""

    def __init__(self, readings: Sequence[ScreenReading]) -> None:
        self._readings = list(readings)
        self.calls = 0

    def judge(self, frame: Frame) -> ScreenReading:
        assert self.calls < len(self._readings), "判定次数超出脚本长度"
        reading = self._readings[self.calls]
        self.calls += 1
        return reading


class _RecordingBack:
    """假返回键：只记录发送次数。"""

    def __init__(self) -> None:
        self.calls = 0

    def send_back(self) -> None:
        self.calls += 1


class _AlwaysInBattle:
    """假战斗门：恒答在战斗中。"""

    def in_battle(self) -> bool:
        return True


class _RecordingWait:
    """注入的零等待：不睡眠，只记录调用次数。"""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1


def test_home_on_first_frame() -> None:
    """现状 2：第一帧判 HOME 即成功：返回 0 次、不等待、证据 1 条。"""
    frames = _ScriptedFrames([_FRAME])
    judge = _ScriptedJudge([_HOME])
    back = _RecordingBack()
    wait = _RecordingWait()
    result = ensure_home(NavigationPorts(frames=frames, judge=judge, back=back), wait=wait)
    assert result.reached_home is True and result.reason is None
    assert back.calls == 0 and wait.calls == 0
    assert len(result.steps) == 1
    assert result.steps[0].sample == 1
    assert result.steps[0].reading.state is NavPageState.HOME


def test_other_then_home_returns_once() -> None:
    """现状 2：OTHER→HOME：按 1 次返回、证据 3 条（按前、按后、复核各判定一次）、成功。"""
    frames = _ScriptedFrames([_FRAME] * 3)
    judge = _ScriptedJudge([_OTHER, _OTHER, _HOME])
    back = _RecordingBack()
    wait = _RecordingWait()
    result = ensure_home(NavigationPorts(frames=frames, judge=judge, back=back), wait=wait)
    assert result.reached_home is True and result.reason is None
    assert back.calls == 1 and wait.calls == 1
    assert [step.sample for step in result.steps] == [1, 2, 3]
    assert [step.reading.state for step in result.steps] == [
        NavPageState.OTHER,
        NavPageState.OTHER,
        NavPageState.HOME,
    ]
    assert [step.pressed_back for step in result.steps] == [True, False, False]


def test_budget_exhausted_stops() -> None:
    """现状 2、3：连续 OTHER 恰好按 3 次返回后停手；第 4 次不再按键，原因码为预算耗尽。"""
    frames = _ScriptedFrames([_FRAME] * 7)
    judge = _ScriptedJudge([_OTHER] * 7)
    back = _RecordingBack()
    wait = _RecordingWait()
    result = ensure_home(
        NavigationPorts(frames=frames, judge=judge, back=back),
        limit=DEFAULT_BACK_BUDGET,
        wait=wait,
    )
    assert result.reached_home is False and result.reason is StopReason.BUDGET_EXHAUSTED
    assert back.calls == 3 and wait.calls == 3
    assert judge.calls == 7 and frames.calls == 7
    assert [step.pressed_back for step in result.steps] == [True, False] * 3 + [False]
    last = result.steps[-1]
    assert last.pressed_back is False and last.reason is StopReason.BUDGET_EXHAUSTED


def test_overlay_stops_without_pressing_back() -> None:
    """现状 4：判为 OVERLAY 立即停手：按键 0 次、不再第二次判定、原因码为覆盖层。"""
    frames = _ScriptedFrames([_FRAME])
    judge = _ScriptedJudge([_OVERLAY])
    back = _RecordingBack()
    result = ensure_home(NavigationPorts(frames=frames, judge=judge, back=back))
    assert result.reached_home is False and result.reason is StopReason.OVERLAY
    assert back.calls == 0
    assert judge.calls == 1 and len(result.steps) == 1
    assert result.steps[0].reason is StopReason.OVERLAY


def test_battle_blocks_back_key() -> None:
    """现状 1、4：战斗门为真不发送返回键并停手；默认战斗门恒答不在战斗。"""
    frames = _ScriptedFrames([_FRAME])
    judge = _ScriptedJudge([_OTHER])
    back = _RecordingBack()
    battle = _AlwaysInBattle()
    result = ensure_home(NavigationPorts(frames=frames, judge=judge, back=back, battle=battle))
    assert result.reached_home is False and result.reason is StopReason.IN_BATTLE
    assert back.calls == 0
    assert NoBattleGate().in_battle() is False


def test_evidence_records_every_sample() -> None:
    """现状 5：每条证据含采样序号/结论/总分/外观命中/是否按键/原因码；条数等于判定次数。"""
    frames = _ScriptedFrames([_FRAME] * 3)
    judge = _ScriptedJudge([ScreenReading(NavPageState.OTHER, 2, 1), _OTHER, _HOME])
    back = _RecordingBack()
    wait = _RecordingWait()
    result = ensure_home(NavigationPorts(frames=frames, judge=judge, back=back), wait=wait)
    assert judge.calls == 3 and len(result.steps) == 3
    assert [step.sample for step in result.steps] == [1, 2, 3]
    assert [step.reading.total_score for step in result.steps] == [2, 0, 11]
    assert [step.reading.appearance_hits for step in result.steps] == [1, 0, 4]
    assert [step.reading.state for step in result.steps] == [
        NavPageState.OTHER,
        NavPageState.OTHER,
        NavPageState.HOME,
    ]
    assert [step.pressed_back for step in result.steps] == [True, False, False]
    assert [step.reason for step in result.steps] == [None, None, None]


def test_injected_wait_is_used() -> None:
    """现状 7：注入零等待时循环仍按序推进；等待调用次数等于返回次数。"""
    frames = _ScriptedFrames([_FRAME] * 5)
    judge = _ScriptedJudge([_OTHER, _OTHER, _OTHER, _OTHER, _HOME])
    back = _RecordingBack()
    wait = _RecordingWait()
    result = ensure_home(
        NavigationPorts(frames=frames, judge=judge, back=back),
        limit=DEFAULT_BACK_BUDGET,
        wait=wait,
    )
    assert result.reached_home is True
    assert back.calls == 2 and wait.calls == 2
    assert judge.calls == 5 and frames.calls == 5


def test_post_press_overlay_stops_without_waiting() -> None:
    """现状 4：按返回后的即时判定判为 OVERLAY：停手、不再等待、原因码为覆盖层。"""
    frames = _ScriptedFrames([_FRAME] * 2)
    judge = _ScriptedJudge([_OTHER, _OVERLAY])
    back = _RecordingBack()
    wait = _RecordingWait()
    result = ensure_home(NavigationPorts(frames=frames, judge=judge, back=back), wait=wait)
    assert result.reached_home is False and result.reason is StopReason.OVERLAY
    assert back.calls == 1 and wait.calls == 0
    assert [step.pressed_back for step in result.steps] == [True, False]
    assert result.steps[1].reason is StopReason.OVERLAY


def test_frame_loss_stops_with_no_frame_reason() -> None:
    """现状 4：按返回后截帧失败：停手、不再判定、原因码为无帧；已按的返回留在证据里。"""
    frames = _ScriptedFrames([_FRAME, None])
    judge = _ScriptedJudge([_OTHER])
    back = _RecordingBack()
    result = ensure_home(NavigationPorts(frames=frames, judge=judge, back=back))
    assert result.reached_home is False and result.reason is StopReason.NO_FRAME
    assert back.calls == 1 and judge.calls == 1
    assert [step.pressed_back for step in result.steps] == [True]
