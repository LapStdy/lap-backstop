"""连续帧确认的行为：两档门槛的正反例、计数归零、reset 与性能预算。"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from plugins.maa.vision.judge import PageState, VisionResult
from plugins.maa.vision.states import FrameStreak

NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)


def _result(
    total_score: int,
    appearance_hits: int,
    handover_hit: bool,
    navigation_hit: bool,
) -> VisionResult:
    """构造一帧判定结果：分数与两档结论由用例显式给定，与判定器口径一致。"""
    return VisionResult(
        page_state=PageState.HOME if handover_hit else PageState.OTHER,
        total_score=total_score,
        appearance_hits=appearance_hits,
        handover_hit=handover_hit,
        navigation_hit=navigation_hit,
        anchors=(),
        error_code=None,
        captured_at=NOW,
    )


def test_handover_requires_two_consecutive_frames() -> None:
    """交接档：连续 1 帧未确认、2 帧确认；中间不达标一帧计数归零重数。"""
    streak = FrameStreak()
    first = streak.observe(_result(8, 2, True, False))
    assert not first.handover_confirmed and first.handover_streak == 1
    second = streak.observe(_result(9, 3, True, False))
    assert second.handover_confirmed and second.handover_streak == 2
    assert second.last_result is not None and second.last_result.total_score == 9
    broken = streak.observe(_result(3, 1, False, False))
    assert not broken.handover_confirmed and broken.handover_streak == 0
    restarted = streak.observe(_result(8, 2, True, False))
    assert not restarted.handover_confirmed and restarted.handover_streak == 1


def test_navigation_requires_three_consecutive_frames() -> None:
    """导航档：连续 2 帧未确认、3 帧确认；同流上两档门槛切换各有正反例。"""
    streak = FrameStreak()
    first = streak.observe(_result(10, 4, True, True))
    second = streak.observe(_result(10, 4, True, True))
    third = streak.observe(_result(11, 5, True, True))
    assert not first.handover_confirmed and not first.navigation_confirmed
    assert second.handover_confirmed and not second.navigation_confirmed
    assert third.navigation_confirmed and third.navigation_streak == 3
    mixed = streak.observe(_result(9, 2, True, False))
    assert not mixed.navigation_confirmed and mixed.navigation_streak == 0
    assert mixed.handover_streak == 4 and mixed.handover_confirmed
    reconfirmed = [streak.observe(_result(10, 4, True, True)) for _ in range(3)]
    assert not reconfirmed[1].navigation_confirmed and reconfirmed[2].navigation_confirmed


def test_reset_clears_sequence() -> None:
    """reset() 后计数归零、不再确认；新实例不携带任何历史。"""
    streak = FrameStreak()
    for _ in range(3):
        streak.observe(_result(10, 4, True, True))
    streak.reset()
    observed = streak.observe(_result(10, 4, True, True))
    assert observed.handover_streak == 1 and observed.navigation_streak == 1
    assert not observed.handover_confirmed and not observed.navigation_confirmed
    fresh = FrameStreak().observe(_result(10, 4, True, True))
    assert fresh.handover_streak == 1 and not fresh.handover_confirmed


def test_process_within_budget() -> None:
    """宽松阈值：单帧连续计数 ≤ 20 毫秒；实测数字进回执，不放宽断言。"""
    streak = FrameStreak()
    result = _result(10, 4, True, True)
    durations = []
    for _ in range(1000):
        begin = time.perf_counter()
        streak.observe(result)
        durations.append(time.perf_counter() - begin)
    assert max(durations) <= 0.02, f"单帧超预算：{max(durations) * 1000:.3f}ms"
