"""主界面判定器的单元行为：明细契约、分辨率错误、角标抑制、门槛与性能预算。"""

from __future__ import annotations

import time
from pathlib import Path

import cv2
import numpy as np
import pytest

from contracts import ErrorCode
from plugins.maa.vision.anchors import ANCHORS, APPEARANCE_THRESHOLD
from plugins.maa.vision.judge import INSTALL_HINT, HomeJudge, evaluate_gates, suppress_badges
from tests.maa_vision_support import (
    NOW,
    TEMPLATES,
    anchor_by_name,
    coverage_rect,
    frame_entry,
    load_frame_png,
    load_manifest,
    make_judge,
    raw_appearance_score,
    rebuild_canvas,
    suppressed_appearance_score,
)

# 合成角标补丁：贴在日历框内的主体格子上。实测该帧抑制后得分 0.90（命中
# 侧余量 0.05）、不抑制 0.79（不命中侧余量 0.06），两侧都留有裕度。
PATCH_XYWH = (255, 40, 14, 10)
PATCH_BGR = (0, 130, 255)


def _patched_canvas(name: str) -> np.ndarray:
    """重建一帧夹具画布并把高饱和补丁贴进日历框，模拟更大的未读角标。"""
    frame = rebuild_canvas(frame_entry(load_manifest(), name))
    x, y, w, h = PATCH_XYWH
    frame[y : y + h, x : x + w] = PATCH_BGR
    return frame


def test_result_carries_anchor_details_and_total() -> None:
    """正例：明细含五个锚点且总分等于明细之和；反例：输入帧不被修改。"""
    frame = rebuild_canvas(frame_entry(load_manifest(), "011"))
    before = frame.tobytes()
    result = make_judge().judge(frame, NOW)
    assert frame.tobytes() == before
    assert result.error_code is None
    assert result.page_state.value == "home"
    assert {detail.name for detail in result.anchors} == {anchor.name for anchor in ANCHORS}
    assert sum(detail.points for detail in result.anchors) == result.total_score
    gear = next(detail for detail in result.anchors if detail.name == "gear")
    assert gear.channels == ("appearance", "position")
    assert gear.points == 3
    redeem = next(detail for detail in result.anchors if detail.name == "redeem")
    assert redeem.channels == ("appearance",)
    assert redeem.position_score is None and not redeem.position_hit


def test_wrong_size_returns_resolution_error() -> None:
    """反例：1920×1080 与 1280×721 都返回分辨率错误码；正例：1280×720 正常判定。"""
    judge = make_judge()
    for height, width in ((1080, 1920), (721, 1280)):
        result = judge.judge(np.zeros((height, width, 3), np.uint8), NOW)
        assert result.error_code == ErrorCode.ENV_RESOLUTION
        assert result.page_state.value == "other"
        assert result.anchors == () and result.total_score == 0
        assert not result.handover_hit and not result.navigation_hit
    normal = judge.judge(rebuild_canvas(frame_entry(load_manifest(), "011")), NOW)
    assert normal.error_code is None
    assert len(normal.anchors) == 5 and normal.page_state.value == "home"


def test_badge_suppression_changes_match(monkeypatch: pytest.MonkeyPatch) -> None:
    """正例：贴高饱和角标后抑制路径仍命中；反例：不抑制则日历外观不命中。"""
    frame = _patched_canvas("011")
    calendar = anchor_by_name("calendar")
    result = make_judge().judge(frame, NOW)
    detail = next(item for item in result.anchors if item.name == "calendar")
    assert detail.appearance_hit and detail.appearance_score >= APPEARANCE_THRESHOLD
    assert raw_appearance_score(frame, calendar) < APPEARANCE_THRESHOLD
    x, y, w, h = PATCH_XYWH
    region = frame[y : y + h, x : x + w]
    assert (suppress_badges(frame)[y : y + h, x : x + w] == 128).all()
    assert not np.array_equal(suppress_badges(region), region)

    def _identity(image: np.ndarray) -> np.ndarray:
        """替身：把抑制换成原样返回，模拟「没有抑制」的判定路径。"""
        return image

    monkeypatch.setattr("plugins.maa.vision.judge.suppress_badges", _identity)
    without = make_judge().judge(frame, NOW)
    without_calendar = next(item for item in without.anchors if item.name == "calendar")
    assert not without_calendar.appearance_hit


def test_redeem_keeps_color_and_is_not_suppressed() -> None:
    """redeem 不参与抑制：判定得分与原始复算逐位一致，且抑制本会改它的像素。"""
    frame = rebuild_canvas(frame_entry(load_manifest(), "011"))
    redeem = anchor_by_name("redeem")
    result = make_judge().judge(frame, NOW)
    detail = next(item for item in result.anchors if item.name == "redeem")
    assert detail.appearance_hit
    assert detail.appearance_score == raw_appearance_score(frame, redeem)
    x, y, w, h = coverage_rect(redeem.box)
    window = frame[y : y + h, x : x + w]
    assert not np.array_equal(suppress_badges(window), window)
    gap = abs(suppressed_appearance_score(frame, redeem) - detail.appearance_score)
    assert gap < 0.01


def test_two_gate_levels_on_same_details() -> None:
    """同一份计分同时给出交接档与导航档；两侧分界各取一例。"""
    cases = [
        (7, 2, False, False),
        (8, 1, False, False),
        (8, 2, True, False),
        (9, 2, True, False),
        (9, 4, True, False),
        (10, 3, True, False),
        (10, 4, True, True),
        (7, 4, False, False),
    ]
    for total, hits, handover, navigation in cases:
        assert evaluate_gates(total, hits) == (handover, navigation), (total, hits)
    result = make_judge().judge(rebuild_canvas(frame_entry(load_manifest(), "011")), NOW)
    assert result.handover_hit and result.navigation_hit


def test_frame_judge_within_budget() -> None:
    """宽松性能阈值：单帧 ≤ 100 毫秒、38 帧判定循环 ≤ 30 秒（实测进回执）。"""
    manifest = load_manifest()
    judge = make_judge()
    judge.judge(rebuild_canvas(manifest["frames"][0]), NOW)  # 首帧 OpenCV 预热，不计时
    durations = []
    start = time.perf_counter()
    for entry in manifest["frames"]:
        begin = time.perf_counter()
        judge.judge(rebuild_canvas(entry), NOW)
        durations.append(time.perf_counter() - begin)
    elapsed = time.perf_counter() - start
    assert max(durations) <= 0.1, f"单帧超预算：{max(durations) * 1000:.1f}ms"
    assert elapsed <= 30.0, f"38 帧超预算：{elapsed:.1f}s"


def test_offset_limit_boundary() -> None:
    """合成帧上偏移 16 像素命中、17 像素不命中：外观偏移上限的分界。"""
    calendar = anchor_by_name("calendar")
    gray = suppress_badges(load_frame_png(TEMPLATES / "calendar.png"))
    template = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
    judge = make_judge()
    for dx, dy, hit in ((16, 0, True), (17, 0, False), (0, 17, False), (0, 16, True)):
        frame = np.zeros((720, 1280, 3), np.uint8)
        x, y = calendar.box[0] + dx, calendar.box[1] + dy
        frame[y : y + template.shape[0], x : x + template.shape[1]] = template
        result = judge.judge(frame, NOW)
        detail = next(item for item in result.anchors if item.name == "calendar")
        assert detail.appearance_hit is hit, (dx, dy, detail)


def test_missing_template_fails_loudly(tmp_path: Path) -> None:
    """模板文件缺失或不是 PNG 时，构造判定器即报错，不静默降级。"""
    with pytest.raises(FileNotFoundError, match="模板"):
        HomeJudge(tmp_path)
    (tmp_path / "gear.png").write_bytes(b"not png")
    with pytest.raises(FileNotFoundError, match="模板"):
        HomeJudge(tmp_path)


def test_install_hint_constant() -> None:
    """缺依赖提示固定指向可选 extra 的安装命令。"""
    assert INSTALL_HINT == "pip install -e .[maa-vision]"
