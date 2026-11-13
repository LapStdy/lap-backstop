"""主界面判定测试共享的夹具装载与独立复算工具。

夹具（``tests/fixtures/maa_gate/``）只存锚点覆盖区裁片与 manifest；判定
输入一律按 manifest 把裁片贴回 1280×720 黑画布重建。反证类断言（不抑制
会怎样）在这里独立复算，不复用判定器的内部状态。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import cv2
import numpy as np

from plugins.maa.vision.anchors import ANCHORS, APPEARANCE_PAD, Anchor
from plugins.maa.vision.judge import HomeJudge, suppress_badges

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "maa_gate"
TEMPLATES = Path(__file__).resolve().parent.parent / "plugins" / "maa" / "templates"
NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)


def load_manifest() -> dict:
    """读冻结样本 manifest；字段完整性由用例自行核对。"""
    text = (FIXTURES / "manifest.json").read_text(encoding="utf-8")
    return json.loads(text)


def frame_entry(manifest: dict, name: str) -> dict:
    """按帧名取一条 manifest 记录；缺失或重复即失败。"""
    entries = [entry for entry in manifest["frames"] if entry["name"] == name]
    assert len(entries) == 1, f"manifest 中帧名缺失或重复：{name}"
    return entries[0]


def load_frame_png(path: Path) -> np.ndarray:
    """UTF-8 路径安全的 PNG 读取（夹具帧目录名含中文）。"""
    data = np.fromfile(str(path), dtype=np.uint8)
    image = cv2.imdecode(data, cv2.IMREAD_COLOR)
    assert image is not None, f"夹具不是 PNG：{path}"
    return image


def coverage_rect(box: tuple[int, int, int, int]) -> tuple[int, int, int, int]:
    """锚点框外扩 ``APPEARANCE_PAD`` 并裁到帧边界的覆盖区（x, y, w, h）。"""
    x, y, w, h = box
    x0 = max(0, x - APPEARANCE_PAD)
    y0 = max(0, y - APPEARANCE_PAD)
    x1 = min(1280, x + w + APPEARANCE_PAD)
    y1 = min(720, y + h + APPEARANCE_PAD)
    return x0, y0, x1 - x0, y1 - y0


def rebuild_canvas(entry: dict) -> np.ndarray:
    """按 manifest 把该帧的覆盖区裁片贴回 1280×720 黑画布。"""
    canvas = np.zeros((720, 1280, 3), np.uint8)
    for region in entry["regions"].values():
        crop = load_frame_png(FIXTURES / str(region["file"]))
        x, y = int(region["x"]), int(region["y"])
        w, h = int(region["w"]), int(region["h"])
        canvas[y : y + h, x : x + w] = crop
    return canvas


def make_judge() -> HomeJudge:
    """默认模板目录（plugins/maa/templates/）的判定器。"""
    return HomeJudge()


def _search_window(frame: np.ndarray, anchor: Anchor) -> np.ndarray:
    """与判定器同口径的外观搜索窗口（外扩并裁到帧边界）。"""
    x, y, w, h = anchor.box
    x0 = max(0, x - APPEARANCE_PAD)
    y0 = max(0, y - APPEARANCE_PAD)
    x1 = min(1280, x + w + APPEARANCE_PAD)
    y1 = min(720, y + h + APPEARANCE_PAD)
    return frame[y0:y1, x0:x1]


def _variant_scores(frame: np.ndarray, anchor: Anchor, suppress: bool) -> list[float]:
    """逐变体计算外观得分；``suppress`` 为真时窗口与模板都先做角标抑制。"""
    window = _search_window(frame, anchor)
    scores = []
    for file in anchor.templates:
        template = load_frame_png(TEMPLATES / file)
        if window.shape[0] < template.shape[0] or window.shape[1] < template.shape[1]:
            continue
        view = suppress_badges(window) if suppress else window
        model = suppress_badges(template) if suppress else template
        match = cv2.matchTemplate(view, model, cv2.TM_CCOEFF_NORMED)
        scores.append(float(cv2.minMaxLoc(match)[1]))
    return scores


def raw_appearance_score(frame: np.ndarray, anchor: Anchor) -> float:
    """不做抑制的外观最佳得分（反证：抑制去掉后会怎样）。"""
    return max(_variant_scores(frame, anchor, suppress=False), default=0.0)


def suppressed_appearance_score(frame: np.ndarray, anchor: Anchor) -> float:
    """两端都抑制的外观最佳得分（对照：判定器实际口径）。"""
    return max(_variant_scores(frame, anchor, suppress=True), default=0.0)


def anchor_by_name(name: str) -> Anchor:
    """按名字取锚点表条目。"""
    for anchor in ANCHORS:
        if anchor.name == name:
            return anchor
    raise AssertionError(f"锚点表没有 {name}")
