"""主界面判定器：外观与位置两通道打分、门槛合成与逐锚点明细。

判定规则与参数见 docs/arknights/03、04。判定器只读帧数组：不点击、不按键、
不启停程序，也不修改传入的帧。cv2 与 numpy 懒加载，缺依赖时构造判定器抛
``RuntimeError`` 并提示安装命令，插件的其余模块照常加载。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING

from contracts import ErrorCode
from plugins.maa.vision.anchors import (
    ANCHORS,
    APPEARANCE_OFFSET_LIMIT,
    APPEARANCE_PAD,
    APPEARANCE_POINTS,
    APPEARANCE_THRESHOLD,
    BADGE_GRAY,
    BADGE_S_LIMIT,
    BADGE_V_LIMIT,
    FRAME_HEIGHT,
    FRAME_SHAPE,
    FRAME_WIDTH,
    HANDOVER_MIN_APPEARANCE,
    HANDOVER_MIN_TOTAL,
    NAVIGATION_MIN_APPEARANCE,
    NAVIGATION_MIN_TOTAL,
    POSITION_DIFF_LIMIT,
    POSITION_PAD,
    POSITION_POINTS,
    Anchor,
)

if TYPE_CHECKING:
    import numpy as np

INSTALL_HINT = "pip install -e .[maa-vision]"
DEFAULT_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"


class PageState(StrEnum):
    """页面状态：本判定器只区分主界面与非主界面，覆盖层归后续模块。"""

    HOME = "home"
    OTHER = "other"


@dataclass(frozen=True)
class AnchorDetail:
    """单锚点计分明细：两通道得分、命中与通道清单。"""

    name: str
    channels: tuple[str, ...]
    appearance_score: float
    appearance_hit: bool
    offset: tuple[int, int]
    position_score: float | None
    position_hit: bool
    points: int


@dataclass(frozen=True)
class VisionResult:
    """一次判定：页面状态、逐锚点明细、总分与两档门槛结论。

    ``error_code`` 非空时环境不满足判定条件（如分辨率不符），此时 ``anchors``
    为空、总分为 0、两档门槛都不命中，调用方据此判环境失败。
    """

    page_state: PageState
    total_score: int
    appearance_hits: int
    handover_hit: bool
    navigation_hit: bool
    anchors: tuple[AnchorDetail, ...]
    error_code: ErrorCode | None
    captured_at: datetime


def evaluate_gates(total_score: int, appearance_hits: int) -> tuple[bool, bool]:
    """同一份计分同时给两档门槛，返回（交接档，导航档）。"""
    handover = total_score >= HANDOVER_MIN_TOTAL and appearance_hits >= HANDOVER_MIN_APPEARANCE
    navigation = (
        total_score >= NAVIGATION_MIN_TOTAL and appearance_hits >= NAVIGATION_MIN_APPEARANCE
    )
    return handover, navigation


def _vision_modules() -> tuple[ModuleType, ModuleType]:
    """懒加载 cv2 与 numpy；缺依赖抛 RuntimeError 并带安装命令。"""
    try:
        import cv2
        import numpy
    except ImportError as error:
        raise RuntimeError(f"缺少视觉依赖，请先安装：{INSTALL_HINT}") from error
    return cv2, numpy


def suppress_badges(bgr: np.ndarray) -> np.ndarray:
    """角标抑制：S>110 且 V>130 的像素置为灰度 ``BADGE_GRAY``，返回单通道灰度图。

    输入数组不被修改；模板与搜索窗口都经本函数处理后再匹配，橙黄角标
    只剩形状与明暗，颜色不参与得分。
    """
    cv2, _ = _vision_modules()
    hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    gray[(hsv[:, :, 1] > BADGE_S_LIMIT) & (hsv[:, :, 2] > BADGE_V_LIMIT)] = BADGE_GRAY
    return gray


def _window_bounds(box: tuple[int, int, int, int], pad: int) -> tuple[int, int, int, int]:
    """锚点框四周外扩 ``pad`` 像素并裁到帧边界，返回 (x0, y0, x1, y1)。"""
    x, y, w, h = box
    return (
        max(0, x - pad),
        max(0, y - pad),
        min(FRAME_WIDTH, x + w + pad),
        min(FRAME_HEIGHT, y + h + pad),
    )


class HomeJudge:
    """主界面判定器：输入一帧 1280×720 BGR 图像与时间戳，输出 ``VisionResult``。"""

    def __init__(self, template_dir: Path | None = None) -> None:
        self._cv2, self._np = _vision_modules()
        self._template_dir = DEFAULT_TEMPLATE_DIR if template_dir is None else template_dir
        self._templates: dict[str, tuple[np.ndarray, ...]] = {
            anchor.name: tuple(self._load_template(anchor, file) for file in anchor.templates)
            for anchor in ANCHORS
        }

    def judge(self, frame: np.ndarray, captured_at: datetime) -> VisionResult:
        """判定一帧；不缩放、不裁剪，分辨率不符时返回 ``E_ENV_RESOLUTION``。"""
        if frame.shape != FRAME_SHAPE:
            return VisionResult(
                page_state=PageState.OTHER,
                total_score=0,
                appearance_hits=0,
                handover_hit=False,
                navigation_hit=False,
                anchors=(),
                error_code=ErrorCode.ENV_RESOLUTION,
                captured_at=captured_at,
            )
        details = tuple(self._score_anchor(frame, anchor) for anchor in ANCHORS)
        total = sum(detail.points for detail in details)
        hits = sum(1 for detail in details if detail.appearance_hit)
        handover, navigation = evaluate_gates(total, hits)
        return VisionResult(
            page_state=PageState.HOME if handover else PageState.OTHER,
            total_score=total,
            appearance_hits=hits,
            handover_hit=handover,
            navigation_hit=navigation,
            anchors=details,
            error_code=None,
            captured_at=captured_at,
        )

    def _load_template(self, anchor: Anchor, file: str) -> np.ndarray:
        """读一张模板裁片；参与抑制的锚点在载入时先做角标抑制。"""
        path = self._template_dir / file
        try:
            data = self._np.fromfile(str(path), dtype=self._np.uint8)
        except OSError as error:
            raise FileNotFoundError(f"模板缺失：{path}") from error
        raw = self._cv2.imdecode(data, self._cv2.IMREAD_COLOR)
        if raw is None:
            raise FileNotFoundError(f"模板不是 PNG：{path}")
        max_w, max_h = anchor.box[2] + 2 * APPEARANCE_PAD, anchor.box[3] + 2 * APPEARANCE_PAD
        if raw.shape[1] > max_w or raw.shape[0] > max_h:
            raise ValueError(f"模板超出搜索窗口：{path} {raw.shape[1]}x{raw.shape[0]}")
        return suppress_badges(raw) if anchor.badge_suppression else raw

    def _score_anchor(self, frame: np.ndarray, anchor: Anchor) -> AnchorDetail:
        """单锚点两通道计分：外观命中 2 分，位置命中 1 分。"""
        score, dx, dy = self._appearance(frame, anchor)
        appearance_hit = (
            score >= APPEARANCE_THRESHOLD and max(abs(dx), abs(dy)) <= APPEARANCE_OFFSET_LIMIT
        )
        channels = ["appearance"]
        position_score: float | None = None
        position_hit = False
        if anchor.position_threshold is not None:
            channels.append("position")
            position_score = self._position_ratio(frame, anchor)
            position_hit = position_score > anchor.position_threshold
        points = APPEARANCE_POINTS * appearance_hit + POSITION_POINTS * position_hit
        return AnchorDetail(
            name=anchor.name,
            channels=tuple(channels),
            appearance_score=score,
            appearance_hit=appearance_hit,
            offset=(dx, dy),
            position_score=position_score,
            position_hit=position_hit,
            points=points,
        )

    def _appearance(self, frame: np.ndarray, anchor: Anchor) -> tuple[float, int, int]:
        """外观通道：锚点框外扩 ``APPEARANCE_PAD`` 的窗口上做模板匹配。

        返回（跨变体最佳得分，匹配位置与锚点框原点的横纵偏移）。
        """
        best, dx, dy = 0.0, 0, 0
        for template in self._templates[anchor.name]:
            window = self._search_window(frame, anchor, template.shape[1], template.shape[0])
            if window is None:
                continue
            roi, x0, y0 = window
            view = suppress_badges(roi) if anchor.badge_suppression else roi
            match = self._cv2.matchTemplate(view, template, self._cv2.TM_CCOEFF_NORMED)
            _min, peak, _loc, peak_loc = self._cv2.minMaxLoc(match)
            if float(peak) > best:
                best = float(peak)
                dx = x0 + peak_loc[0] - anchor.box[0]
                dy = y0 + peak_loc[1] - anchor.box[1]
        return best, dx, dy

    def _search_window(
        self,
        frame: np.ndarray,
        anchor: Anchor,
        width: int,
        height: int,
    ) -> tuple[np.ndarray, int, int] | None:
        """外观搜索窗口：锚点框四周外扩并裁到帧边界；放不下模板时返回 None。"""
        x0, y0, x1, y1 = _window_bounds(anchor.box, APPEARANCE_PAD)
        if x1 - x0 < width or y1 - y0 < height:
            return None
        return frame[y0:y1, x0:x1], x0, y0

    def _position_ratio(self, frame: np.ndarray, anchor: Anchor) -> float:
        """位置通道：外扩窗口的灰度与局部中位数相差超过 45 的像素占比。

        在原始帧上计算，不做角标抑制：只回答「这个位置有没有图标结构」。
        """
        x0, y0, x1, y1 = _window_bounds(anchor.box, POSITION_PAD)
        gray = self._cv2.cvtColor(frame[y0:y1, x0:x1], self._cv2.COLOR_BGR2GRAY)
        values = gray.astype(self._np.float32)
        diff = self._np.abs(values - float(self._np.median(values)))
        return float((diff > POSITION_DIFF_LIMIT).mean())
