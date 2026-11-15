"""登录判读器用例：把仓库内模板贴到合成画布，验证阶段识别与缺模板兜底。"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np

from plugins.maa.vision.frames import Frame, FrameOrigin
from plugins.maa.vision.judge import DEFAULT_TEMPLATE_DIR, vision_modules
from plugins.maa.vision.login_vision import (
    LOADING_TEMPLATE,
    START_TEMPLATE,
    WAKE_TEMPLATE,
    LoginVision,
)
from plugins.maa.vision.readiness import LoginStage


def _canvas() -> np.ndarray:
    rng = np.random.default_rng(7)
    return rng.integers(0, 60, (720, 1280, 3), dtype=np.uint8)


def _place(canvas: np.ndarray, name: str, x: int, y: int) -> np.ndarray:
    cv2, _ = vision_modules()
    template = cv2.imread(str(DEFAULT_TEMPLATE_DIR / name), cv2.IMREAD_COLOR)
    height, width = template.shape[:2]
    canvas[y : y + height, x : x + width] = template
    return canvas


def _frame(image: np.ndarray) -> Frame:
    return Frame(
        image=image,
        width=1280,
        height=720,
        captured_at=datetime.now(UTC),
        sha256="00" * 32,
        source=FrameOrigin.ADB,
        png=b"",
    )


def test_start_screen_recognized() -> None:
    vision = LoginVision()
    reading = vision.judge(_frame(_place(_canvas(), START_TEMPLATE, 600, 640)))
    assert reading.stage is LoginStage.START


def test_login_screen_recognized() -> None:
    vision = LoginVision()
    reading = vision.judge(_frame(_place(_canvas(), WAKE_TEMPLATE, 530, 472)))
    assert reading.stage is LoginStage.LOGIN


def test_loading_screen_recognized() -> None:
    vision = LoginVision()
    reading = vision.judge(_frame(_place(_canvas(), LOADING_TEMPLATE, 480, 210)))
    assert reading.stage is LoginStage.LOADING


def test_blank_canvas_is_unknown() -> None:
    vision = LoginVision()
    assert vision.judge(_frame(_canvas())).stage is LoginStage.UNKNOWN


def test_missing_templates_fall_back_to_unknown(tmp_path) -> None:
    vision = LoginVision(template_dir=tmp_path)
    assert vision.judge(_frame(_canvas())).stage is LoginStage.UNKNOWN
