"""登录阶段判读：覆盖层先于主界面，其次是准备画面、登录界面与加载屏三张模板。

阈值来自 2026-11-15 真机帧实测：START 模板在准备画面各页 0.82–1.0、
在登录与主界面 ≤0.36；开始唤醒与加载三角在目标画面 1.0、其余 ≤0.67。
每日签到锚点在两套签到面板上 0.986–1.0、在主界面与其余样本 ≤0.57，故门槛取 0.90：
覆盖层盖住主界面时主界面锚点会全部落空，先认覆盖层才不会把它当未知画面。
判读只读像素：不点击、不按键，缺模板文件按 UNKNOWN 处理，不抛异常。
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from plugins.maa.vision.frames import Frame
from plugins.maa.vision.judge import HomeJudge, vision_modules
from plugins.maa.vision.readiness import LoginReading, LoginStage, OverlayKind

if TYPE_CHECKING:
    import numpy as np

DEFAULT_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"
START_TEMPLATE = "start_button.png"
WAKE_TEMPLATE = "wake_button.png"
LOADING_TEMPLATE = "loading_logo.png"
DAILY_SIGNIN_TEMPLATE = "daily_signin_anchor.png"
START_THRESHOLD = 0.80
TEMPLATE_THRESHOLD = 0.85
OVERLAY_THRESHOLD = 0.90


class LoginVision:
    """登录阶段判读器：覆盖层优先，其次主界面，再其次三张模板，都不中给 UNKNOWN。"""

    def __init__(self, template_dir: Path = DEFAULT_TEMPLATE_DIR) -> None:
        self._dir = template_dir
        self._home = HomeJudge()
        self._cache: dict[str, np.ndarray | None] = {}

    def judge(self, frame: Frame) -> LoginReading:
        """判读一帧：先认覆盖层，再判主界面，最后按模板得分定阶段。"""
        overlay = self._overlay(frame.image)
        if overlay is not None:
            return overlay
        result = self._home.judge(frame.image, frame.captured_at)
        if result.handover_hit:
            return LoginReading(LoginStage.HOME)
        for name, stage in (
            (START_TEMPLATE, LoginStage.START),
            (WAKE_TEMPLATE, LoginStage.LOGIN),
            (LOADING_TEMPLATE, LoginStage.LOADING),
        ):
            score = self._score(frame.image, name)
            threshold = START_THRESHOLD if name is START_TEMPLATE else TEMPLATE_THRESHOLD
            if score is not None and score >= threshold:
                return LoginReading(stage, confidence=score)
        return LoginReading(LoginStage.UNKNOWN)

    def _overlay(self, image: np.ndarray) -> LoginReading | None:
        """每日签到面板的判读；未命中返回 None，交给后续阶段。"""
        score = self._score(image, DAILY_SIGNIN_TEMPLATE)
        if score is None or score < OVERLAY_THRESHOLD:
            return None
        return LoginReading(LoginStage.OVERLAY, OverlayKind.DAILY_SIGNIN, confidence=score)

    def _score(self, image: np.ndarray, name: str) -> float | None:
        """灰度模板匹配的最高分；模板缺失返回 None。"""
        template = self._template(name)
        if template is None:
            return None
        cv2, _ = vision_modules()
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        if template.shape[0] > gray.shape[0] or template.shape[1] > gray.shape[1]:
            return None
        return float(cv2.matchTemplate(gray, template, cv2.TM_CCOEFF_NORMED).max())

    def _template(self, name: str) -> np.ndarray | None:
        """模板懒加载并缓存；文件不存在返回 None。"""
        if name not in self._cache:
            path = self._dir / name
            if not path.is_file():
                self._cache[name] = None
            else:
                cv2, _ = vision_modules()
                self._cache[name] = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
        return self._cache[name]
