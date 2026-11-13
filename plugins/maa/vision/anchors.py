"""主界面判定的锚点表与常量：docs/arknights/03、04 参数表的唯一代码出处。

判定器与冻结样本夹具共用本表：外观搜索窗口、位置通道窗口与夹具覆盖区
都从 ``Anchor.box`` 派生，调整框位即三者同步变化。本模块只含数据，
不导入视觉依赖，可在未安装 cv2 的环境加载。
"""

from __future__ import annotations

from dataclasses import dataclass

FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
FRAME_SHAPE = (FRAME_HEIGHT, FRAME_WIDTH, 3)

APPEARANCE_THRESHOLD = 0.85
APPEARANCE_OFFSET_LIMIT = 16
APPEARANCE_PAD = 18
POSITION_PAD = 10
POSITION_DIFF_LIMIT = 45
BADGE_S_LIMIT = 110
BADGE_V_LIMIT = 130
BADGE_GRAY = 128
APPEARANCE_POINTS = 2
POSITION_POINTS = 1
HANDOVER_MIN_TOTAL = 8
HANDOVER_MIN_APPEARANCE = 2
NAVIGATION_MIN_TOTAL = 10
NAVIGATION_MIN_APPEARANCE = 4


@dataclass(frozen=True)
class Anchor:
    """一个主题无关锚点：位置框、模板变体清单与通道参与方式。

    ``badge_suppression`` 为真时，该锚点的模板与搜索窗口在匹配前做角标
    抑制；``redeem`` 本体是高饱和色块，抑制会抹掉它自身的字形，故为假。
    """

    name: str
    box: tuple[int, int, int, int]
    templates: tuple[str, ...]
    position_threshold: float | None
    badge_suppression: bool


ANCHORS: tuple[Anchor, ...] = (
    Anchor(
        name="gear",
        box=(27, 19, 41, 41),
        templates=("gear.png",),
        position_threshold=0.26,
        badge_suppression=True,
    ),
    Anchor(
        name="warning",
        box=(97, 19, 44, 39),
        templates=("warning.png",),
        position_threshold=0.33,
        badge_suppression=True,
    ),
    Anchor(
        name="calendar",
        box=(247, 21, 53, 36),
        templates=("calendar.png", "calendar-clean.png"),
        position_threshold=0.31,
        badge_suppression=True,
    ),
    Anchor(
        name="redeem",
        box=(1053, 27, 41, 49),
        templates=("redeem.png",),
        position_threshold=None,
        badge_suppression=False,
    ),
    Anchor(
        name="loop",
        box=(290, 391, 29, 24),
        templates=("loop.png",),
        position_threshold=None,
        badge_suppression=True,
    ),
)
