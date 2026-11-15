"""冻结样本回归：42 帧按期望判定，manifest 重建画布与生成时明细一致。"""

from __future__ import annotations

import math

import numpy as np

from plugins.maa.vision.anchors import ANCHORS, HANDOVER_MIN_TOTAL
from plugins.maa.vision.judge import VisionResult
from tests.maa_vision_support import (
    FIXTURES,
    NOW,
    coverage_rect,
    load_frame_png,
    load_manifest,
    make_judge,
    rebuild_canvas,
)

# 非主界面验收上限 2 来自 docs/arknights/03 的验收判据，不是判定器门槛。
OTHER_MAX_TOTAL = 2


def _check_overlap(entry: dict, problems: list[str]) -> None:
    """同一帧内两两裁片的重叠区必须逐像素一致（容差 0）。"""
    regions = entry["regions"]
    names = list(regions)
    for index, a in enumerate(names):
        for b in names[index + 1 :]:
            ra, rb = regions[a], regions[b]
            x0 = max(int(ra["x"]), int(rb["x"]))
            y0 = max(int(ra["y"]), int(rb["y"]))
            x1 = min(int(ra["x"]) + int(ra["w"]), int(rb["x"]) + int(rb["w"]))
            y1 = min(int(ra["y"]) + int(ra["h"]), int(rb["y"]) + int(rb["h"]))
            if x1 <= x0 or y1 <= y0:
                continue
            ca = load_frame_png(FIXTURES / str(ra["file"]))
            cb = load_frame_png(FIXTURES / str(rb["file"]))
            sa = ca[y0 - int(ra["y"]) : y1 - int(ra["y"]), x0 - int(ra["x"]) : x1 - int(ra["x"])]
            sb = cb[y0 - int(rb["y"]) : y1 - int(rb["y"]), x0 - int(rb["x"]) : x1 - int(rb["x"])]
            if not np.array_equal(sa, sb):
                problems.append(f"{entry['name']}: 裁片 {a}/{b} 重叠区不一致")


def _same_optional_float(a: float | None, b: float | None) -> bool:
    """可选浮点比较：同时缺省算一致，数值按绝对误差 1e-6。"""
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(a, b, rel_tol=0.0, abs_tol=1e-6)


def _check_judge(entry: dict, result: VisionResult, problems: list[str]) -> None:
    """重建画布的判定明细必须与 manifest 记录的生成时明细一致。"""
    recorded = entry["judge"]
    if (
        result.total_score != recorded["total_score"]
        or result.appearance_hits != recorded["appearance_hits"]
        or result.handover_hit != recorded["handover_hit"]
        or result.navigation_hit != recorded["navigation_hit"]
    ):
        problems.append(f"{entry['name']}: 重建判定总分/命中/门槛与 manifest 不一致")
        return
    for detail in result.anchors:
        row = recorded["anchors"][detail.name]
        if not _same_optional_float(detail.appearance_score, row["appearance_score"]):
            problems.append(f"{entry['name']}/{detail.name}: 外观得分漂移")
        if not _same_optional_float(detail.position_score, row["position_score"]):
            problems.append(f"{entry['name']}/{detail.name}: 位置得分漂移")
        if (
            detail.appearance_hit != row["appearance_hit"]
            or detail.position_hit != row["position_hit"]
            or detail.offset != tuple(row["offset"])
            or detail.points != row["points"]
        ):
            problems.append(f"{entry['name']}/{detail.name}: 命中/偏移/计分不一致")


def test_frozen_frames_regression() -> None:
    """22 帧主界面全部 HOME 且最低总分 ≥ 8；20 帧非主界面全部 OTHER 且最高总分 ≤ 2。"""
    manifest = load_manifest()
    judge = make_judge()
    rows = []
    failures = []
    home_totals: list[int] = []
    other_totals: list[int] = []
    for entry in manifest["frames"]:
        result = judge.judge(rebuild_canvas(entry), NOW)
        state = result.page_state.value
        expected = str(entry["expected"])
        total = result.total_score
        ok = state == expected
        if expected == "home":
            home_totals.append(total)
            ok = ok and total >= HANDOVER_MIN_TOTAL
        else:
            other_totals.append(total)
            ok = ok and total <= OTHER_MAX_TOTAL
        row = (
            f"{entry['name']:<28} {entry['theme']:<17} 期望 {expected:<5}"
            f" 实得 {state:<5} 总分 {total}"
        )
        rows.append(row)
        if not ok:
            failures.append(row)
    assert len(home_totals) == 22, f"主界面帧数 {len(home_totals)} ≠ 22"
    assert len(other_totals) == 20, f"非主界面帧数 {len(other_totals)} ≠ 20"
    assert min(home_totals) >= HANDOVER_MIN_TOTAL, (
        f"主界面最低总分 {min(home_totals)} < {HANDOVER_MIN_TOTAL}"
    )
    assert max(other_totals) <= OTHER_MAX_TOTAL, f"非主界面最高总分 {max(other_totals)} > 2"
    assert not failures, "冻结回归失败，逐帧明细：\n" + "\n".join(rows)


def test_manifest_matches_rebuilt_canvas() -> None:
    """manifest 字段齐全且 42 条；几何与锚点表一致；重叠裁片逐像素一致；重建判定一致。"""
    manifest = load_manifest()
    assert len(manifest["frames"]) == 42
    judge = make_judge()
    problems: list[str] = []
    for entry in manifest["frames"]:
        for field in ("name", "batch", "theme", "expected", "regions", "judge"):
            if field not in entry:
                problems.append(f"{entry.get('name')}: 缺字段 {field}")
        for anchor in ANCHORS:
            region = entry["regions"].get(anchor.name)
            if region is None:
                problems.append(f"{entry['name']}: 缺覆盖区 {anchor.name}")
                continue
            x, y, w, h = coverage_rect(anchor.box)
            if (region["x"], region["y"], region["w"], region["h"]) != (x, y, w, h):
                problems.append(f"{entry['name']}/{anchor.name}: 覆盖区几何与锚点表不符")
            crop = load_frame_png(FIXTURES / str(region["file"]))
            if crop.shape != (h, w, 3):
                problems.append(f"{entry['name']}/{anchor.name}: 裁片尺寸不符 {crop.shape}")
        _check_overlap(entry, problems)
        _check_judge(entry, judge.judge(rebuild_canvas(entry), NOW), problems)
    assert not problems, "manifest 与重建画布不一致：\n" + "\n".join(problems)
