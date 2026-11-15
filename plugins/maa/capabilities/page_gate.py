"""主界面门控：截帧 → 判定 → 连帧确认 → 证据，放行才进入脚本阶段。

交接档门槛与判定规则、错误映射与证据口径见 docs/arknights/03、04。
门控只读：不点击、不按键、不启停进程，放行与否全部经返回值表达。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from contracts import ErrorCode, EventType, RunContext
from plugins.maa.vision.frames import Frame, FrameSource
from plugins.maa.vision.judge import AnchorDetail, HomeJudge, VisionResult
from plugins.maa.vision.states import HANDOVER_CONFIRM_FRAMES, FrameStreak

GATE_SIGNAL_KIND = "maa.gate.unknown"
FRAME_WAIT_SECONDS = 0.3
_EVIDENCE_PREFIX = "page-gate"


@dataclass(frozen=True)
class GateOutcome:
    """门控一次运行的结果：放行与否、错误码与给人看的门控说明。"""

    passed: bool
    error_code: ErrorCode | None
    detail: str


class PageGate:
    """交接档主界面门控：连续两帧确认才放行，每次判定写两条证据。"""

    def __init__(self, frames: FrameSource, wait: Callable[[float], None]) -> None:
        self._frames = frames
        self._wait = wait
        self._judge: HomeJudge | None = None

    def run(self, ctx: RunContext) -> GateOutcome:
        """按交接档截帧判定：确认即放行；截帧、分辨率、连帧失败各自收口。"""
        streak = FrameStreak()
        attempt = 0
        while True:
            frame = self._frames.capture()
            if frame is None:
                return _failed(ErrorCode.ENV_UNREACHABLE, "主界面门控截帧失败：帧源未返回帧")
            result = self._judge_frame(ctx, frame)
            attempt += 1
            if result.error_code is not None:
                size = f"{frame.width}x{frame.height}"
                detail = f"主界面门控分辨率不符：{size}，期望 1280x720，不缩放"
                return _failed(result.error_code, detail)
            if streak.observe(result).handover_confirmed:
                return _confirmed(result)
            if attempt >= HANDOVER_CONFIRM_FRAMES:
                return _not_confirmed(ctx, frame, result)
            self._wait(FRAME_WAIT_SECONDS)

    def _judge_frame(self, ctx: RunContext, frame: Frame) -> VisionResult:
        """判定一帧并落两条证据；判定耗时写进明细，供取证与预算核对。"""
        if self._judge is None:
            self._judge = HomeJudge()
        started = ctx.clock.monotonic()
        result = self._judge.judge(frame.image, frame.captured_at)
        elapsed_ms = (ctx.clock.monotonic() - started) * 1000.0
        _record_evidence(ctx, frame, result, elapsed_ms)
        return result


def _failed(code: ErrorCode, detail: str) -> GateOutcome:
    """不放行的统一出口：错误码与给人看的原因。"""
    return GateOutcome(passed=False, error_code=code, detail=detail)


def _confirmed(result: VisionResult) -> GateOutcome:
    """连续两帧达交接档：放行，说明供 prepare 的 detail 引用。"""
    return GateOutcome(
        passed=True,
        error_code=None,
        detail=(
            f"主界面门控放行：总分 {result.total_score}，外观命中 {result.appearance_hits}，"
            f"连续 {HANDOVER_CONFIRM_FRAMES} 帧"
        ),
    )


def _not_confirmed(ctx: RunContext, frame: Frame, result: VisionResult) -> GateOutcome:
    """连续两帧未达交接档：写 run.signal 后按环境未就绪失败。"""
    body = {"total_score": result.total_score, "appearance_hits": result.appearance_hits}
    ctx.emit(
        EventType.RUN_SIGNAL.value,
        {"kind": GATE_SIGNAL_KIND, "payload": body | {"frame_sha256": frame.sha256}},
    )
    return GateOutcome(
        passed=False,
        error_code=ErrorCode.ENV_NOT_READY,
        detail=(
            f"主界面门控未放行：总分 {result.total_score}，外观命中 {result.appearance_hits}，"
            f"连续 {HANDOVER_CONFIRM_FRAMES} 帧未达交接档"
        ),
    )


def _record_evidence(ctx: RunContext, frame: Frame, result: VisionResult, judge_ms: float) -> None:
    """这一帧的两条证据：截帧 PNG 用原始字节，判定明细写逐锚点 JSON。"""
    stem = f"{_EVIDENCE_PREFIX}-{frame.sha256[:12]}"
    (ctx.evidence_dir / f"{stem}.png").write_bytes(frame.png)
    payload = json.dumps(_probe_payload(frame, result, judge_ms), ensure_ascii=False, indent=2)
    (ctx.evidence_dir / f"{stem}.json").write_text(payload + "\n", encoding="utf-8")
    state, total, hits = result.page_state.value, result.total_score, result.appearance_hits
    verdict = "交接档命中" if result.handover_hit else "未达交接档"
    summary = f"主界面门控判定：{state} 总分 {total}，外观命中 {hits}，{verdict}"
    ctx.record_evidence(
        "screenshot",
        Path(f"{stem}.png"),
        f"主界面门控截帧：{state} 总分 {total}",
    )
    ctx.record_evidence("probe", Path(f"{stem}.json"), summary)


def _probe_payload(frame: Frame, result: VisionResult, judge_ms: float) -> dict[str, object]:
    """判定明细载荷：帧哈希、总分与档位结论、判定耗时与逐锚点计分。"""
    return {
        "frame_sha256": frame.sha256,
        "frame_source": frame.source.value,
        "page_state": result.page_state.value,
        "total_score": result.total_score,
        "appearance_hits": result.appearance_hits,
        "handover_hit": result.handover_hit,
        "navigation_hit": result.navigation_hit,
        "error_code": None if result.error_code is None else result.error_code.value,
        "judge_ms": round(judge_ms, 3),
        "anchors": [_anchor_row(anchor) for anchor in result.anchors],
    }


def _anchor_row(anchor: AnchorDetail) -> dict[str, object]:
    """单锚点计分明细：通道、得分、偏移、两路命中与本锚点分值。"""
    position = anchor.position_score
    return {
        "name": anchor.name,
        "channels": list(anchor.channels),
        "appearance_score": round(anchor.appearance_score, 4),
        "appearance_hit": anchor.appearance_hit,
        "offset": list(anchor.offset),
        "position_score": None if position is None else round(position, 4),
        "position_hit": anchor.position_hit,
        "points": anchor.points,
    }
