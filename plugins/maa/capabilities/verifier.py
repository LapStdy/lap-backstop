"""MAA 验证能力（docs/plugins/maa.md 验证节，Verifier 协议见 docs/05）。

判定只用脚本证据：脚本启动时把本次运行的日志起点写进工作目录的会话标记，验证
阶段按标记取 ``gui.log`` 的本次运行段，逐行识别信号后给结论。失败优先于完成——
MAA 在单任务出错后仍会打印「任务已全部完成！」，两者同时出现记 ``failed``。

标记缺失、日志滚动、日志不可读或段内没有终止信号都记 ``unknown``，不降级为完成：
没有可判定的证据时，宁可交人工核对。画面证据与理智核对未接入本能力。
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from contracts import (
    CheckResult,
    CheckStatus,
    RunContext,
    Verdict,
    VerdictStatus,
)
from plugins.maa.logparse import (
    GUI_LOG_NAME,
    KIND_RUN_COMPLETED,
    KIND_RUN_DISCONNECTED,
    KIND_RUN_FAILED,
    KIND_TASK_COMPLETED,
    KIND_TASK_ERROR,
    KIND_TASK_STARTED,
    LOG_DIR_NAME,
    ParsedSignal,
    parse_gui_line,
    read_new_lines,
)
from plugins.maa.session import read_marker

DEFAULT_POLICY_ID = "maa-policy"
EVIDENCE_LOG_NAME = "maa-run-guilog.log"
EVIDENCE_NOTE_NAME = "maa-verify-unknown.txt"
MAX_EVIDENCE_BYTES = 1_048_576

_CHECK_NAME = "maa-script-log"
_FAILED_KINDS = frozenset({KIND_TASK_ERROR, KIND_RUN_FAILED, KIND_RUN_DISCONNECTED})
_EVIDENCE_CONFIDENCE = 0.9


class MaaVerifier:
    """verifier 能力：按本次运行的 gui.log 段给出 completed / failed / unknown。"""

    def verify(self, ctx: RunContext) -> Verdict:
        """取本次运行的日志段并判定；没有可判定证据时记 unknown 并留说明证据。"""
        marker = read_marker(ctx.work_dir)
        if marker is None:
            return _unknown(ctx, "缺少会话标记：取不到本次运行的日志段")
        gui_log = _gui_log_path(ctx)
        if gui_log is None:
            return _unknown(ctx, "插件配置缺 maa_dir：定位不了脚本日志")
        problem, lines = _read_segment(gui_log, marker.gui_offset)
        if problem is not None:
            return _unknown(ctx, problem)
        return _judge(ctx, lines)


def _gui_log_path(ctx: RunContext) -> Path | None:
    """日志路径由插件配置推出：MAA 目录下的 debug/gui.log。"""
    maa_dir = ctx.config.get("maa_dir")
    if not isinstance(maa_dir, str) or not maa_dir:
        return None
    return Path(maa_dir) / LOG_DIR_NAME / GUI_LOG_NAME


def _read_segment(path: Path, offset: int) -> tuple[str | None, tuple[str, ...]]:
    """读本次运行段；日志不在或已滚动时返回原因，不猜段。"""
    if not path.is_file():
        return f"脚本日志不存在：{path}", ()
    chunk = read_new_lines(path, offset)
    if chunk.truncated:
        return "脚本日志已滚动：本次运行段被截断", ()
    return None, chunk.lines


def _judge(ctx: RunContext, lines: tuple[str, ...]) -> Verdict:
    """按段内信号判定；失败信号优先，其次完成行，都没有记 unknown。"""
    signals = _signals(lines)
    detail = _detail(signals)
    now = ctx.clock.utc_now()
    evidence_id = _write_evidence(
        ctx,
        "\n".join(lines) + "\n",
        EVIDENCE_LOG_NAME,
        "本次运行的 gui.log 段",
    )
    if any(signal.kind in _FAILED_KINDS for signal in signals):
        status, check_status = VerdictStatus.FAILED, CheckStatus.FAILED
        reason = f"脚本证据判定失败：{detail}"
    elif any(signal.kind == KIND_RUN_COMPLETED for signal in signals):
        status, check_status = VerdictStatus.COMPLETED, CheckStatus.OK
        reason = f"脚本证据判定完成：{detail}"
    else:
        status, check_status = VerdictStatus.UNKNOWN, CheckStatus.UNKNOWN
        reason = f"日志段内没有终止信号：{detail}"
    check = CheckResult(
        name=_CHECK_NAME,
        status=check_status,
        detail=detail,
        evidence_ids=(evidence_id,),
        at=now,
    )
    return Verdict(
        status=status,
        policy_id=DEFAULT_POLICY_ID,
        confidence=0.0 if status is VerdictStatus.UNKNOWN else _EVIDENCE_CONFIDENCE,
        reason=reason,
        checks=(check,),
        evidence_ids=(evidence_id,),
        created_at=now,
    )


def _signals(lines: tuple[str, ...]) -> list[ParsedSignal]:
    """逐行识别信号；未知行跳过，不参与判定。"""
    signals: list[ParsedSignal] = []
    for line in lines:
        parsed = parse_gui_line(line)
        if parsed is not None:
            signals.append(parsed)
    return signals


def _detail(signals: list[ParsedSignal]) -> str:
    """折成一句可读说明：任务计数、掉线次数与出错任务名。"""
    counts = Counter(signal.kind for signal in signals)
    failed_names = sorted(
        {
            str(signal.payload.get("task", ""))
            for signal in signals
            if signal.kind == KIND_TASK_ERROR
        }
        - {""}
    )
    text = (
        f"任务开始 {counts[KIND_TASK_STARTED]} 个，完成 {counts[KIND_TASK_COMPLETED]} 个，"
        f"出错 {counts[KIND_TASK_ERROR]} 个，掉线 {counts[KIND_RUN_DISCONNECTED]} 次，"
        f"整轮完成行 {counts[KIND_RUN_COMPLETED]} 条"
    )
    if failed_names:
        text += "；出错任务：" + "、".join(failed_names)
    return text


def _unknown(ctx: RunContext, reason: str) -> Verdict:
    """没有可判定证据时的收口：写说明证据，结论记 unknown。"""
    now = ctx.clock.utc_now()
    evidence_id = _write_evidence(ctx, f"{reason}\n", EVIDENCE_NOTE_NAME, "验证不可用的原因")
    check = CheckResult(
        name=_CHECK_NAME,
        status=CheckStatus.UNKNOWN,
        detail=reason,
        evidence_ids=(evidence_id,),
        at=now,
    )
    return Verdict(
        status=VerdictStatus.UNKNOWN,
        policy_id=DEFAULT_POLICY_ID,
        confidence=0.0,
        reason=reason,
        checks=(check,),
        evidence_ids=(evidence_id,),
        created_at=now,
    )


def _write_evidence(ctx: RunContext, text: str, name: str, summary: str) -> str:
    """写证据文件并请内核登记；超长日志段只保留末尾，避免证据无限膨胀。"""
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_EVIDENCE_BYTES:
        tail = encoded[-MAX_EVIDENCE_BYTES:].decode("utf-8", errors="replace")
        text = "# 日志段超过 1 MiB，只保留末尾\n" + tail
    target = ctx.evidence_dir / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    return ctx.record_evidence("log", Path(name), summary)
