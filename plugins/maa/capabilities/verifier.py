"""MAA 验证能力（docs/plugins/maa.md 验证节，Verifier 协议见 docs/05）。

档位由插件配置 ``verify_mode`` 选：``script``（默认）按脚本证据判定；``screen``
只看画面证据——画面判定未接入，恒记 ``unknown``；``dual`` 先按 script 档得出脚本
结论，再与画面证据合成，失败优先，画面证据缺失时完成压成 ``unknown``。

script 档判定：脚本启动时把本次运行的日志起点写进工作目录的会话标记，验证阶段按
标记取 ``gui.log`` 的本次运行段，逐行识别信号后给结论。失败优先于完成——MAA 在单
任务出错后仍会打印「任务已全部完成！」，两者同时出现记 ``failed``。

插件配置带期望任务集时，完成行还须覆盖全部 enabled 任务才判完成；配置没有期望
任务集时维持只看完成行与失败行的口径。缺失任务只把结论压成 ``unknown``，不改
变失败优先级。理智采样从 ``asst.log`` 的本轮段汇总成 ``maa-sanity.json`` 作旁证，
不参与结论。

标记缺失、日志滚动、日志不可读或段内没有终止信号都记 ``unknown``，不降级为完成：
没有可判定的证据时，宁可交人工核对。``verify_mode`` 取值非法时按 ``script`` 处理
并记 warn，不让配置错误打断验证。
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from contracts import (
    CheckResult,
    CheckStatus,
    RunContext,
    Verdict,
    VerdictStatus,
)
from plugins.maa.logparse import (
    ASST_LOG_NAME,
    GUI_LOG_NAME,
    KIND_RUN_COMPLETED,
    KIND_RUN_DISCONNECTED,
    KIND_RUN_FAILED,
    KIND_TASK_COMPLETED,
    KIND_TASK_ERROR,
    KIND_TASK_STARTED,
    LOG_DIR_NAME,
    ParsedSignal,
    parse_asst_line,
    parse_gui_line,
    read_new_lines,
)
from plugins.maa.session import SessionMarker, read_marker

DEFAULT_POLICY_ID = "maa-policy"
EVIDENCE_LOG_NAME = "maa-run-guilog.log"
EVIDENCE_NOTE_NAME = "maa-verify-unknown.txt"
EVIDENCE_SANITY_NAME = "maa-sanity.json"
MAX_EVIDENCE_BYTES = 1_048_576

VERIFY_MODE_KEY = "verify_mode"
MODE_SCRIPT = "script"
MODE_SCREEN = "screen"
MODE_DUAL = "dual"
VERIFY_MODES = frozenset({MODE_SCRIPT, MODE_SCREEN, MODE_DUAL})
VERIFY_MODE_INVALID_EVENT = "maa.verify_mode.invalid"
SCREEN_UNAVAILABLE_REASON = "画面证据未接入：screen 档不读脚本日志，记 unknown 交人工核对"
SCREEN_MISSING_SUFFIX = "；画面证据缺失：画面口径未接入，不判完成"

_CHECK_NAME = "maa-script-log"
_SCREEN_CHECK_NAME = "maa-screen"
_TASK_SET_CHECK_NAME = "maa-task-set"
_FAILED_KINDS = frozenset({KIND_TASK_ERROR, KIND_RUN_FAILED, KIND_RUN_DISCONNECTED})
_EVIDENCE_CONFIDENCE = 0.9
# EvidenceKind 没有 json 取值（contracts 只认 screenshot/log/probe/exit/artifact），
# 结构化判定明细沿用 page_gate 的 probe。
_SANITY_EVIDENCE_KIND = "probe"


@dataclass(frozen=True, slots=True)
class _EvidenceFile:
    """一条待写证据：文件名、内容、登记 kind 与摘要。"""

    name: str
    text: str
    summary: str
    kind: str = "log"


class MaaVerifier:
    """verifier 能力：按 verify_mode 档位给出 completed / failed / unknown。"""

    def verify(self, ctx: RunContext) -> Verdict:
        """按档位判定；没有可判定证据时记 unknown 并留说明证据。"""
        mode = _resolve_verify_mode(ctx)
        if mode == MODE_SCREEN:
            return _unknown(ctx, SCREEN_UNAVAILABLE_REASON, check_name=_SCREEN_CHECK_NAME)
        verdict = _script_verdict(ctx)
        if mode == MODE_DUAL:
            return _merge_screen_evidence(verdict)
        return verdict


def _resolve_verify_mode(ctx: RunContext) -> str:
    """验证档位：插件配置 verify_mode；缺省按 script，非法取值记 warn 后回退。"""
    raw = ctx.config.get(VERIFY_MODE_KEY)
    if isinstance(raw, str) and raw in VERIFY_MODES:
        return raw
    if raw is not None:
        ctx.log.warn({"event": VERIFY_MODE_INVALID_EVENT, "run_id": ctx.run_id, "value": raw})
    return MODE_SCRIPT


def _script_verdict(ctx: RunContext) -> Verdict:
    """script 档判定：按会话标记取 gui.log 本次运行段并给结论。"""
    marker = read_marker(ctx.work_dir)
    if marker is None:
        return _unknown(ctx, "缺少会话标记：取不到本次运行的日志段")
    gui_log = _gui_log_path(ctx)
    if gui_log is None:
        return _unknown(ctx, "插件配置缺 maa_dir：定位不了脚本日志")
    problem, lines = _read_segment(gui_log, marker.gui_offset)
    if problem is not None:
        return _unknown(ctx, problem)
    return _judge(ctx, lines, marker, gui_log)


def _merge_screen_evidence(verdict: Verdict) -> Verdict:
    """dual 档合成：画面证据缺失时脚本完成压成 unknown；失败与未知原样保留。"""
    if verdict.status is not VerdictStatus.COMPLETED:
        return verdict
    return replace(
        verdict,
        status=VerdictStatus.UNKNOWN,
        confidence=0.0,
        reason=verdict.reason + SCREEN_MISSING_SUFFIX,
    )


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


def _judge(
    ctx: RunContext, lines: tuple[str, ...], marker: SessionMarker, gui_log: Path
) -> Verdict:
    """按段内信号判定：失败信号优先，完成行再过期望任务集；理智采样只作旁证。"""
    signals = _signals(lines)
    detail = _detail(signals)
    now = ctx.clock.utc_now()
    gui_segment = _EvidenceFile(
        name=EVIDENCE_LOG_NAME, text="\n".join(lines) + "\n", summary="本次运行的 gui.log 段"
    )
    evidence_id = _write_evidence(ctx, gui_segment)
    raw_tasks = ctx.config.get("tasks")
    task_check = _task_set_check(raw_tasks, _completed_tasks(signals), evidence_id, now)
    sanity_id = _write_sanity_evidence(ctx, marker, gui_log)
    script_check = CheckResult(
        name=_CHECK_NAME,
        status=_script_check_status(signals),
        detail=detail,
        evidence_ids=(evidence_id,),
        at=now,
    )
    status, reason = _outcome(signals, task_check, detail)
    evidence_ids = (evidence_id,) if sanity_id is None else (evidence_id, sanity_id)
    return Verdict(
        status=status,
        policy_id=DEFAULT_POLICY_ID,
        confidence=0.0 if status is VerdictStatus.UNKNOWN else _EVIDENCE_CONFIDENCE,
        reason=reason,
        checks=(script_check, task_check),
        evidence_ids=evidence_ids,
        created_at=now,
    )


def _has_failed_lines(signals: list[ParsedSignal]) -> bool:
    """段内是否出现任一失败信号（任务出错、错误汇总、掉线）。"""
    return any(signal.kind in _FAILED_KINDS for signal in signals)


def _has_completion_line(signals: list[ParsedSignal]) -> bool:
    """段内是否出现「任务已全部完成！」。"""
    return any(signal.kind == KIND_RUN_COMPLETED for signal in signals)


def _outcome(
    signals: list[ParsedSignal], task_check: CheckResult, detail: str
) -> tuple[VerdictStatus, str]:
    """结论与原因：失败行 > 完成行加任务集全齐 > 其余 unknown；缺失任务压成 unknown。"""
    if _has_failed_lines(signals):
        return VerdictStatus.FAILED, f"脚本证据判定失败：{detail}"
    if _has_completion_line(signals):
        if task_check.status is CheckStatus.FAILED:
            return VerdictStatus.UNKNOWN, f"完成行在而期望任务未全部完成，{task_check.detail}"
        return VerdictStatus.COMPLETED, f"脚本证据判定完成：{detail}"
    return VerdictStatus.UNKNOWN, f"日志段内没有终止信号：{detail}"


def _script_check_status(signals: list[ParsedSignal]) -> CheckStatus:
    """脚本日志检查项：失败行 failed，完成行 ok，没有终止信号 unknown。"""
    if _has_failed_lines(signals):
        return CheckStatus.FAILED
    if _has_completion_line(signals):
        return CheckStatus.OK
    return CheckStatus.UNKNOWN


def _task_set_check(
    raw_tasks: object, completed: frozenset[str], evidence_id: str, at: datetime | None
) -> CheckResult:
    """期望任务集核对：全部完成 ok，有缺失 failed，配置没有期望集 unknown。"""
    expected = _expected_task_names(raw_tasks)
    if expected is None:
        status, detail = CheckStatus.UNKNOWN, "插件配置没有期望任务集，未核对"
    elif missing := sorted(expected - completed):
        status, detail = CheckStatus.FAILED, "缺失任务：" + "、".join(missing)
    else:
        status, detail = CheckStatus.OK, f"期望任务 {len(expected)} 个全部完成"
    return CheckResult(
        name=_TASK_SET_CHECK_NAME,
        status=status,
        detail=detail,
        evidence_ids=(evidence_id,),
        at=at,
    )


def _expected_task_names(raw_tasks: object) -> frozenset[str] | None:
    """期望任务集：插件配置 tasks 里 enabled 不为 false 的任务名；取不到集合返回 None。"""
    names: set[str] = set()
    if isinstance(raw_tasks, list):
        for item in raw_tasks:
            name = _enabled_task_name(item)
            if name is not None:
                names.add(name)
    return frozenset(names) if names else None


def _enabled_task_name(item: object) -> str | None:
    """单个任务项的名字：enabled 不为 false 且名字非空才参与核对。"""
    if not isinstance(item, Mapping) or item.get("enabled") is False:
        return None
    name = item.get("name")
    if not isinstance(name, str):
        return None
    return name.strip() or None


def _completed_tasks(signals: list[ParsedSignal]) -> frozenset[str]:
    """段内已完成任务名集合：完成行点名，空名不参与核对。"""
    names: set[str] = set()
    for signal in signals:
        if signal.kind != KIND_TASK_COMPLETED:
            continue
        name = str(signal.payload.get("task", ""))
        if name:
            names.add(name)
    return frozenset(names)


def _write_sanity_evidence(ctx: RunContext, marker: SessionMarker, gui_log: Path) -> str | None:
    """本轮 asst 段的理智采样汇总写 maa-sanity.json 并登记；没有采样就不写。"""
    samples = _sanity_samples(_asst_log_path(gui_log), marker.asst_offset)
    summary = _sanity_summary(samples)
    if summary is None:
        return None
    file = _EvidenceFile(
        name=EVIDENCE_SANITY_NAME,
        text=json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        summary="本轮 asst.log 的理智采样汇总",
        kind=_SANITY_EVIDENCE_KIND,
    )
    return _write_evidence(ctx, file)


def _asst_log_path(gui_log: Path) -> Path:
    """asst 日志与 gui.log 同目录：MAA 目录下的 debug/asst.log。"""
    return gui_log.parent / ASST_LOG_NAME


def _sanity_samples(path: Path, offset: int) -> list[ParsedSignal]:
    """本轮 asst 段内的理智采样；文件读不了或已滚动按没有采样处理。"""
    chunk = read_new_lines(path, offset)
    if chunk.truncated:
        return []
    return [
        signal for signal in (parse_asst_line(line) for line in chunk.lines) if signal is not None
    ]


def _sanity_summary(samples: list[ParsedSignal]) -> dict[str, int] | None:
    """采样汇总：条数与首末 current_sanity，delta 为末减首；取不到值返回 None。"""
    values: list[int] = []
    for sample in samples:
        value = sample.payload.get("current_sanity")
        if isinstance(value, int) and not isinstance(value, bool):
            values.append(value)
    if not values:
        return None
    return {
        "sample_count": len(values),
        "first_current_sanity": values[0],
        "last_current_sanity": values[-1],
        "delta": values[-1] - values[0],
    }


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


def _unknown(ctx: RunContext, reason: str, *, check_name: str = _CHECK_NAME) -> Verdict:
    """没有可判定证据时的收口：写说明证据，结论记 unknown；检查项名随档位给。"""
    now = ctx.clock.utc_now()
    note = _EvidenceFile(name=EVIDENCE_NOTE_NAME, text=f"{reason}\n", summary="验证不可用的原因")
    evidence_id = _write_evidence(ctx, note)
    check = CheckResult(
        name=check_name,
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


def _write_evidence(ctx: RunContext, file: _EvidenceFile) -> str:
    """写证据文件并请内核登记；超长日志段只保留末尾，避免证据无限膨胀。"""
    encoded = file.text.encode("utf-8")
    text = file.text
    if len(encoded) > MAX_EVIDENCE_BYTES:
        tail = encoded[-MAX_EVIDENCE_BYTES:].decode("utf-8", errors="replace")
        text = "# 日志段超过 1 MiB，只保留末尾\n" + tail
    target = ctx.evidence_dir / file.name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    return ctx.record_evidence(file.kind, Path(file.name), file.summary)
