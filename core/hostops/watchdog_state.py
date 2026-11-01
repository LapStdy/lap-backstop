"""状态文件读写与停手报告：``watchdog.state.json`` 的读改写与停手载荷的构造、解析、兜底。

状态文件由看门狗与核心两个进程共写，读缺失或损坏一律按初始态处理，写走
临时文件加原子替换；停手报告的唯一构造点是 _build_report，键名只在此枚举
一次，取证、解析与兜底都经它落形。
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

STATE_FILENAME = "watchdog.state.json"


class GaveUpReport(TypedDict):
    """停手当时的取证载荷：窗口失败数、最近失败时刻、心跳内容与核心日志尾部。"""

    failed_count: int
    last_failure_at: str | None
    heartbeat: dict[str, Any] | None
    log_tail: list[str]


class WatchdogState(TypedDict, total=False):
    """状态文件 schema：拉起待验收时刻、验收失败时刻列表、停手待上报标记、
    停手报告与 dead-man 失败通知去重标记（键由 core.hostops.deadman 读写）。"""

    pending_start: str | None
    failures: list[str]
    gave_up_pending_report: bool
    gave_up_report: GaveUpReport | None
    deadman_fail_notified: bool


def read_state(path: Path) -> WatchdogState:
    """读状态文件；缺失或损坏一律按初始态处理（人工删除即复位）。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    failures = data.get("failures")
    pending_start = data.get("pending_start")
    return {
        "pending_start": pending_start if isinstance(pending_start, str) else None,
        "failures": (
            [item for item in failures if isinstance(item, str)]
            if isinstance(failures, list)
            else []
        ),
        "gave_up_pending_report": bool(data.get("gave_up_pending_report")),
        "gave_up_report": _parse_gave_up_report(data.get("gave_up_report")),
        "deadman_fail_notified": bool(data.get("deadman_fail_notified")),
    }


def write_state(path: Path, state: WatchdogState) -> None:
    """原子写状态文件（临时文件加替换）：本文件由看门狗与核心两个进程共写。

    Raises:
        OSError: 目录不可写等；调用方记日志退出，状态丢失可接受——下次检查
            会重新判死重新拉起，方向安全。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(json.dumps(state, ensure_ascii=False, sort_keys=True))
    os.replace(temporary, path)


def take_gave_up_event_payload(state_path: Path) -> dict[str, object] | None:
    """取走停手待上报载荷：有报告则清标记并返回事件 payload，无则 None。

    报告不可解析走兜底：报告里 failed_count 或 log_tail 缺失、类型不符按
    不可解析处理，失败次数与最近失败时刻从已存 failures 推，last_failure_at
    与 heartbeat 缺失按 None 补，日志尾部为空。payload 是人读 detail（通知
    路径依赖它）加这四个证据字段。供核心 serve 启动时调用：看门狗零数据库
    访问，事件由核心补写。

    Raises:
        OSError: 清标记后写回状态文件失败；磁盘上的标记未变，下次启动
            仍会取到同一报告，由调用方决定退出还是重试。
    """
    state = read_state(state_path)
    if not state.get("gave_up_pending_report"):
        return None
    report = state.get("gave_up_report")
    if report is None:
        report = _fallback_gave_up_report(state)
    state["gave_up_pending_report"] = False
    state["gave_up_report"] = None
    write_state(state_path, state)
    return _gave_up_event_payload(report)


def _build_report(
    *,
    failed_count: int,
    last_failure_at: str | None,
    heartbeat: dict[str, Any] | None,
    log_tail: list[str],
) -> GaveUpReport:
    """停手报告的唯一构造点：键名只在此枚举一次，取证、解析与兜底都经它落形。"""
    return {
        "failed_count": failed_count,
        "last_failure_at": last_failure_at,
        "heartbeat": heartbeat,
        "log_tail": log_tail,
    }


def _parse_gave_up_report(value: object) -> GaveUpReport | None:
    """校验停手报告形态；failed_count 或 log_tail 缺失、类型不符按不可解析处理。

    last_failure_at 与 heartbeat 不要求存在：缺失按 None 补，类型不符才拒收。
    """
    if not isinstance(value, dict):
        return None
    failed_count = value.get("failed_count")
    last_failure_at = value.get("last_failure_at")
    heartbeat = value.get("heartbeat")
    log_tail = value.get("log_tail")
    if not isinstance(failed_count, int) or isinstance(failed_count, bool):
        return None
    if last_failure_at is not None and not isinstance(last_failure_at, str):
        return None
    if heartbeat is not None and not isinstance(heartbeat, dict):
        return None
    if not isinstance(log_tail, list) or not all(isinstance(item, str) for item in log_tail):
        return None
    return _build_report(
        failed_count=failed_count,
        last_failure_at=last_failure_at,
        heartbeat=heartbeat,
        log_tail=list(log_tail),
    )


def _fallback_gave_up_report(state: WatchdogState) -> GaveUpReport:
    """兜底载荷：旧状态文件只有布尔标记时，从已存失败记录推证据字段。"""
    failures = state.get("failures", [])
    last = _parse_iso(failures[-1]) if failures else None
    return _build_report(
        failed_count=len(failures),
        last_failure_at=_iso_seconds(last),
        heartbeat=None,
        log_tail=[],
    )


def _gave_up_event_payload(report: GaveUpReport) -> dict[str, object]:
    """停手事件 payload：人读 detail 加四个证据字段（docs/08 停手一节）。"""
    last = report["last_failure_at"]
    detail = (
        f"看门狗已停止拉起：窗口内 {report['failed_count']} 次验收失败，"
        f"最近一次 {last if last is not None else '未知时刻'}"
    )
    return {
        "detail": detail,
        **_build_report(
            failed_count=report["failed_count"],
            last_failure_at=last,
            heartbeat=report["heartbeat"],
            log_tail=list(report["log_tail"]),
        ),
    }


def _iso_seconds(moment: datetime | None) -> str | None:
    """格式化为秒级 UTC ISO8601；None 原样通过。"""
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat(timespec="seconds")


def _parse_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)
