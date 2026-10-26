"""事件到通知文案的映射：结论、作业名、耗时、错误码与证据入口（docs/06、docs/07）。

只做映射与查库取素材，不投递、不落事件；编排与簿记由 service 负责。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from contracts import EnvironmentState, Notification, NotificationLevel
from storage.repositories.evidence import EvidenceRepository
from storage.repositories.job_runs import JobRunRecord, JobRunRepository
from storage.repositories.job_templates import JobTemplateRepository

_EVIDENCE_IDS_LIMIT: Final[int] = 3

# 运行终态到通知级别、标题与结论文案的映射；cancelled 是用户主动行为，不推。
_RUN_ENDS: Final[dict[str, tuple[NotificationLevel, str, str]]] = {
    "succeeded": (NotificationLevel.INFO, "运行成功", "成功"),
    "failed": (NotificationLevel.ERROR, "运行失败", "失败"),
    "unknown": (NotificationLevel.WARN, "运行结束但结果未知", "未知"),
}


def no_emit(event: str, fields: Mapping[str, Any]) -> None:
    """内核投递上下文没有事件出口。"""


def reject_evidence(kind: str, path: Path, summary: str) -> str:
    """内核投递上下文不接受证据写入。"""
    raise ValueError("通知投递上下文不接收证据")


def _format_duration(duration_ms: object) -> str | None:
    """毫秒转人可读时长；缺失或非法返回 None，由调用方决定是否显示。"""
    if isinstance(duration_ms, bool) or not isinstance(duration_ms, (int, float)):
        return None
    if duration_ms < 0:
        return None
    minutes, seconds = divmod(round(float(duration_ms) / 1000), 60)
    if minutes >= 60:
        hours, minutes = divmod(minutes, 60)
        return f"{hours} 小时 {minutes} 分 {seconds} 秒"
    if minutes > 0:
        return f"{minutes} 分 {seconds} 秒"
    return f"{seconds} 秒"


def _record_duration(run: JobRunRecord | None) -> str | None:
    """用运行的起止时刻算耗时；两端不齐返回 None。"""
    if run is None or run.started_at is None or run.finished_at is None:
        return None
    seconds = (run.finished_at - run.started_at).total_seconds()
    return _format_duration(seconds * 1000)


def test_notification() -> Notification:
    """连通性测试的固定文案。"""
    return Notification(level=NotificationLevel.INFO, title="测试通知", body="连通性测试")


class NotificationFactory:
    """按事件载荷拼通知；作业名、错误码、耗时与证据 id 从库取素材补齐。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._runs = JobRunRepository(connection)
        self._templates = JobTemplateRepository(connection)
        self._evidence = EvidenceRepository(connection)

    def run_end(self, run_id: str, payload: Mapping[str, Any]) -> Notification | None:
        """运行终态通知；cancelled 不推。"""
        end = _RUN_ENDS.get(str(payload.get("status", "")))
        if end is None:
            return None
        level, title_prefix, conclusion = end
        run = self._runs.get(run_id)
        job_name = self._job_name(run)
        error_code = run.error_code if run is not None else None
        duration = _format_duration(payload.get("duration_ms")) or _record_duration(run)
        lines = [f"结论：{conclusion}", f"作业：{job_name}", f"运行：{run_id}"]
        if duration:
            lines.append(f"耗时：{duration}")
        if error_code:
            lines.append(f"错误码：{error_code}")
        lines.append(f"证据：evidence/{run_id}")
        return Notification(
            level=level,
            title=f"{title_prefix}：{job_name}",
            body="\n".join(lines),
            evidence_ids=self._evidence_ids(run_id),
        )

    def recovery(self, run_id: str, payload: Mapping[str, Any]) -> Notification | None:
        """恢复停手通知：预算耗尽或动作失败；恢复成功返回 None。"""
        result = str(payload.get("result", ""))
        if result not in ("failed", "exhausted"):
            return None
        action = str(payload.get("action", ""))
        run = self._runs.get(run_id)
        job_name = self._job_name(run)
        error_code = run.error_code if run is not None else None
        reason = "恢复预算耗尽" if result == "exhausted" else "恢复动作失败"
        duration = _record_duration(run)
        lines = [f"结论：{reason}，已停手（动作 {action}）", f"作业：{job_name}", f"运行：{run_id}"]
        if duration:
            lines.append(f"耗时：{duration}")
        if error_code:
            lines.append(f"错误码：{error_code}")
        lines.append(f"证据：evidence/{run_id}")
        return Notification(
            level=NotificationLevel.ERROR,
            title=f"恢复停手：{job_name}",
            body="\n".join(lines),
            evidence_ids=self._evidence_ids(run_id),
        )

    def environment(self, payload: Mapping[str, Any]) -> Notification:
        """环境 failed 通知；探测详情进正文供排障。"""
        environment_id = str(payload.get("environment_id", "未知环境"))
        lines = [f"结论：环境转为 {EnvironmentState.FAILED.value}", f"环境：{environment_id}"]
        probe = payload.get("probe")
        if isinstance(probe, dict) and probe.get("detail"):
            lines.append(f"详情：{probe['detail']}")
        return Notification(
            level=NotificationLevel.ERROR,
            title=f"环境失败：{environment_id}",
            body="\n".join(lines),
        )

    def watchdog(self, payload: Mapping[str, Any]) -> Notification:
        """看门狗停手通知；detail 是停手前的失败摘要。"""
        detail = payload.get("detail") or payload.get("payload") or ""
        if isinstance(detail, Mapping):
            detail = json.dumps(dict(detail), ensure_ascii=False)
        lines = ["结论：核心多次无法拉起，看门狗已停手"]
        if detail:
            lines.append(f"详情：{detail}")
        return Notification(
            level=NotificationLevel.ERROR,
            title="看门狗停手",
            body="\n".join(lines),
        )

    def tailscale(self, payload: Mapping[str, Any]) -> Notification:
        """Tailscale 未运行通知；detail 是 BackendState 或不可查原因。"""
        detail = str(payload.get("detail", ""))
        lines = ["结论：Tailscale 未运行，出网远程访问暂不可用，本机访问不受影响"]
        if detail:
            lines.append(f"详情：{detail}")
        return Notification(
            level=NotificationLevel.WARN,
            title="Tailscale 未运行",
            body="\n".join(lines),
        )

    def _job_name(self, run: JobRunRecord | None) -> str:
        """作业名：模板在用名字，模板已删退回 template_id。"""
        if run is None:
            return "未知作业"
        template = self._templates.get(run.template_id)
        if template is None:
            return run.template_id or "未知作业"
        return template.name or run.template_id

    def _evidence_ids(self, run_id: str) -> tuple[str, ...]:
        """运行已登记的证据 id，最多带 _EVIDENCE_IDS_LIMIT 条。"""
        return tuple(sorted(self._evidence.known_ids(run_id)))[:_EVIDENCE_IDS_LIMIT]


__all__ = ["NotificationFactory", "no_emit", "reject_evidence", "test_notification"]
