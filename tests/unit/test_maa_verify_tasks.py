"""MaaVerifier 的期望任务集核对与理智采样证据（docs/plugins/maa.md 验证节）。

用例只写文件、不碰进程：gui.log / asst.log 都落在 tmp_path 下，会话标记写在
本轮日志追加之前，等价脚本刚启动、任务还没跑的时刻。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import CheckResult, CheckStatus, RunContext, Verdict, VerdictStatus
from plugins.maa.capabilities.verifier import (
    EVIDENCE_LOG_NAME,
    EVIDENCE_SANITY_NAME,
    MaaVerifier,
)
from plugins.maa.logparse import ASST_LOG_NAME, GUI_LOG_NAME, file_size
from plugins.maa.session import SessionMarker, write_marker

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
GUI_ALL_DONE = (
    "2026-11-08 03:05:00.000 INF 开始任务: 开始唤醒",
    "2026-11-08 03:05:10.000 INF 完成任务: 开始唤醒",
    "2026-11-08 03:05:20.000 INF 开始任务: 理智作战",
    "2026-11-08 03:06:00.000 INF 完成任务: 理智作战",
    "2026-11-08 03:06:10.000 INF 任务已全部完成！",
)
GUI_PARTIAL_DONE = (
    "2026-11-08 03:05:00.000 INF 开始任务: 开始唤醒",
    "2026-11-08 03:05:10.000 INF 完成任务: 开始唤醒",
    "2026-11-08 03:06:10.000 INF 任务已全部完成！",
)
GUI_ERROR_THEN_DONE = (
    "2026-11-08 03:05:00.000 INF 开始任务: 理智作战",
    "2026-11-08 03:05:30.000 ERR 任务出错: 理智作战",
    "2026-11-08 03:06:10.000 INF 任务已全部完成！",
)
WAKE_TASK = {"type": "StartUpTask", "name": "开始唤醒"}
FIGHT_TASK = {"type": "FightTask", "name": "理智作战"}
RECRUIT_TASK = {"type": "RecruitTask", "name": "自动招募", "enabled": False}


class _Clock:
    """虚拟时钟：时间恒定，测试不读系统时间。"""

    def monotonic(self) -> float:
        return 0.0

    def utc_now(self) -> datetime:
        return NOW


class _CancelFlag:
    """取消令牌假件：永不取消。"""

    def is_cancelled(self) -> bool:
        return False


class _Logger:
    """结构化日志假件：全部丢弃。"""

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


def _context(maa_dir: Path, *, config: Mapping[str, Any] | None = None) -> RunContext:
    """构造上下文；证据登记回调核对文件确实落在证据目录里。"""
    evidence = maa_dir / "evidence"
    plugin_config: Mapping[str, Any] = {"maa_dir": str(maa_dir)} if config is None else config

    def record(kind: str, path: Path, summary: str) -> str:
        target = path if path.is_absolute() else evidence / path
        assert target.is_file(), f"证据文件不存在：{target}"
        return f"evidence:{path.name}"

    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config=plugin_config,
        work_dir=maa_dir,
        evidence_dir=evidence,
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=_Logger(),
        emit=lambda event, payload: None,
        record_evidence=record,
    )


def _append_log(maa_dir: Path, log_name: str, lines: tuple[str, ...]) -> None:
    """向 debug/ 下的指定日志追加行；目录不存在时先建。"""
    path = maa_dir / "debug" / log_name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        for line in lines:
            stream.write(line + "\n")


def _mark_run(maa_dir: Path) -> None:
    """按当前两份日志的大小写会话标记，等价脚本刚启动的时刻。"""
    log_dir = maa_dir / "debug"
    write_marker(
        maa_dir,
        SessionMarker(
            gui_offset=file_size(log_dir / GUI_LOG_NAME),
            asst_offset=file_size(log_dir / ASST_LOG_NAME),
        ),
    )


def _tasks_config(maa_dir: Path, *tasks: dict[str, Any]) -> Mapping[str, Any]:
    """带任务队列的插件配置；任务项只含 schema 必需的字段。"""
    return {"maa_dir": str(maa_dir), "tasks": list(tasks)}


def _task_check(verdict: Verdict) -> CheckResult:
    """按名字取 maa-task-set 检查项；缺失或重复即测试失败。"""
    matches = [check for check in verdict.checks if check.name == "maa-task-set"]
    assert len(matches) == 1, f"应恰有一条 maa-task-set 检查项：{verdict.checks}"
    return matches[0]


def _sanity_line(seconds: int, current: int) -> str:
    """一条 asst.log 理智采样行：嵌套形态由 MAA 回调 JSON 决定。"""
    details = json.dumps({"data": {"current_sanity": current, "max_sanity": 130}})
    return f"[2026-11-08 03:06:{seconds:02d}.000][INF] SubTaskExtraInfo SanityBeforeStage {details}"


def _asst_lines(*sanity_values: int) -> tuple[str, ...]:
    """把理智数值折成本轮 asst 段的行，行首夹一条任务链噪音行。"""
    lines = ['[2026-11-08 03:05:59.000][INF] TaskChainStart | {"taskchain": "Fight"}']
    for index, value in enumerate(sanity_values):
        lines.append(_sanity_line(index, value))
    return tuple(lines)


def test_completed_requires_all_expected_tasks(tmp_path: Path) -> None:
    """完成行加全部期望任务完成：completed，任务集检查 ok；停用任务不参与核对。"""
    _mark_run(tmp_path)
    _append_log(tmp_path, GUI_LOG_NAME, GUI_ALL_DONE)
    config = _tasks_config(tmp_path, WAKE_TASK, FIGHT_TASK, RECRUIT_TASK)
    verdict = MaaVerifier().verify(_context(tmp_path, config=config))
    assert verdict.status is VerdictStatus.COMPLETED
    check = _task_check(verdict)
    assert check.status is CheckStatus.OK
    assert "期望任务 2 个全部完成" in check.detail


def test_missing_expected_task_blocks_completed(tmp_path: Path) -> None:
    """缺一个期望任务：结论压成 unknown，任务集检查 failed 并点名缺失任务。"""
    _mark_run(tmp_path)
    _append_log(tmp_path, GUI_LOG_NAME, GUI_PARTIAL_DONE)
    config = _tasks_config(tmp_path, WAKE_TASK, FIGHT_TASK)
    verdict = MaaVerifier().verify(_context(tmp_path, config=config))
    assert verdict.status is VerdictStatus.UNKNOWN
    check = _task_check(verdict)
    assert check.status is CheckStatus.FAILED
    assert "理智作战" in check.detail
    assert verdict.confidence == 0.0


def test_task_error_outranks_missing_tasks(tmp_path: Path) -> None:
    """出错行加完成行加缺任务：失败优先，结论仍为 failed。"""
    _mark_run(tmp_path)
    _append_log(tmp_path, GUI_LOG_NAME, GUI_ERROR_THEN_DONE)
    config = _tasks_config(tmp_path, WAKE_TASK, FIGHT_TASK)
    verdict = MaaVerifier().verify(_context(tmp_path, config=config))
    assert verdict.status is VerdictStatus.FAILED
    check = _task_check(verdict)
    assert check.status is CheckStatus.FAILED
    assert "理智作战" in check.detail


def test_missing_tasks_config_keeps_legacy_completed(tmp_path: Path) -> None:
    """配置没有 tasks：维持只看完成行与失败行的口径，任务集检查记 unknown。"""
    _mark_run(tmp_path)
    _append_log(tmp_path, GUI_LOG_NAME, GUI_ALL_DONE)
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.COMPLETED
    check = _task_check(verdict)
    assert check.status is CheckStatus.UNKNOWN
    assert check.detail == "插件配置没有期望任务集，未核对"


def test_sanity_samples_written_as_evidence(tmp_path: Path) -> None:
    """本轮 asst 段有理智采样：maa-sanity.json 落盘并登记，标记前的采样不混入。"""
    _append_log(tmp_path, ASST_LOG_NAME, _asst_lines(130))
    _mark_run(tmp_path)
    _append_log(tmp_path, GUI_LOG_NAME, GUI_ALL_DONE)
    _append_log(tmp_path, ASST_LOG_NAME, _asst_lines(100, 97))
    config = _tasks_config(tmp_path, WAKE_TASK, FIGHT_TASK)
    verdict = MaaVerifier().verify(_context(tmp_path, config=config))
    assert verdict.status is VerdictStatus.COMPLETED
    sanity = json.loads((tmp_path / "evidence" / EVIDENCE_SANITY_NAME).read_text(encoding="utf-8"))
    assert sanity == {
        "sample_count": 2,
        "first_current_sanity": 100,
        "last_current_sanity": 97,
        "delta": -3,
    }
    assert f"evidence:{EVIDENCE_SANITY_NAME}" in verdict.evidence_ids
    assert f"evidence:{EVIDENCE_LOG_NAME}" in verdict.evidence_ids


def test_no_asst_log_skips_sanity_evidence(tmp_path: Path) -> None:
    """没有 asst.log：不写理智证据，结论按 gui 段照常给出。"""
    _mark_run(tmp_path)
    _append_log(tmp_path, GUI_LOG_NAME, GUI_ALL_DONE)
    config = _tasks_config(tmp_path, WAKE_TASK, FIGHT_TASK)
    verdict = MaaVerifier().verify(_context(tmp_path, config=config))
    assert verdict.status is VerdictStatus.COMPLETED
    assert not (tmp_path / "evidence" / EVIDENCE_SANITY_NAME).exists()
    assert f"evidence:{EVIDENCE_SANITY_NAME}" not in verdict.evidence_ids


def test_sanity_evidence_skipped_when_asst_has_no_samples(tmp_path: Path) -> None:
    """asst 段没有理智采样：同样不写理智证据。"""
    _mark_run(tmp_path)
    _append_log(tmp_path, GUI_LOG_NAME, GUI_ALL_DONE)
    _append_log(
        tmp_path,
        ASST_LOG_NAME,
        ('[2026-11-08 03:05:59.000][INF] TaskChainStart | {"taskchain": "Fight"}',),
    )
    config = _tasks_config(tmp_path, WAKE_TASK, FIGHT_TASK)
    verdict = MaaVerifier().verify(_context(tmp_path, config=config))
    assert verdict.status is VerdictStatus.COMPLETED
    assert not (tmp_path / "evidence" / EVIDENCE_SANITY_NAME).exists()
    assert f"evidence:{EVIDENCE_SANITY_NAME}" not in verdict.evidence_ids
