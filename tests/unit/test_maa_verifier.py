"""MaaVerifier 的脚本证据判定（docs/plugins/maa.md 验证节）。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import CheckStatus, RunContext, VerdictStatus
from plugins.maa.capabilities.verifier import (
    DEFAULT_POLICY_ID,
    EVIDENCE_LOG_NAME,
    EVIDENCE_NOTE_NAME,
    MaaVerifier,
)
from plugins.maa.logparse import GUI_LOG_NAME
from plugins.maa.session import SessionMarker, write_marker

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
OLD_LINE = "2026-11-07 03:00:00.000 INF 任务已全部完成！"
COMPLETED_LINES = (
    "2026-11-08 03:05:00.000 INF 开始任务: 开始唤醒",
    "2026-11-08 03:05:10.000 INF 完成任务: 开始唤醒",
    "2026-11-08 03:06:00.000 INF 任务已全部完成！",
)


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


def _context(
    maa_dir: Path,
    *,
    work_dir: Path | None = None,
    evidence_dir: Path | None = None,
    config: Mapping[str, Any] | None = None,
) -> RunContext:
    """构造上下文；证据登记回调会核对文件确实落在证据目录里。"""
    work = maa_dir if work_dir is None else work_dir
    evidence = maa_dir / "evidence" if evidence_dir is None else evidence_dir
    plugin_config: Mapping[str, Any] = {"maa_dir": str(maa_dir)} if config is None else config

    def record(kind: str, path: Path, summary: str) -> str:
        target = path if path.is_absolute() else evidence / path
        assert target.is_file(), f"证据文件不存在：{target}"
        return "evidence-1"

    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config=plugin_config,
        work_dir=work,
        evidence_dir=evidence,
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=_Logger(),
        emit=lambda event, payload: None,
        record_evidence=record,
    )


def app_log(maa_dir: Path, lines: tuple[str, ...]) -> None:
    """向 debug/gui.log 追加行；目录不存在时先建。"""
    path = maa_dir / "debug" / GUI_LOG_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        for line in lines:
            stream.write(line + "\n")


def log_size(maa_dir: Path) -> int:
    """gui.log 当前字节数；文件不在按 0 处理。"""
    path = maa_dir / "debug" / GUI_LOG_NAME
    return path.stat().st_size if path.exists() else 0


def mark_run(maa_dir: Path) -> None:
    """按当前日志大小写会话标记，等价于脚本刚启动的时刻。"""
    write_marker(maa_dir, SessionMarker(gui_offset=log_size(maa_dir), asst_offset=0))


def test_completed_when_run_finishes_without_errors(tmp_path: Path) -> None:
    """完成行出现且无失败行：判 completed，证据只含本次运行段。"""
    app_log(tmp_path, (OLD_LINE,))
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.COMPLETED
    assert verdict.policy_id == DEFAULT_POLICY_ID
    assert verdict.confidence > 0
    assert verdict.evidence_ids == ("evidence-1",)
    assert verdict.checks[0].status is CheckStatus.OK
    evidence = (tmp_path / "evidence" / EVIDENCE_LOG_NAME).read_text(encoding="utf-8")
    assert OLD_LINE not in evidence
    assert "任务已全部完成！" in evidence


def test_failed_on_task_error_line(tmp_path: Path) -> None:
    """段内出现任务出错行：判 failed，说明里带出错任务名。"""
    mark_run(tmp_path)
    app_log(
        tmp_path,
        (
            "2026-11-08 03:05:00.000 INF 开始任务: 开始唤醒",
            "2026-11-08 03:06:00.000 ERR 任务出错: 开始唤醒",
        ),
    )
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.FAILED
    assert verdict.checks[0].status is CheckStatus.FAILED
    assert "开始唤醒" in verdict.reason


def test_failed_on_disconnect_line(tmp_path: Path) -> None:
    """游戏掉线行同样判 failed。"""
    mark_run(tmp_path)
    app_log(tmp_path, ("2026-11-08 03:09:00.000 ERR 游戏掉线，停止任务",))
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.FAILED
    assert "掉线 1 次" in verdict.checks[0].detail


def test_failed_on_error_summary_line(tmp_path: Path) -> None:
    """错误汇总行判 failed。"""
    mark_run(tmp_path)
    app_log(tmp_path, ("2026-11-08 03:10:09.000 ERR 任务已结束，以下任务出现错误:",))
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.FAILED


def test_failed_wins_over_completion_line(tmp_path: Path) -> None:
    """单任务出错后 MAA 仍会打印完成行：失败优先，不判完成。"""
    mark_run(tmp_path)
    app_log(
        tmp_path,
        (
            "2026-11-08 03:06:00.000 ERR 任务出错: 理智作战",
            "2026-11-08 03:07:00.000 INF 任务已全部完成！",
        ),
    )
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.FAILED


def test_unknown_without_terminal_signal(tmp_path: Path) -> None:
    """段内只有任务开始、没有终止信号：unknown，证据仍可读。"""
    mark_run(tmp_path)
    app_log(tmp_path, ("2026-11-08 03:05:00.000 INF 开始任务: 开始唤醒",))
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert verdict.confidence == 0.0
    assert "没有终止信号" in verdict.reason
    assert (tmp_path / "evidence" / EVIDENCE_LOG_NAME).is_file()


def test_unknown_when_session_marker_missing(tmp_path: Path) -> None:
    """没有会话标记时不给结论，说明证据写进证据目录。"""
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "缺少会话标记" in verdict.reason
    note = (tmp_path / "evidence" / EVIDENCE_NOTE_NAME).read_text(encoding="utf-8")
    assert "缺少会话标记" in note


def test_unknown_when_log_rolled(tmp_path: Path) -> None:
    """日志滚动后本次运行段不可定位：unknown，不重读整份日志。"""
    old_lines = tuple(f"2026-11-08 03:00:{index:02d}.000 INF 旧行 {index}" for index in range(20))
    app_log(tmp_path, old_lines)
    mark_run(tmp_path)
    log = tmp_path / "debug" / GUI_LOG_NAME
    log.write_text("短日志\n", encoding="utf-8", newline="\n")
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "已滚动" in verdict.reason


def test_unknown_when_log_file_missing(tmp_path: Path) -> None:
    """标记在但日志文件被删：unknown。"""
    mark_run(tmp_path)
    verdict = MaaVerifier().verify(_context(tmp_path))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "日志不存在" in verdict.reason


def test_unknown_when_maa_dir_missing(tmp_path: Path) -> None:
    """插件配置缺 maa_dir 时定位不了日志：unknown。"""
    mark_run(tmp_path)
    verdict = MaaVerifier().verify(_context(tmp_path, config={}))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "maa_dir" in verdict.reason


def test_verdict_is_stable_for_same_segment(tmp_path: Path) -> None:
    """同一份日志段重复验证：结论与判据说明稳定。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    verifier = MaaVerifier()
    first = verifier.verify(_context(tmp_path))
    second = verifier.verify(_context(tmp_path))
    assert first.status is second.status
    assert first.reason == second.reason
    assert first.checks[0].detail == second.checks[0].detail
