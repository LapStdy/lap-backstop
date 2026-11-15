"""MaaVerifier 的 verify_mode 三档行为（docs/plugins/maa.md 验证节）。"""

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
COMPLETED_LINES = (
    "2026-11-08 03:05:00.000 INF 开始任务: 开始唤醒",
    "2026-11-08 03:05:10.000 INF 完成任务: 开始唤醒",
    "2026-11-08 03:06:00.000 INF 任务已全部完成！",
)
ERROR_THEN_DONE = (
    "2026-11-08 03:05:00.000 INF 开始任务: 理智作战",
    "2026-11-08 03:05:30.000 ERR 任务出错: 理智作战",
    "2026-11-08 03:06:10.000 INF 任务已全部完成！",
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


class _RecordingLogger:
    """记录 warn 的日志假件：警告存进列表供断言，其余级别丢弃。"""

    def __init__(self) -> None:
        self.warnings: list[Mapping[str, Any]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        self.warnings.append(dict(fields))

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


def _context(
    maa_dir: Path,
    *,
    config_extra: Mapping[str, Any] | None = None,
    logger: _RecordingLogger | None = None,
    verification_policy: Mapping[str, Any] | None = None,
) -> RunContext:
    """构造上下文；证据登记回调会核对文件确实落在证据目录里。"""
    evidence = maa_dir / "evidence"
    config: dict[str, Any] = {"maa_dir": str(maa_dir)}
    if config_extra is not None:
        config.update(config_extra)

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
        config=config,
        work_dir=maa_dir,
        evidence_dir=evidence,
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=_RecordingLogger() if logger is None else logger,
        emit=lambda event, payload: None,
        record_evidence=record,
        verification_policy={} if verification_policy is None else verification_policy,
    )


def app_log(maa_dir: Path, lines: tuple[str, ...]) -> None:
    """向 debug/gui.log 追加行；目录不存在时先建。"""
    path = maa_dir / "debug" / GUI_LOG_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        for line in lines:
            stream.write(line + "\n")


def mark_run(maa_dir: Path) -> None:
    """按当前日志大小写会话标记，等价于脚本刚启动的时刻。"""
    path = maa_dir / "debug" / GUI_LOG_NAME
    offset = path.stat().st_size if path.exists() else 0
    write_marker(maa_dir, SessionMarker(gui_offset=offset, asst_offset=0))


def test_screen_mode_returns_unknown_without_reading_log(tmp_path: Path) -> None:
    """screen 档不读脚本日志：段里有完成行也记 unknown，只留画面检查项与说明证据。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    verdict = MaaVerifier().verify(_context(tmp_path, config_extra={"verify_mode": "screen"}))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "画面证据未接入" in verdict.reason
    assert verdict.policy_id == DEFAULT_POLICY_ID
    assert verdict.confidence == 0.0
    assert [check.name for check in verdict.checks] == ["maa-screen"]
    assert verdict.checks[0].status is CheckStatus.UNKNOWN
    assert (tmp_path / "evidence" / EVIDENCE_NOTE_NAME).is_file()
    assert not (tmp_path / "evidence" / EVIDENCE_LOG_NAME).exists()


def test_dual_mode_never_completes_without_screen_evidence(tmp_path: Path) -> None:
    """dual 档：脚本完成而画面证据缺失，结论压成 unknown 而不是 completed。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    verdict = MaaVerifier().verify(_context(tmp_path, config_extra={"verify_mode": "dual"}))
    assert verdict.status is not VerdictStatus.COMPLETED
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "画面证据缺失" in verdict.reason
    assert verdict.confidence == 0.0
    assert (tmp_path / "evidence" / EVIDENCE_LOG_NAME).is_file()


def test_dual_mode_keeps_failure_priority(tmp_path: Path) -> None:
    """dual 档：脚本失败行存在时失败优先，不因画面缺失降成 unknown。"""
    mark_run(tmp_path)
    app_log(tmp_path, ERROR_THEN_DONE)
    verdict = MaaVerifier().verify(_context(tmp_path, config_extra={"verify_mode": "dual"}))
    assert verdict.status is VerdictStatus.FAILED
    assert "脚本证据判定失败" in verdict.reason


def test_dual_mode_passes_script_unknown_through(tmp_path: Path) -> None:
    """dual 档：脚本侧本就 unknown 时原样保留，理由不混入画面证据缺失。"""
    verdict = MaaVerifier().verify(_context(tmp_path, config_extra={"verify_mode": "dual"}))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "缺少会话标记" in verdict.reason
    assert "画面证据缺失" not in verdict.reason


def test_invalid_mode_falls_back_to_script_with_warn_log(tmp_path: Path) -> None:
    """verify_mode 非法取值：按 script 档判定并记 warn，不抛异常。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    logger = _RecordingLogger()
    verdict = MaaVerifier().verify(
        _context(tmp_path, config_extra={"verify_mode": "weird"}, logger=logger)
    )
    assert verdict.status is VerdictStatus.COMPLETED
    assert verdict.policy_id == DEFAULT_POLICY_ID
    assert [check.name for check in verdict.checks] == ["maa-script-log", "maa-task-set"]
    assert len(logger.warnings) == 1
    assert logger.warnings[0]["event"] == "maa.verify_mode.invalid"
    assert logger.warnings[0]["value"] == "weird"


def test_non_string_mode_falls_back_to_script_with_warn_log(tmp_path: Path) -> None:
    """verify_mode 不是字符串：同样按 script 档判定并记 warn。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    logger = _RecordingLogger()
    verdict = MaaVerifier().verify(
        _context(tmp_path, config_extra={"verify_mode": 42}, logger=logger)
    )
    assert verdict.status is VerdictStatus.COMPLETED
    assert len(logger.warnings) == 1
    assert logger.warnings[0]["event"] == "maa.verify_mode.invalid"
    assert logger.warnings[0]["value"] == 42


def test_policy_mode_screen_overrides_config_script(tmp_path: Path) -> None:
    """模板 params.mode=screen 且合法时以模板为准：配置 script 不再生效。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    policy: dict[str, Any] = {"params": {"mode": "screen"}}
    verdict = MaaVerifier().verify(
        _context(tmp_path, config_extra={"verify_mode": "script"}, verification_policy=policy)
    )
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "画面证据未接入" in verdict.reason
    assert [check.name for check in verdict.checks] == ["maa-screen"]
    assert verdict.policy_id == DEFAULT_POLICY_ID
    assert (tmp_path / "evidence" / EVIDENCE_NOTE_NAME).is_file()
    assert not (tmp_path / "evidence" / EVIDENCE_LOG_NAME).exists()


def test_policy_mode_absent_falls_back_to_config_dual(tmp_path: Path) -> None:
    """模板缺 params.mode 时回退配置：dual 档行为与配置直选一致。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    policy: dict[str, Any] = {"id": "policy-tpl", "params": {}}
    verdict = MaaVerifier().verify(
        _context(tmp_path, config_extra={"verify_mode": "dual"}, verification_policy=policy)
    )
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "画面证据缺失" in verdict.reason
    assert verdict.confidence == 0.0
    assert [check.name for check in verdict.checks] == [
        "maa-script-log",
        "maa-task-set",
        "maa-screen",
    ]
    assert (tmp_path / "evidence" / EVIDENCE_LOG_NAME).is_file()


def test_policy_mode_invalid_falls_back_to_config(tmp_path: Path) -> None:
    """模板 params.mode 非法按缺失处理：回退配置行为且不记事件，不抛错。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    logger = _RecordingLogger()
    policy: dict[str, Any] = {"params": {"mode": "weird"}}
    verdict = MaaVerifier().verify(
        _context(
            tmp_path,
            config_extra={"verify_mode": "dual"},
            logger=logger,
            verification_policy=policy,
        )
    )
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "画面证据缺失" in verdict.reason
    assert logger.warnings == []


def test_policy_mode_invalid_and_config_invalid_falls_back_to_script(tmp_path: Path) -> None:
    """模板档位非法且回退到的配置也非法：按 script 判定并记配置侧 warn。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    logger = _RecordingLogger()
    policy: dict[str, Any] = {"params": {"mode": "weird"}}
    verdict = MaaVerifier().verify(
        _context(
            tmp_path,
            config_extra={"verify_mode": "also-weird"},
            logger=logger,
            verification_policy=policy,
        )
    )
    assert verdict.status is VerdictStatus.COMPLETED
    assert len(logger.warnings) == 1
    assert logger.warnings[0]["event"] == "maa.verify_mode.invalid"
    assert logger.warnings[0]["value"] == "also-weird"


def test_dual_policy_mode_never_completes_without_screen_evidence(tmp_path: Path) -> None:
    """dual 经模板档位时同样不判完成：检查项含脚本侧结论与 maa-screen unknown。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    policy: dict[str, Any] = {"params": {"mode": "dual"}}
    verdict = MaaVerifier().verify(_context(tmp_path, verification_policy=policy))
    assert verdict.status is not VerdictStatus.COMPLETED
    assert verdict.status is VerdictStatus.UNKNOWN
    assert "画面证据缺失" in verdict.reason
    assert verdict.confidence == 0.0
    assert [check.name for check in verdict.checks] == [
        "maa-script-log",
        "maa-task-set",
        "maa-screen",
    ]
    assert verdict.checks[-1].status is CheckStatus.UNKNOWN
    assert (tmp_path / "evidence" / EVIDENCE_LOG_NAME).is_file()


def test_dual_policy_mode_keeps_script_failure(tmp_path: Path) -> None:
    """dual 经模板档位时脚本失败仍判 failed：失败优先，理由不混入画面缺失。"""
    mark_run(tmp_path)
    app_log(tmp_path, ERROR_THEN_DONE)
    policy: dict[str, Any] = {"params": {"mode": "dual"}}
    verdict = MaaVerifier().verify(_context(tmp_path, verification_policy=policy))
    assert verdict.status is VerdictStatus.FAILED
    assert "脚本证据判定失败" in verdict.reason
    assert "画面证据缺失" not in verdict.reason


def test_policy_id_fills_verdict_from_template(tmp_path: Path) -> None:
    """模板策略带 id 时回填 Verdict.policy_id，替换写死的缺省值。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    policy: dict[str, Any] = {"id": "policy-x", "params": {"mode": "script"}}
    verdict = MaaVerifier().verify(_context(tmp_path, verification_policy=policy))
    assert verdict.status is VerdictStatus.COMPLETED
    assert verdict.policy_id == "policy-x"
    assert [check.name for check in verdict.checks] == ["maa-script-log", "maa-task-set"]


def test_policy_id_fills_unknown_verdict_from_template(tmp_path: Path) -> None:
    """unknown 结论同样回填模板策略 id：screen 档与缺证据收口共用该口径。"""
    mark_run(tmp_path)
    app_log(tmp_path, COMPLETED_LINES)
    policy: dict[str, Any] = {"id": "policy-x", "params": {"mode": "screen"}}
    verdict = MaaVerifier().verify(_context(tmp_path, verification_policy=policy))
    assert verdict.status is VerdictStatus.UNKNOWN
    assert verdict.policy_id == "policy-x"
