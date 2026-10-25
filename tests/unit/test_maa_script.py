"""MaaScript 日志观测的单元测试（docs/plugins/maa.md 脚本节）。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import RunContext, ScriptRef, ScriptState, Signal, StopMode
from plugins.maa.capabilities.script import MaaScript

NOW = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)


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
    """结构化日志假件：记录 info 事件，其余级别丢弃。"""

    def __init__(self) -> None:
        self.events: list[Mapping[str, Any]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        self.events.append(fields)

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


def make_context(maa_dir: Path, logger: _RecordingLogger | None = None) -> RunContext:
    """构造指向临时 MAA 目录的上下文；日志假件可注入。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={"maa_dir": str(maa_dir)},
        work_dir=maa_dir,
        evidence_dir=maa_dir,
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=logger if logger is not None else _RecordingLogger(),
        emit=lambda event, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


def append_log(maa_dir: Path, name: str, text: str) -> None:
    """向 debug/ 下的日志追加内容；目录不存在时先建。"""
    debug_dir = maa_dir / "debug"
    debug_dir.mkdir(exist_ok=True)
    with (debug_dir / name).open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(text)


def kinds(signals: tuple[Signal, ...]) -> list[str]:
    return [signal.kind for signal in signals]


def test_launch_records_log_dir_and_start_offsets(tmp_path: Path) -> None:
    """launch 不启动进程：meta 记录日志目录与两份日志的当前字节数。"""
    append_log(tmp_path, "gui.log", "旧的一行\n")
    append_log(tmp_path, "asst.log", "旧的一条\n")
    handle = MaaScript().launch(ScriptRef(id="script-1", plugin_id="maa"), make_context(tmp_path))
    assert handle.pid is None
    assert handle.meta["launched"] is False
    assert handle.meta["log_dir"] == str(tmp_path / "debug")
    assert handle.meta["gui_log_offset"] == len("旧的一行\n".encode())
    assert handle.meta["asst_log_offset"] == len("旧的一条\n".encode())
    assert handle.meta["launched_at"] == NOW.isoformat()


def test_observe_reports_running_while_queue_in_progress(tmp_path: Path) -> None:
    """任务开始而未见终态行：状态 RUNNING，信号逐条给出。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(tmp_path, "gui.log", "2026-10-20 03:07:20.000 INF 开始任务: 开始唤醒\n")
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.RUNNING
    assert kinds(observation.signals) == ["task.started"]
    assert observation.last_signal is not None
    assert observation.last_signal.kind == "task.started"
    assert observation.since == NOW


def test_observe_exits_when_queue_completes(tmp_path: Path) -> None:
    """出现「任务已全部完成！」：状态转 EXITED，完成信号在列。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(
        tmp_path,
        "gui.log",
        "2026-10-20 03:07:20.000 INF 开始任务: 开始唤醒\n"
        "2026-10-20 03:10:09.000 INF 任务已全部完成！\n",
    )
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.EXITED
    assert kinds(observation.signals) == ["task.started", "run.completed"]
    assert observation.last_signal is not None
    assert observation.last_signal.kind == "run.completed"


def test_observe_keeps_running_on_single_error_until_summary(tmp_path: Path) -> None:
    """单任务出错不终轮：状态保持 RUNNING，错误汇总行才转 EXITED。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(
        tmp_path,
        "gui.log",
        "2026-10-20 03:07:20.000 INF 开始任务: 开始唤醒\n"
        "2026-10-20 03:08:26.500 WRN 任务出错: 开始唤醒\n"
        "2026-10-20 03:08:27.000 INF 开始任务: 日常任务\n",
    )
    first = adapter.observe(handle, ctx)
    assert first.state is ScriptState.RUNNING
    assert "task.error" in kinds(first.signals)
    append_log(tmp_path, "gui.log", "2026-10-20 03:10:09.000 ERR 任务已结束，以下任务出现错误:\n")
    second = adapter.observe(handle, ctx)
    assert second.state is ScriptState.EXITED
    assert kinds(second.signals) == ["run.failed"]
    assert second.last_signal is not None
    assert second.last_signal.kind == "run.failed"


def test_observe_exits_on_game_disconnect(tmp_path: Path) -> None:
    """「游戏掉线，停止任务」终轮：状态 EXITED，掉线信号在列。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(tmp_path, "gui.log", "2026-10-20 03:09:00.000 ERR 游戏掉线，停止任务\n")
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.EXITED
    assert kinds(observation.signals) == ["run.disconnected"]


def test_observe_collects_sanity_sample_from_asst_log(tmp_path: Path) -> None:
    """asst.log 的 SanityBeforeStage 采样进入信号，不影响运行状态。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    line = (
        "[2026-10-20 03:08:30.123][INF][Px1f0][Tx6e00]"
        '{"what":"SubTaskExtraInfo","details":{"target":"SanityBeforeStage",'
        '"current_sanity":82,"max_sanity":135}}'
    )
    append_log(tmp_path, "asst.log", f"{line}\n")
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.RUNNING
    assert kinds(observation.signals) == ["sanity.sample"]
    assert observation.signals[0].payload == {
        "current_sanity": 82,
        "max_sanity": 135,
        "line": line,
    }


def test_observe_merges_signals_across_logs_by_time(tmp_path: Path) -> None:
    """两份日志的新增信号按时间归并输出。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(tmp_path, "gui.log", "2026-10-20 03:08:00.000 INF 完成任务: 开始唤醒\n")
    append_log(
        tmp_path,
        "asst.log",
        "[2026-10-20 03:07:30.000][INF][Px1f0][Tx6e00]"
        '{"what":"SubTaskExtraInfo","details":{"target":"SanityBeforeStage",'
        '"current_sanity":82,"max_sanity":135}}\n',
    )
    observation = adapter.observe(handle, ctx)
    assert kinds(observation.signals) == ["sanity.sample", "task.completed"]


def test_observe_returns_only_new_signals_and_is_repeatable(tmp_path: Path) -> None:
    """重复 observe 幂等：无新增时信号为空，状态与最近信号保持。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(tmp_path, "gui.log", "2026-10-20 03:07:20.000 INF 开始任务: 开始唤醒\n")
    first = adapter.observe(handle, ctx)
    assert kinds(first.signals) == ["task.started"]
    second = adapter.observe(handle, ctx)
    assert second.state is first.state is ScriptState.RUNNING
    assert second.signals == ()
    assert second.last_signal == first.last_signal


def test_stop_is_idempotent_and_finalizes_observation(tmp_path: Path) -> None:
    """stop 可重复且成功；停止后 observe 报 EXITED。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(tmp_path, "gui.log", "2026-10-20 03:07:20.000 INF 开始任务: 开始唤醒\n")
    assert adapter.observe(handle, ctx).state is ScriptState.RUNNING
    assert adapter.stop(handle, ctx, StopMode.GRACEFUL).ok is True
    assert adapter.stop(handle, ctx, StopMode.FORCE).ok is True
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.EXITED
    assert observation.signals == ()


def test_observe_counts_unknown_lines_and_logs_total(tmp_path: Path) -> None:
    """未知行忽略但计数：非空未知行累计并写一条日志，空行不计。"""
    logger = _RecordingLogger()
    adapter = MaaScript()
    ctx = make_context(tmp_path, logger)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(
        tmp_path,
        "gui.log",
        "2026-10-20 03:07:14.512 INF 正在连接模拟器……\n"
        "\n"
        "2026-10-20 03:07:15.000 INF 最快截图耗时: 7ms (MumuExtras)\n",
    )
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.RUNNING
    assert observation.signals == ()
    assert logger.events == [{"event": "maa.log.unknown", "count": 2}]
    adapter.observe(handle, ctx)
    assert len(logger.events) == 1


def test_observe_rereads_log_after_rollover(tmp_path: Path) -> None:
    """日志滚动（文件变短）后从头重读新文件，并记录一次 rolled 事件。"""
    logger = _RecordingLogger()
    adapter = MaaScript()
    ctx = make_context(tmp_path, logger)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(
        tmp_path,
        "gui.log",
        "2026-10-20 03:07:20.000 INF 开始任务: 开始唤醒\n"
        "2026-10-20 03:08:00.000 INF 完成任务: 开始唤醒\n",
    )
    assert kinds(adapter.observe(handle, ctx).signals) == ["task.started", "task.completed"]
    (tmp_path / "debug" / "gui.log").write_text(
        "2026-10-21 03:07:20 INF 任务已全部完成！\n", encoding="utf-8", newline="\n"
    )
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.EXITED
    assert kinds(observation.signals) == ["run.completed"]
    assert {"event": "maa.log.rolled", "file": "gui.log"} in logger.events


def test_observe_keeps_terminal_state_on_late_signals(tmp_path: Path) -> None:
    """终态后到达的信号照常给出，但不再翻转状态。"""
    adapter = MaaScript()
    ctx = make_context(tmp_path)
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="maa"), ctx)
    append_log(tmp_path, "gui.log", "2026-10-20 03:10:09.000 INF 任务已全部完成！\n")
    assert adapter.observe(handle, ctx).state is ScriptState.EXITED
    append_log(tmp_path, "gui.log", "2026-10-20 03:10:10.000 ERR 游戏掉线，停止任务\n")
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.EXITED
    assert kinds(observation.signals) == ["run.disconnected"]
