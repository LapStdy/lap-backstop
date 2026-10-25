"""MAA 日志解析与增量读取的单元测试（docs/arknights/02 日志信号表）。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from plugins.maa.logparse import (
    KIND_RUN_COMPLETED,
    KIND_RUN_DISCONNECTED,
    KIND_RUN_FAILED,
    KIND_SANITY_SAMPLE,
    KIND_TASK_COMPLETED,
    KIND_TASK_ERROR,
    KIND_TASK_STARTED,
    LogChunk,
    file_size,
    parse_asst_line,
    parse_gui_line,
    read_new_lines,
)

ASST_SANITY_LINE = (
    "[2026-10-20 03:08:30.123][INF][Px1f0][Tx6e00]"
    '{"what":"SubTaskExtraInfo","details":{"target":"SanityBeforeStage",'
    '"current_sanity":82,"max_sanity":135}}'
)


def local_time(iso: str) -> datetime:
    """构造与解析器同口径的时间：日志本地墙钟时间补宿主时区。"""
    return datetime.fromisoformat(iso).astimezone()


def test_parse_gui_line_yields_task_signals_with_task_name() -> None:
    """开始/完成/出错三类任务行：名称、任务参数、时间与原始行齐备。"""
    cases = [
        (
            "2026-10-20 03:07:20.123 INF 开始任务: 开始唤醒",
            KIND_TASK_STARTED,
            "开始唤醒",
            "2026-10-20T03:07:20.123000",
        ),
        (
            "2026-10-20 03:08:00.001 INF 完成任务: 日常任务",
            KIND_TASK_COMPLETED,
            "日常任务",
            "2026-10-20T03:08:00.001000",
        ),
        (
            "2026-10-20 03:08:26.500 WRN 任务出错: 开始唤醒",
            KIND_TASK_ERROR,
            "开始唤醒",
            "2026-10-20T03:08:26.500000",
        ),
    ]
    for line, kind, task, iso in cases:
        signal = parse_gui_line(line)
        assert signal is not None, line
        assert signal.kind == kind
        assert signal.payload == {"task": task}
        assert signal.at == local_time(iso)
        assert signal.line == line


def test_parse_gui_line_yields_run_terminal_signals() -> None:
    """整轮完成、错误汇总与掉线：识别为对应终态信号，不带任务参数。"""
    cases = [
        ("2026-10-20 03:10:09.000 INF 任务已全部完成！", KIND_RUN_COMPLETED),
        ("2026-10-20 03:10:09.000 ERR 任务已结束，以下任务出现错误:", KIND_RUN_FAILED),
        ("2026-10-20 03:09:00.000 ERR 游戏掉线，停止任务", KIND_RUN_DISCONNECTED),
    ]
    for line, kind in cases:
        signal = parse_gui_line(line)
        assert signal is not None, line
        assert signal.kind == kind
        assert signal.payload == {}
        assert signal.at is not None


def test_parse_gui_line_without_timestamp_keeps_signal_with_no_time() -> None:
    """缺时间戳的行仍给出信号；时间留空由调用方补观察时刻。"""
    signal = parse_gui_line("任务已全部完成！")
    assert signal is not None
    assert signal.kind == KIND_RUN_COMPLETED
    assert signal.at is None


def test_parse_gui_line_ignores_unrelated_lines() -> None:
    """连接、截图与版本行不属于本层信号，返回 None 交调用方计数。"""
    for line in (
        "2026-10-20 03:07:14.512 INF 正在连接模拟器……",
        "2026-10-20 03:07:15.000 INF 最快截图耗时: 7ms (MumuExtras)",
        "2026-10-20 03:05:52.000 INF MaaAssistantArknights GUI started",
        "2026-10-20 03:05:52.000 INF Version v6.20.1",
        "",
    ):
        assert parse_gui_line(line) is None, line


def test_parse_asst_line_yields_sanity_sample() -> None:
    """SubTaskExtraInfo 携带 SanityBeforeStage：提取理智上下限与行首时间。"""
    signal = parse_asst_line(ASST_SANITY_LINE)
    assert signal is not None
    assert signal.kind == KIND_SANITY_SAMPLE
    assert signal.payload == {"current_sanity": 82, "max_sanity": 135}
    assert signal.at == local_time("2026-10-20T03:08:30.123000")
    assert signal.line == ASST_SANITY_LINE


def test_parse_asst_line_collects_sanity_from_any_nesting() -> None:
    """理智数值嵌在 SanityBeforeStage 子对象里时同样收齐两项。"""
    line = (
        "[2026-10-20 03:08:31.000][INF][Px1f0][Tx6e00]"
        '{"details":{"what":"SubTaskExtraInfo","SanityBeforeStage":'
        '{"current_sanity":130,"max_sanity":135}}}'
    )
    signal = parse_asst_line(line)
    assert signal is not None
    assert signal.payload == {"current_sanity": 130, "max_sanity": 135}


def test_parse_asst_line_ignores_other_callbacks_and_partial_samples() -> None:
    """任务链与 AllTasksCompleted 不作完成依据；缺一项数值不算采样。"""
    for line in (
        "[2026-10-20 03:10:09.000][INF][Px1f0][Tx6e00]"
        '{"what":"AllTasksCompleted","details":{"finished_tasks":[1,2]}}',
        '[2026-10-20 03:10:08.000][INF][Px1f0][Tx6e00]{"what":"TaskChainStart"}',
        "[2026-10-20 03:08:30.123][INF][Px1f0][Tx6e00]"
        '{"what":"SubTaskExtraInfo","details":{"target":"SanityBeforeStage",'
        '"current_sanity":82}}',
        "2026-10-20 03:08:30.123 SanityBeforeStage {broken json",
    ):
        assert parse_asst_line(line) is None, line


def test_read_new_lines_returns_only_complete_lines(tmp_path: Path) -> None:
    """半行不消费，写完后再读；偏移按字节推进，中文不截断。"""
    log = tmp_path / "gui.log"
    log.write_bytes("第一行\n第二行\n半".encode())
    first = read_new_lines(log, 0)
    assert first.lines == ("第一行", "第二行")
    assert first.truncated is False
    with log.open("ab") as stream:
        stream.write("行\n".encode())
    second = read_new_lines(log, first.offset)
    assert second.lines == ("半行",)
    assert second.offset == log.stat().st_size


def test_read_new_lines_strips_carriage_returns(tmp_path: Path) -> None:
    """CRLF 行尾的回车不属于信号内容，读出的行可直接解析。"""
    log = tmp_path / "gui.log"
    log.write_bytes("2026-10-20 03:07:20.123 INF 开始任务: 开始唤醒\r\n".encode())
    chunk = read_new_lines(log, 0)
    assert chunk.lines == ("2026-10-20 03:07:20.123 INF 开始任务: 开始唤醒",)
    assert parse_gui_line(chunk.lines[0]) is not None


def test_read_new_lines_treats_missing_file_as_empty(tmp_path: Path) -> None:
    """日志尚未创建时读不到新行，偏移保持不变。"""
    chunk = read_new_lines(tmp_path / "asst.log", 0)
    assert chunk == LogChunk((), 0, truncated=False)


def test_read_new_lines_returns_empty_when_unchanged(tmp_path: Path) -> None:
    """文件无新增时返回空行集，偏移停在原地。"""
    log = tmp_path / "gui.log"
    log.write_bytes("一行\n".encode())
    first = read_new_lines(log, 0)
    second = read_new_lines(log, first.offset)
    assert second == LogChunk((), first.offset, truncated=False)


def test_read_new_lines_rereads_after_truncation_or_rollover(tmp_path: Path) -> None:
    """文件变短（截断或滚动）视为新文件：偏移归零，从头重读并标记。"""
    log = tmp_path / "gui.log"
    log.write_bytes("旧内容很长很长\n旧第二行\n".encode())
    first = read_new_lines(log, 0)
    log.write_bytes("新文件首行\n".encode())
    second = read_new_lines(log, first.offset)
    assert second.truncated is True
    assert second.lines == ("新文件首行",)
    assert second.offset == len("新文件首行\n".encode())


def test_file_size_is_zero_for_missing_file(tmp_path: Path) -> None:
    """文件不存在按 0 字节处理，launch 可直接当起始偏移。"""
    missing = tmp_path / "gui.log"
    assert file_size(missing) == 0
    missing.write_bytes(b"abc")
    assert file_size(missing) == 3
