"""核心日志文件名文法单源：三处消费方共用同一对象，解析口径一致。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from core import logging as core_logging
from core.hostops import retention, watchdog
from core.log_names import CORE_LOG_ARCHIVED_NAME, CORE_LOG_NAME

RAW_DAY = date(2026, 11, 1)
DAY_TEXT = "2026-11-01"
MATCHED_RAW = ("core-2026-11-01.jsonl", "core-2026-11-01.3.jsonl")
MATCHED_ARCHIVED = ("core-2026-11-01.jsonl.gz", "core-2026-11-01.2.jsonl.gz")
UNMATCHED = ("core.jsonl", "other-2026-11-01.jsonl", "core-2026-11-01.txt")


def test_three_consumers_share_single_pattern_objects() -> None:
    """日志写入、保留清理与看门狗拿到的是 log_names 编译的同一对象。"""
    assert core_logging.CORE_LOG_NAME is CORE_LOG_NAME
    assert retention.CORE_LOG_NAME is CORE_LOG_NAME
    assert watchdog.CORE_LOG_NAME is CORE_LOG_NAME
    assert retention.CORE_LOG_ARCHIVED_NAME is CORE_LOG_ARCHIVED_NAME


def test_raw_pattern_groups_day_and_index() -> None:
    """原始形态按 day 与 index 分组：无续号 index 为 None，续号原样捕获。"""
    match = CORE_LOG_NAME.fullmatch("core-2026-11-01.jsonl")
    assert match is not None
    assert match.group("day") == DAY_TEXT
    assert match.group("index") is None
    match = CORE_LOG_NAME.fullmatch("core-2026-11-01.3.jsonl")
    assert match is not None
    assert match.group("day") == DAY_TEXT
    assert match.group("index") == "3"


def test_archived_pattern_groups_day_and_index() -> None:
    """归档形态多一段 .gz，day 与 index 分组与原始形态一致。"""
    match = CORE_LOG_ARCHIVED_NAME.fullmatch("core-2026-11-01.jsonl.gz")
    assert match is not None
    assert match.group("day") == DAY_TEXT
    assert match.group("index") is None
    match = CORE_LOG_ARCHIVED_NAME.fullmatch("core-2026-11-01.2.jsonl.gz")
    assert match is not None
    assert match.group("day") == DAY_TEXT
    assert match.group("index") == "2"


def test_patterns_reject_non_log_names() -> None:
    """无日期、前缀不符、后缀不符与两形态互相错配都不匹配。"""
    for name in UNMATCHED:
        assert CORE_LOG_NAME.fullmatch(name) is None
        assert CORE_LOG_ARCHIVED_NAME.fullmatch(name) is None
    assert CORE_LOG_NAME.fullmatch("core-2026-11-01.jsonl.gz") is None
    assert CORE_LOG_ARCHIVED_NAME.fullmatch("core-2026-11-01.jsonl") is None


def test_three_consumers_parse_matched_names_consistently(tmp_path: Path) -> None:
    """能匹配的样例在三处解析出同一日期与续号；归档形态只有保留清理认。"""
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    for name in (*MATCHED_RAW, *MATCHED_ARCHIVED):
        (logs_dir / name).write_bytes(b"")
    for name in MATCHED_RAW:
        index = 1 if name == MATCHED_RAW[0] else 3
        assert core_logging._parse_daily_name(name) == (RAW_DAY, index)
        assert watchdog._parse_core_log_name(name) == (DAY_TEXT, index)
    assert retention._log_file(logs_dir / MATCHED_RAW[0]) == (RAW_DAY, False)
    assert retention._log_file(logs_dir / MATCHED_RAW[1]) == (RAW_DAY, False)
    assert retention._log_file(logs_dir / MATCHED_ARCHIVED[0]) == (RAW_DAY, True)
    assert retention._log_file(logs_dir / MATCHED_ARCHIVED[1]) == (RAW_DAY, True)


def test_three_consumers_reject_unmatched_names(tmp_path: Path) -> None:
    """不该匹配的样例在三处都不产生解析结果。"""
    for name in UNMATCHED:
        assert core_logging._parse_daily_name(name) is None
        assert watchdog._parse_core_log_name(name) is None
        assert retention._log_file(tmp_path / name) is None
