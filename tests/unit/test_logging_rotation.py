"""日志按 UTC 日界轮转：按天分文件、满额续号、重启续写与旧文件压缩。"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from core import logging as logging_module
from core.clock import VirtualClock
from core.logging import (
    COMPRESS_AFTER_DAYS,
    MAX_FILE_BYTES,
    DailyRotatingJsonLineLogger,
)

NOW = datetime(2026, 10, 30, 9, 0, tzinfo=UTC)


def _read_lines(path: Path) -> list[dict[str, object]]:
    text = path.read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line]


def _events(path: Path) -> list[object]:
    return [row["event"] for row in _read_lines(path)]


def test_default_constants_match_host_limits() -> None:
    assert MAX_FILE_BYTES == 200 * 1024 * 1024
    assert COMPRESS_AFTER_DAYS == 14


def test_writes_lines_into_utc_day_file(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    clock = VirtualClock(datetime(2026, 10, 30, 23, 0, tzinfo=UTC))
    logger = DailyRotatingJsonLineLogger(logs, clock=clock)

    logger.info({"event": "run.started", "run_id": "r-1"})
    clock.advance(timedelta(seconds=1))
    logger.warn({"event": "run.slow", "run_id": "r-1"})
    logger.error({"run_id": "r-1"})

    rows = _read_lines(logs / "core-2026-10-30.jsonl")
    assert [row["level"] for row in rows] == ["INFO", "WARNING", "ERROR"]
    assert [row["at"] for row in rows] == [
        "2026-10-30T23:00:00+00:00",
        "2026-10-30T23:00:01+00:00",
        "2026-10-30T23:00:01+00:00",
    ]
    assert rows[0]["run_id"] == "r-1"
    assert rows[2]["event"] == "unspecified"
    assert set(rows[0]) == {"at", "level", "event", "run_id"}


def test_crossing_utc_day_starts_new_file_without_losing_lines(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    clock = VirtualClock(datetime(2026, 10, 30, 23, 59, tzinfo=UTC))
    logger = DailyRotatingJsonLineLogger(logs, clock=clock)

    logger.info({"event": "before.1"})
    logger.info({"event": "before.2"})
    clock.advance(timedelta(minutes=2))
    logger.info({"event": "after.1"})

    first = _read_lines(logs / "core-2026-10-30.jsonl")
    second = _read_lines(logs / "core-2026-10-31.jsonl")
    assert _events(logs / "core-2026-10-30.jsonl") == ["before.1", "before.2"]
    assert _events(logs / "core-2026-10-31.jsonl") == ["after.1"]
    assert len(first) + len(second) == 3


def test_full_file_rolls_to_next_index_keeping_lines_whole(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    logger = DailyRotatingJsonLineLogger(logs, clock=VirtualClock(NOW), max_file_bytes=1)

    logger.info({"event": "first"})
    logger.info({"event": "second"})
    logger.info({"event": "third"})

    base = logs / "core-2026-10-30.jsonl"
    second = logs / "core-2026-10-30.2.jsonl"
    third = logs / "core-2026-10-30.3.jsonl"
    assert _events(base) == ["first"]
    assert _events(second) == ["second"]
    assert _events(third) == ["third"]
    for path in (base, second, third):
        rows = _read_lines(path)
        assert len(rows) == 1
        assert rows[0]["level"] == "INFO"


def test_restart_appends_to_last_unfinished_file(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    clock = VirtualClock(NOW)
    DailyRotatingJsonLineLogger(logs, clock=clock).info({"event": "before.restart"})
    DailyRotatingJsonLineLogger(logs, clock=clock).info({"event": "after.restart"})

    assert _events(logs / "core-2026-10-30.jsonl") == [
        "before.restart",
        "after.restart",
    ]


def test_restart_advances_index_when_last_file_is_full(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    clock = VirtualClock(NOW)
    first = DailyRotatingJsonLineLogger(logs, clock=clock, max_file_bytes=1)
    first.info({"event": "one"})
    first.info({"event": "two"})

    DailyRotatingJsonLineLogger(logs, clock=clock, max_file_bytes=1).info({"event": "three"})

    assert _events(logs / "core-2026-10-30.jsonl") == ["one"]
    assert _events(logs / "core-2026-10-30.2.jsonl") == ["two"]
    assert _events(logs / "core-2026-10-30.3.jsonl") == ["three"]


def test_startup_compresses_files_outside_keep_window(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    old = logs / "core-2026-10-27.jsonl"
    old.write_text('{"event": "old"}\n', encoding="utf-8")
    old_bytes = old.read_bytes()
    kept = logs / "core-2026-10-28.jsonl"
    kept.write_text('{"event": "kept"}\n', encoding="utf-8")
    existing = logs / "core-2026-10-26.jsonl.gz"
    existing.write_bytes(gzip.compress(b'{"event": "existing"}\n'))

    DailyRotatingJsonLineLogger(logs, clock=VirtualClock(NOW), keep_days=3)

    assert not old.exists()
    assert gzip.decompress((logs / "core-2026-10-27.jsonl.gz").read_bytes()) == old_bytes
    assert kept.is_file()
    assert not (logs / "core-2026-10-28.jsonl.gz").exists()
    assert gzip.decompress(existing.read_bytes()) == b'{"event": "existing"}\n'
    assert not list(logs.glob("*.part"))


def test_compress_skips_source_another_instance_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logs = tmp_path / "logs"
    logger = DailyRotatingJsonLineLogger(logs, clock=VirtualClock(NOW), keep_days=1)
    vanished = logs / "core-2026-10-25.jsonl"
    # 扫描时文件还在，压缩前已被另一实例归档并删除：快照里留着过期条目。
    monkeypatch.setattr(
        logger,
        "_existing_files",
        lambda: [(vanished, NOW.date() - timedelta(days=5), 1)],
    )

    logger._compress_before(NOW.date())

    assert not (logs / "core-2026-10-25.jsonl.gz").exists()
    assert not list(logs.glob("*.part"))
    # 没有源文件不是失败：不写 log.compress_failed，当天日志文件不会被创建。
    assert not (logs / "core-2026-10-30.jsonl").exists()


def test_cross_day_switch_compresses_previous_day(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    clock = VirtualClock(datetime(2026, 10, 30, 23, 59, tzinfo=UTC))
    logger = DailyRotatingJsonLineLogger(logs, clock=clock, keep_days=1)

    logger.info({"event": "day.30"})
    clock.advance(timedelta(minutes=2))
    logger.info({"event": "day.31"})

    assert not (logs / "core-2026-10-30.jsonl").exists()
    assert (logs / "core-2026-10-30.jsonl.gz").is_file()
    assert _events(logs / "core-2026-10-31.jsonl") == ["day.31"]


def test_compression_failure_is_logged_without_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logs = tmp_path / "logs"
    logs.mkdir()
    old = logs / "core-2026-09-01.jsonl"
    old.write_text('{"event": "old"}\n', encoding="utf-8")

    def fail_replace(source: Path, target: Path) -> None:
        raise OSError("模拟替换失败")

    monkeypatch.setattr(logging_module, "_gzip_replace", fail_replace)
    logger = DailyRotatingJsonLineLogger(logs, clock=VirtualClock(NOW))
    logger.info({"event": "after.start"})

    events = _events(logs / "core-2026-10-30.jsonl")
    assert "log.compress_failed" in events
    assert "after.start" in events
    assert old.is_file()
    assert not (logs / "core-2026-09-01.jsonl.gz").exists()
