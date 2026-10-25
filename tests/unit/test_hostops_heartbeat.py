"""心跳文件：原子写、字段 schema 与 mtime 陈旧度。"""

from __future__ import annotations

import os
from pathlib import Path

from core.hostops.heartbeat import (
    HEARTBEAT_FILENAME,
    HeartbeatContent,
    heartbeat_age_seconds,
    read_heartbeat,
    write_heartbeat,
)


def make_content(active_run: str | None = None) -> HeartbeatContent:
    return {
        "pid": 1234,
        "started_at": "2026-10-25T08:00:00+00:00",
        "version": "0.1.0",
        "last_tick": "2026-10-25T08:01:00+00:00",
        "active_run": active_run,
    }


def test_write_heartbeat_is_atomic_and_complete(tmp_path: Path) -> None:
    path = tmp_path / HEARTBEAT_FILENAME
    write_heartbeat(path, make_content())
    assert read_heartbeat(path) == make_content()
    # 临时文件已被替换掉，目录里只剩心跳本体
    assert list(tmp_path.iterdir()) == [path]


def test_write_heartbeat_replaces_previous(tmp_path: Path) -> None:
    path = tmp_path / HEARTBEAT_FILENAME
    write_heartbeat(path, make_content())
    write_heartbeat(path, make_content(active_run="run-7"))
    assert read_heartbeat(path) == make_content(active_run="run-7")
    assert list(tmp_path.iterdir()) == [path]


def test_read_heartbeat_rejects_missing_corrupt_and_misshapen(tmp_path: Path) -> None:
    path = tmp_path / HEARTBEAT_FILENAME
    assert read_heartbeat(path) is None
    path.write_text("not json", encoding="utf-8")
    assert read_heartbeat(path) is None
    path.write_text('{"pid": "1234"}', encoding="utf-8")
    assert read_heartbeat(path) is None


def test_heartbeat_age_uses_mtime_with_stale_boundary(tmp_path: Path) -> None:
    path = tmp_path / HEARTBEAT_FILENAME
    assert heartbeat_age_seconds(path, now=1000.0) is None
    write_heartbeat(path, make_content())
    # mtime 本身是墙钟 epoch，用定值时间戳构造三处边界，不读系统时间
    base = 1_000_000.0
    for offset in (299.0, 300.0, 301.0):
        moment = base - offset
        os.utime(path, (moment, moment))
        age = heartbeat_age_seconds(path, now=base)
        assert age is not None
        assert abs(age - offset) < 0.5
    # 判死口径是「超过 300 秒」：恰好 300 不算陈旧
    limit = base - 300.0
    os.utime(path, (limit, limit))
    age_at_limit = heartbeat_age_seconds(path, now=base)
    assert age_at_limit is not None and age_at_limit <= 300.0
    stale = base - 301.0
    os.utime(path, (stale, stale))
    age_stale = heartbeat_age_seconds(path, now=base)
    assert age_stale is not None and age_stale > 300.0
