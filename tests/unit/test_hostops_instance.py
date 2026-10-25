"""锁文件、pid 存活与复用判定、命名互斥量与第二实例事件。"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from core.hostops import instance
from storage import migrate, open_database

NOW = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)


def make_lock(pid: int, started_at: str, *, boot_id: str = "boot-a") -> instance.LockContent:
    return {
        "pid": pid,
        "started_at": started_at,
        "exe_path": r"D:\lap-backstop\app\current\lap-backstop.exe",
        "boot_id": boot_id,
    }


def test_lock_file_roundtrip_creates_run_dir(tmp_path: Path) -> None:
    path = tmp_path / "run" / instance.LOCK_FILENAME
    content = make_lock(42, "2026-10-25T08:00:00+00:00")
    instance.write_lock_file(path, content)
    assert instance.read_lock_file(path) == content


def test_stale_lock_file_is_overwritten(tmp_path: Path) -> None:
    path = tmp_path / instance.LOCK_FILENAME
    instance.write_lock_file(path, make_lock(1, "2026-10-24T08:00:00+00:00"))
    fresh = make_lock(2, "2026-10-25T08:00:00+00:00")
    instance.write_lock_file(path, fresh)
    assert instance.read_lock_file(path) == fresh


def test_read_lock_file_rejects_missing_and_broken(tmp_path: Path) -> None:
    path = tmp_path / instance.LOCK_FILENAME
    assert instance.read_lock_file(path) is None
    path.write_text("not json", encoding="utf-8")
    assert instance.read_lock_file(path) is None
    path.write_text(
        '{"pid": "1", "started_at": "x", "exe_path": "y", "boot_id": ""}',
        encoding="utf-8",
    )
    assert instance.read_lock_file(path) is None


def test_read_boot_id_returns_string() -> None:
    # 本机注册表可读时是 MachineGuid；读不到时降级空串，两种都是合法结果。
    assert isinstance(instance.read_boot_id(), str)


def test_pid_liveness_and_reuse_detection() -> None:
    finished = subprocess.Popen([sys.executable, "-c", "pass"])
    finished.wait(timeout=30)
    assert finished.returncode == 0
    assert not instance.is_pid_alive(finished.pid)
    assert instance.is_pid_alive(os.getpid())

    creation = instance.process_creation_time(os.getpid())
    assert creation is not None
    same_process = make_lock(os.getpid(), creation.isoformat())
    assert instance.process_matches_lock(same_process, boot_id="boot-a")
    reused_pid = make_lock(os.getpid(), (creation - timedelta(hours=1)).isoformat())
    assert not instance.process_matches_lock(reused_pid, boot_id="boot-a")
    across_reboot = make_lock(os.getpid(), creation.isoformat(), boot_id="boot-b")
    assert not instance.process_matches_lock(across_reboot, boot_id="boot-a")
    dead = make_lock(finished.pid, creation.isoformat())
    assert not instance.process_matches_lock(dead, boot_id="boot-a")
    # 降级路径：锁里 boot_id 为空时只比 started_at，不做跨重启排除
    degraded = make_lock(os.getpid(), creation.isoformat(), boot_id="")
    assert instance.process_matches_lock(degraded, boot_id="boot-a")


def test_named_mutex_blocks_second_owner() -> None:
    name = f"Global\\lap-backstop-test-{os.getpid()}"
    handle = instance.acquire_named_mutex(name)
    assert handle is not None
    assert instance.acquire_named_mutex(name) is None
    instance.release_named_mutex(handle)
    again = instance.acquire_named_mutex(name)
    assert again is not None
    instance.release_named_mutex(again)


def test_report_second_instance_writes_event_and_tolerates_missing_db(tmp_path: Path) -> None:
    database = tmp_path / "backstop.sqlite"
    instance.report_second_instance(database, at=NOW)
    connection = open_database(database)
    try:
        migrate(connection)
    finally:
        connection.close()
    instance.report_second_instance(database, at=NOW)
    reader = sqlite3.connect(database)
    try:
        rows = reader.execute("SELECT payload FROM events_global").fetchall()
    finally:
        reader.close()
    events = [json.loads(str(row[0])) for row in rows]
    assert any(event.get("event") == "core.second_instance" for event in events)
