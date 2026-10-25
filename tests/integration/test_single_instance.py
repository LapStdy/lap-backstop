"""单实例：第二实例退出码 3、事件落库、锁文件不动；持有者退出后可再获取。"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from core.hostops import instance
from core.hostops.heartbeat import HEARTBEAT_FILENAME
from tests.integration.test_serve_shell import (
    REPO_ROOT,
    WAIT_SECONDS,
    spawn_serve,
    stderr_of,
    wait_for_file,
    wait_for_heartbeat_pid,
)


def test_second_instance_exits_3_and_keeps_lock_untouched(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    heartbeat_path = data_root / "run" / HEARTBEAT_FILENAME
    lock_path = data_root / "run" / instance.LOCK_FILENAME
    first = spawn_serve(data_root)
    try:
        wait_for_file(heartbeat_path)
        second = subprocess.run(
            [sys.executable, "-m", "core", "serve", "--data-root", str(data_root)],
            cwd=REPO_ROOT,
            capture_output=True,
            timeout=WAIT_SECONDS,
            env={**os.environ, "PYTHONUTF8": "1"},
        )
        assert second.returncode == 3
        reader = sqlite3.connect(data_root / "backstop.sqlite")
        try:
            rows = reader.execute("SELECT payload FROM events_global").fetchall()
        finally:
            reader.close()
        events = [json.loads(str(row[0])) for row in rows]
        assert any(event.get("event") == "core.second_instance" for event in events)
        lock = instance.read_lock_file(lock_path)
        assert lock is not None
        assert lock["pid"] == first.pid
    except AssertionError:
        first.kill()
        raise AssertionError(f"失败时第一实例 stderr：{stderr_of(first)}") from None
    finally:
        if first.poll() is None:
            first.terminate()
            first.wait(timeout=WAIT_SECONDS)


def test_mutex_free_for_next_start_after_owner_exits(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    heartbeat_path = data_root / "run" / HEARTBEAT_FILENAME
    first = spawn_serve(data_root)
    wait_for_file(heartbeat_path)
    first.terminate()
    first.wait(timeout=WAIT_SECONDS)
    successor = spawn_serve(data_root)
    try:
        wait_for_heartbeat_pid(heartbeat_path, successor.pid)
    except AssertionError:
        successor.kill()
        raise AssertionError(f"后续实例未取得互斥量；stderr：{stderr_of(successor)}") from None
    finally:
        successor.terminate()
        successor.wait(timeout=WAIT_SECONDS)
