"""serve 外壳：进程内注入时钟跑有界轮次；真子进程验证启动序与崩溃残留。"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from core.clock import VirtualClock
from core.hostops import instance, tasks, watchdog
from core.hostops.heartbeat import HEARTBEAT_FILENAME, read_heartbeat
from core.hostops.serve import (
    AUTOLOGON_CHECKED_EVENT,
    TASKS_REGISTERED_EVENT,
    WATCHDOG_GAVE_UP_EVENT,
    ServeOptions,
    run_serve,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
WAIT_SECONDS = 30.0


def fake_task_runner() -> tuple[tasks.Runner, list[list[str]]]:
    """假 schtasks：记录调用并一律成功。"""
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(list(command))
        return subprocess.CompletedProcess(command, 0, b"", b"")

    return run, calls


def autologon_registry(logon: str, user: str) -> dict[str, str]:
    return {"AutoAdminLogon": logon, "DefaultUserName": user}


def global_events(database: Path) -> list[str]:
    """读全局事件表里的事件名，按写入顺序。"""
    reader = sqlite3.connect(database)
    try:
        rows = reader.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    finally:
        reader.close()
    return [str(json.loads(str(row[0])).get("event")) for row in rows]


def wait_for_file(path: Path, timeout: float = WAIT_SECONDS) -> None:
    """轮询等待文件出现；超时抛 AssertionError。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.is_file():
            return
        time.sleep(0.2)
    raise AssertionError(f"{timeout:g} 秒内未出现 {path}")


def wait_for_heartbeat_pid(path: Path, pid: int, timeout: float = WAIT_SECONDS) -> None:
    """轮询等待心跳文件里出现指定 pid，证明该实例已完成启动并开始 tick。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        content = read_heartbeat(path)
        if content is not None and content["pid"] == pid:
            return
        time.sleep(0.2)
    raise AssertionError(f"{timeout:g} 秒内心跳未更新为 pid {pid}")


def spawn_serve(data_root: Path) -> subprocess.Popen[bytes]:
    """拉起 serve 子进程；stdout/stderr 留管道便于排障。

    子进程跳过计划任务注册：测试环境不应改动宿主的 schtasks。
    """
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "core",
            "serve",
            "--data-root",
            str(data_root),
            "--no-register-tasks",
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "PYTHONUTF8": "1"},
    )


def stderr_of(process: subprocess.Popen[bytes]) -> str:
    """子进程已终止后取 stderr 文本。"""
    assert process.stderr is not None
    return process.stderr.read().decode("utf-8", errors="replace")


def test_run_serve_bounded_ticks_write_heartbeat_and_cleanup(tmp_path: Path) -> None:
    task_runner, task_calls = fake_task_runner()
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=3,
        tick_interval_seconds=0.0,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
        task_runner=task_runner,
    )
    code = run_serve(tmp_path, options)
    assert code == 0
    content = read_heartbeat(tmp_path / "run" / HEARTBEAT_FILENAME)
    assert content is not None
    assert content["pid"] == os.getpid()
    assert content["active_run"] is None
    datetime.fromisoformat(content["started_at"])
    datetime.fromisoformat(content["last_tick"])
    # 正常收尾：锁文件删除
    assert not (tmp_path / "run" / instance.LOCK_FILENAME).exists()
    events = global_events(tmp_path / "backstop.sqlite")
    assert AUTOLOGON_CHECKED_EVENT in events
    assert TASKS_REGISTERED_EVENT in events
    # 注册走 schtasks 覆盖式导入，P0 清单只有 core 与 watchdog 两个任务
    assert [call[3] for call in task_calls] == ["/tn", "/tn"]
    assert {call[4] for call in task_calls} == {tasks.TASK_CORE, tasks.TASK_WATCHDOG}


def test_run_serve_reports_watchdog_gave_up_marker(tmp_path: Path) -> None:
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    watchdog.write_state(
        state_path,
        {
            "pending_start": None,
            "failures": ["2026-10-25T07:50:00+00:00"],
            "gave_up_pending_report": True,
        },
    )
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 0
    assert WATCHDOG_GAVE_UP_EVENT in global_events(tmp_path / "backstop.sqlite")
    # 补报是取走式：标记清零，不会重复上报
    assert watchdog.read_state(state_path)["gave_up_pending_report"] is False


def test_subprocess_hard_kill_leaves_lock_and_next_start_overwrites(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    heartbeat_path = data_root / "run" / HEARTBEAT_FILENAME
    lock_path = data_root / "run" / instance.LOCK_FILENAME
    process = spawn_serve(data_root)
    try:
        wait_for_file(heartbeat_path)
    except AssertionError:
        process.kill()
        raise AssertionError(f"serve 子进程未写出心跳；stderr：{stderr_of(process)}") from None
    content = read_heartbeat(heartbeat_path)
    assert content is not None
    assert content["pid"] == process.pid
    lock = instance.read_lock_file(lock_path)
    assert lock is not None
    assert lock["pid"] == process.pid
    # Windows 的 terminate 是硬杀：收尾代码不会跑，锁文件成为崩溃残留
    process.terminate()
    assert process.wait(timeout=WAIT_SECONDS) != 0
    assert lock_path.exists()
    restart = spawn_serve(data_root)
    try:
        wait_for_heartbeat_pid(heartbeat_path, restart.pid)
    except AssertionError:
        restart.kill()
        raise AssertionError(f"重启实例未接管；stderr：{stderr_of(restart)}") from None
    fresh_lock = instance.read_lock_file(lock_path)
    assert fresh_lock is not None
    assert fresh_lock["pid"] == restart.pid
    restart.terminate()
    restart.wait(timeout=WAIT_SECONDS)
