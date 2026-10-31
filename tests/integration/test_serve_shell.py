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
from typing import Any, cast

import pytest

from core.clock import VirtualClock
from core.hostops import instance, serve, tasks, watchdog
from core.hostops.heartbeat import HEARTBEAT_FILENAME, read_heartbeat
from core.hostops.serve import (
    AUTOLOGON_CHECKED_EVENT,
    TASKS_REGISTERED_EVENT,
    WATCHDOG_GAVE_UP_EVENT,
    ServeOptions,
    run_serve,
)
from core.logging import JsonLineLogger

REPO_ROOT = Path(__file__).resolve().parents[2]
WAIT_SECONDS = 30.0


def fake_task_runner() -> tuple[tasks.Runner, list[list[str]]]:
    """假 schtasks：记录调用并一律成功。"""
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(list(command))
        return subprocess.CompletedProcess(command, 0, b"", b"")

    return run, calls


def failing_task_runner(mode: str) -> tasks.Runner:
    """假 schtasks：按 mode 一律非零退出或一律超时。"""
    if mode == "timeout":

        def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
            raise subprocess.TimeoutExpired(cmd=command, timeout=10.0)

        return run

    def failed(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(command, 1, b"", b"")

    return failed


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


def global_event_rows(database: Path) -> list[tuple[str, str]]:
    """读全局事件表里每行的顶层类型与事件名，按写入顺序。"""
    reader = sqlite3.connect(database)
    try:
        rows = reader.execute("SELECT type, payload FROM events_global ORDER BY seq").fetchall()
    finally:
        reader.close()
    return [(str(row[0]), str(json.loads(str(row[1])).get("event"))) for row in rows]


def global_event_payloads(database: Path) -> list[dict[str, Any]]:
    """读全局事件表里每行的事件 payload，按写入顺序。"""
    reader = sqlite3.connect(database)
    try:
        rows = reader.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    finally:
        reader.close()
    return [dict(json.loads(str(row[0]))) for row in rows]


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
    host_rows = {
        (event_type, name)
        for event_type, name in global_event_rows(tmp_path / "backstop.sqlite")
        if name in (AUTOLOGON_CHECKED_EVENT, TASKS_REGISTERED_EVENT)
    }
    assert host_rows == {
        ("host.event", AUTOLOGON_CHECKED_EVENT),
        ("host.event", TASKS_REGISTERED_EVENT),
    }
    # 注册走 schtasks 覆盖式导入，清单以 tasks.REGISTERED_TASKS 为准
    assert [call[3] for call in task_calls] == ["/tn"] * len(tasks.REGISTERED_TASKS)
    assert {call[4] for call in task_calls} == set(tasks.REGISTERED_TASKS)


@pytest.mark.parametrize("mode", ["nonzero_exit", "timeout"])
def test_run_serve_records_failed_registration_and_still_serves(tmp_path: Path, mode: str) -> None:
    """注册命令失败（非零退出、超时）不算启动失败：失败口径照落事件，核心照常进循环。"""
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
        task_runner=failing_task_runner(mode),
    )
    assert run_serve(tmp_path, options) == 0
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == TASKS_REGISTERED_EVENT
    )
    assert payload["detail"] == "注册失败：" + "、".join(tasks.REGISTERED_TASKS)
    # 核心没有被注册失败拖住：心跳照写，锁文件照常收尾
    assert read_heartbeat(tmp_path / "run" / HEARTBEAT_FILENAME) is not None
    assert not (tmp_path / "run" / instance.LOCK_FILENAME).exists()


def test_run_serve_reports_watchdog_gave_up_marker(tmp_path: Path) -> None:
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    # 旧版状态文件：只有布尔标记，claim 走兜底载荷
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
    assert ("host.event", WATCHDOG_GAVE_UP_EVENT) in global_event_rows(tmp_path / "backstop.sqlite")
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == WATCHDOG_GAVE_UP_EVENT
    )
    assert "1 次验收失败" in payload["detail"]
    assert payload["failed_count"] == 1
    assert payload["last_failure_at"] == "2026-10-25T07:50:00+00:00"
    assert payload["heartbeat"] is None
    assert payload["log_tail"] == []
    # 补报是取走式：标记清零，不会重复上报
    assert watchdog.read_state(state_path)["gave_up_pending_report"] is False


def test_run_serve_reports_stored_watchdog_report_with_evidence(tmp_path: Path) -> None:
    """新格式报告经核心启动补报：事件五个键齐全，证据字段原样透传。"""
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    watchdog.write_state(
        state_path,
        {
            "pending_start": None,
            "failures": ["2026-10-25T07:50:00+00:00"],
            "gave_up_pending_report": True,
            "gave_up_report": {
                "failed_count": 3,
                "last_failure_at": "2026-10-25T07:50:00+00:00",
                "heartbeat": {"pid": 4242, "last_tick": "2026-10-25T07:49:00+00:00"},
                "log_tail": ["first line", "second line"],
            },
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
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == WATCHDOG_GAVE_UP_EVENT
    )
    assert set(payload) == {
        "event",
        "detail",
        "failed_count",
        "last_failure_at",
        "heartbeat",
        "log_tail",
    }
    assert "3 次验收失败" in payload["detail"]
    assert payload["failed_count"] == 3
    assert payload["last_failure_at"] == "2026-10-25T07:50:00+00:00"
    assert payload["heartbeat"] == {"pid": 4242, "last_tick": "2026-10-25T07:49:00+00:00"}
    assert payload["log_tail"] == ["first line", "second line"]


def test_run_serve_falls_back_when_stored_report_lacks_log_tail(tmp_path: Path) -> None:
    """报告缺 log_tail → 不可解析，走兜底载荷（失败次数从 failures 推）。"""
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    watchdog.write_state(
        state_path,
        {
            "pending_start": None,
            "failures": ["2026-10-25T07:50:00+00:00"],
            "gave_up_pending_report": True,
            "gave_up_report": cast(watchdog.GaveUpReport, {"failed_count": 9}),
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
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == WATCHDOG_GAVE_UP_EVENT
    )
    assert payload["failed_count"] == 1
    assert payload["heartbeat"] is None
    assert payload["log_tail"] == []


def test_run_serve_skips_gave_up_event_when_state_corrupt(tmp_path: Path) -> None:
    """状态文件损坏时无法证明停过手，不产生停手事件，核心照常启动。"""
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("not json", encoding="utf-8")
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 0
    assert WATCHDOG_GAVE_UP_EVENT not in global_events(tmp_path / "backstop.sqlite")


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


def test_autologon_failed_check_marks_event_payload(tmp_path: Path) -> None:
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        registry_get=lambda _key, name: autologon_registry("0", "")[name],
    )
    assert run_serve(tmp_path, options) == 0
    reader = sqlite3.connect(tmp_path / "backstop.sqlite")
    try:
        rows = reader.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    finally:
        reader.close()
    payloads = [json.loads(str(row[0])) for row in rows]
    autologon = next(
        payload for payload in payloads if payload.get("event") == AUTOLOGON_CHECKED_EVENT
    )
    assert autologon["ok"] is False
    assert "AutoAdminLogon=0" in autologon["detail"]


def test_serve_snapshots_database_before_pending_migration(tmp_path: Path) -> None:
    clock = VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC))
    options = ServeOptions(
        clock=clock,
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 0
    backups = tmp_path / "backups"
    assert [path.name for path in backups.glob("pre-migrate-*.sqlite")] == [
        "pre-migrate-2-20261025-080000.sqlite"
    ]
    assert (backups / "pre-migrate-2-20261025-080000.manifest.json").is_file()
    # 库已是最新版本，下一次启动不再生成迁移前快照
    assert run_serve(tmp_path, options) == 0
    assert len(list(backups.glob("pre-migrate-*.sqlite"))) == 1


def test_serve_refuses_start_when_pre_migrate_snapshot_fails(tmp_path: Path) -> None:
    backups = tmp_path / "backups"
    backups.mkdir()
    occupied = backups / "pre-migrate-2-20261025-080000.sqlite"
    occupied.write_bytes(b"occupied")
    log_path = tmp_path / "logs" / "core.jsonl"
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        log=JsonLineLogger(log_path),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 1
    connection = sqlite3.connect(tmp_path / "backstop.sqlite")
    try:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
    finally:
        connection.close()
    logged = [
        json.loads(line)["event"] for line in log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert "serve.migrate_snapshot_failed" in logged
    assert not (tmp_path / "run" / instance.LOCK_FILENAME).exists()


def test_serve_logs_program_error_and_reraises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log_path = tmp_path / "logs" / "core.jsonl"
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        log=JsonLineLogger(log_path),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )

    def broken_migrate(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("程序错误")

    monkeypatch.setattr(serve, "migrate", broken_migrate)
    with pytest.raises(RuntimeError, match="程序错误"):
        run_serve(tmp_path, options)
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    failed = [entry for entry in entries if entry.get("event") == "serve.run_failed"]
    assert failed
    assert failed[0]["error"] == "程序错误"
    assert failed[0]["error_type"] == "RuntimeError"
    assert not (tmp_path / "run" / instance.LOCK_FILENAME).exists()
