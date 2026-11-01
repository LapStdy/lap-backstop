"""serve 外壳：进程内注入时钟跑有界轮次；真子进程验证启动序与崩溃残留。"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from contracts import RunContext, RunState, ScriptHandle, ScriptObservation
from core.clock import VirtualClock
from core.hostops import instance, serve, tasks, timesync, watchdog
from core.hostops.heartbeat import HEARTBEAT_FILENAME, read_heartbeat
from core.hostops.serve import (
    AUTOLOGON_CHECKED_EVENT,
    TASKS_REGISTERED_EVENT,
    WATCHDOG_GAVE_UP_EVENT,
    HeartbeatWriter,
    ServeLoop,
    ServeOptions,
    run_serve,
)
from core.jobs.run_service import JobRunService
from core.logging import JsonLineLogger
from core.notify.sweep import SWEEP_ERROR
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.notifications import NotificationRepository

REPO_ROOT = Path(__file__).resolve().parents[2]
WAIT_SECONDS = 30.0
THREAD_T0 = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)
_STABLE_DISK_FREE = 100 * 1024**3


def _loop_options(clock: VirtualClock, stop: threading.Event) -> ServeOptions:
    """循环级用例的注入项：不碰 schtasks 与注册表，tick 由停机事件收口。"""
    return ServeOptions(
        clock=clock,
        stop=stop,
        max_ticks=None,
        tick_interval_seconds=0.001,
        register_tasks=False,
        run_timesync=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )


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


def missing_timesync_runner() -> timesync.Runner:
    """假 w32tm：命令不存在。"""

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise FileNotFoundError(2, "系统找不到指定的文件", command[0])

    return run


def failing_timesync_runner() -> timesync.Runner:
    """假 w32tm：一律非零退出。"""

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(command, 1, b"", b"denied")

    return run


def skewed_timesync_runner(offset_line: bytes) -> timesync.Runner:
    """假 w32tm：校时成功，stripchart 报固定偏移。"""

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        if command == list(timesync.SYNC_COMMAND):
            return subprocess.CompletedProcess(command, 0, b"", b"")
        return subprocess.CompletedProcess(command, 0, offset_line, b"")

    return run


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

    子进程跳过计划任务注册与系统校时：测试环境不改宿主的 schtasks，也不动系统时钟。
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
            "--no-timesync",
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
        run_timesync=False,
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


def test_run_serve_sweeps_pending_notifications_from_previous_process(tmp_path: Path) -> None:
    """上次进程遗留的 pending 通知在启动序里清扫：行落 failed，事件逐条可追。"""
    database = tmp_path / "backstop.sqlite"
    connection = open_database(database)
    migrate(connection)
    repository = NotificationRepository(connection)
    repository.insert_pending("n-left-1", None, "serverchan")
    repository.insert_pending("n-left-2", None, "serverchan")
    connection.execute("UPDATE notifications SET attempts = 2 WHERE id = 'n-left-2'")
    repository.insert_pending("n-sent", None, "serverchan")
    repository.mark_sent("n-sent", attempts=1, at=datetime(2026, 10, 25, 8, 0, tzinfo=UTC))
    connection.close()
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
        register_tasks=False,
        run_timesync=False,
    )
    assert run_serve(tmp_path, options) == 0
    reader = sqlite3.connect(database)
    reader.row_factory = sqlite3.Row
    try:
        rows = {
            str(row["id"]): row for row in reader.execute("SELECT * FROM notifications").fetchall()
        }
    finally:
        reader.close()
    assert rows["n-left-1"]["status"] == "failed"
    assert rows["n-left-1"]["error"] == SWEEP_ERROR
    assert rows["n-left-1"]["sent_at"] is None
    assert rows["n-left-2"]["attempts"] == 2
    assert rows["n-sent"]["status"] == "sent"
    assert rows["n-sent"]["error"] is None
    swept = [
        payload
        for payload in global_event_payloads(database)
        if payload.get("event") == "notify.pending_swept"
    ]
    assert {payload["payload"]["notification_id"] for payload in swept} == {
        "n-left-1",
        "n-left-2",
    }
    # 第二次启动：无遗留 pending，不清扫也不追加事件
    assert run_serve(tmp_path, options) == 0
    swept_after_restart = [
        payload
        for payload in global_event_payloads(database)
        if payload.get("event") == "notify.pending_swept"
    ]
    assert swept_after_restart == swept


@pytest.mark.parametrize("mode", ["nonzero_exit", "timeout"])
def test_run_serve_records_failed_registration_and_still_serves(tmp_path: Path, mode: str) -> None:
    """注册命令失败（非零退出、超时）不算启动失败：失败口径照落事件，核心照常进循环。"""
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
        task_runner=failing_task_runner(mode),
        run_timesync=False,
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


def test_run_serve_records_missing_timesync_and_still_serves(tmp_path: Path) -> None:
    """校时命令不存在不算启动失败：失败口径照落事件，核心照常进循环。"""
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
        register_tasks=False,
        timesync_runner=missing_timesync_runner(),
    )
    assert run_serve(tmp_path, options) == 0
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == timesync.TIMESYNC_EVENT
    )
    assert payload["ok"] is False
    assert "找不到 w32tm" in str(payload["detail"])
    assert read_heartbeat(tmp_path / "run" / HEARTBEAT_FILENAME) is not None


def test_run_serve_records_failed_timesync_command_and_still_serves(tmp_path: Path) -> None:
    """校时命令非零退出同样不算启动失败：失败口径照落事件，核心照常进循环。"""
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
        register_tasks=False,
        timesync_runner=failing_timesync_runner(),
    )
    assert run_serve(tmp_path, options) == 0
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == timesync.TIMESYNC_EVENT
    )
    assert payload["ok"] is False
    assert "退出码 1" in str(payload["detail"])
    assert read_heartbeat(tmp_path / "run" / HEARTBEAT_FILENAME) is not None


def test_run_serve_pauses_catch_up_on_measured_skew(tmp_path: Path) -> None:
    """启动测得偏移 31 秒：host.clock_skew 落库，补跑暂停标记置位。"""
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
        register_tasks=False,
        timesync_runner=skewed_timesync_runner(b"16:02:36, +31.0s\n"),
    )
    assert run_serve(tmp_path, options) == 0
    payloads = global_event_payloads(tmp_path / "backstop.sqlite")
    skew = next(payload for payload in payloads if payload.get("event") == timesync.SKEW_EVENT)
    assert skew["offset_seconds"] == 31.0
    reader = sqlite3.connect(tmp_path / "backstop.sqlite")
    try:
        row = reader.execute(
            "SELECT value FROM settings WHERE key = ?", (timesync.CATCH_UP_PAUSED_KEY,)
        ).fetchone()
    finally:
        reader.close()
    assert row is not None
    assert json.loads(str(row[0])) is True


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
        run_timesync=False,
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
        run_timesync=False,
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
        run_timesync=False,
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
        run_timesync=False,
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
        run_timesync=False,
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
        run_timesync=False,
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
        run_timesync=False,
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
        run_timesync=False,
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


class _GatedScript(FakeScriptAdapter):
    """observe 停在门上直到测试放行；进入事件供主线程确定性同步。"""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self.entered.set()
        assert self.release.wait(timeout=WAIT_SECONDS * 2), "测试没有放行执行器"
        return super().observe(handle, ctx)


class _ShutdownThenExitScript(FakeScriptAdapter):
    """第一次观察置位停机并正常结束：停机要等执行阶段跑完再收尾。"""

    def __init__(self, stop: threading.Event) -> None:
        super().__init__()
        self._stop = stop

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self._stop.set()
        return super().observe(handle, ctx)


class _NoopHeartbeat:
    """丢弃心跳：循环级用例只关心 tick 节律，不关心心跳文件。"""

    def write(self, *, last_tick_at: datetime, active_run: str | None) -> None:
        return None


def _seed_template(connection: sqlite3.Connection, template_id: str, environment_id: str) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            template_id,
            environment_id,
            json.dumps({"scenario": "instant"}),
            json.dumps(
                {
                    "id": "fake-policy",
                    "verifier_plugin_id": "fake",
                    "required_evidence": ["log"],
                    "params": {},
                }
            ),
            THREAD_T0.isoformat(),
            THREAD_T0.isoformat(),
        ),
    )


def _seed_run(
    runs: JobRunService,
    template_id: str,
    *,
    key: str,
    scheduled_at: datetime = THREAD_T0,
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=template_id,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=THREAD_T0,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    runs.enqueue(record.id)
    return record.id


def _fake_registry(script: FakeScriptAdapter) -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=script,
            verifier=FakeVerifier(),
        )
    )
    return registry


def _run_state(database: Path, run_id: str) -> str | None:
    reader = sqlite3.connect(database)
    try:
        row = reader.execute("SELECT state FROM job_runs WHERE id = ?", (run_id,)).fetchone()
    finally:
        reader.close()
    return None if row is None else str(row[0])


class _RecordingWriter:
    """记录每次心跳的在途运行；主线程按内容轮询，不以写入笔数置位。"""

    def __init__(self, writes: list[dict[str, Any]]) -> None:
        self._writes = writes

    def write(self, *, last_tick_at: datetime, active_run: str | None) -> None:
        self._writes.append({"last_tick": last_tick_at, "active_run": active_run})


def test_serve_loop_writes_heartbeat_while_run_blocks_in_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """一次长运行不占住 tick：阻塞期间心跳照写、轮次照进，运行仍处 executing。"""
    monkeypatch.setattr(serve, "_HEARTBEAT_INTERVAL", timedelta(0))
    database = tmp_path / "backstop.sqlite"
    adapter = _GatedScript()
    writes: list[dict[str, Any]] = []
    stop = threading.Event()
    outcome: dict[str, object] = {}
    ready = threading.Event()
    seeded: dict[str, str] = {}

    def owner() -> None:
        connection = open_database(database)
        try:
            migrate(connection)
            _seed_template(connection, "template-1", "env-1")
            clock = VirtualClock(THREAD_T0)
            runs = JobRunService(connection, clock)
            seeded["run_id"] = _seed_run(runs, "template-1", key="blocked")
            supervisor = Supervisor(
                connection,
                clock,
                _fake_registry(adapter),
                data_root=tmp_path,
                disk_free=lambda: _STABLE_DISK_FREE,
                shutdown=stop.is_set,
            )
            loop = ServeLoop(
                supervisor,
                runs,
                cast(HeartbeatWriter, _RecordingWriter(writes)),
                _loop_options(clock, stop),
            )
            ready.set()
            outcome["code"] = loop.run()
        finally:
            connection.close()

    thread = threading.Thread(target=owner, name="serve-owner", daemon=True)
    thread.start()
    run_id = ""
    try:
        assert ready.wait(WAIT_SECONDS), "serve 循环没有就绪"
        run_id = seeded["run_id"]
        assert adapter.entered.wait(WAIT_SECONDS), "运行没有进入执行"

        def heartbeats_with_run() -> bool:
            return len([entry for entry in writes if entry["active_run"] == run_id]) >= 2

        deadline = time.monotonic() + WAIT_SECONDS
        while time.monotonic() < deadline and not heartbeats_with_run():
            time.sleep(0.05)
        active_writes = [entry for entry in writes if entry["active_run"] == run_id]
        assert len(active_writes) >= 2, f"阻塞期间没有记录在途运行：{writes[:3]}"
        assert _run_state(database, run_id) == RunState.EXECUTING.value
    finally:
        adapter.release.set()
        stop.set()
        thread.join(WAIT_SECONDS)
    assert not thread.is_alive(), "循环没有收尾"
    assert outcome.get("code") == 0


def test_shutdown_writes_single_interrupt_at_phase_boundary(tmp_path: Path) -> None:
    """执行中停机：阶段跑完写恰一条 run.interrupted；状态与租约留给下次启动回收。"""
    database = tmp_path / "backstop.sqlite"
    stop = threading.Event()
    outcome: dict[str, object] = {}
    ready = threading.Event()
    seeded: dict[str, str] = {}

    def owner() -> None:
        connection = open_database(database)
        try:
            migrate(connection)
            _seed_template(connection, "template-1", "env-1")
            _seed_template(connection, "template-2", "env-2")
            clock = VirtualClock(THREAD_T0)
            runs = JobRunService(connection, clock)
            seeded["run_id"] = _seed_run(runs, "template-1", key="interrupt")
            seeded["future_id"] = _seed_run(
                runs, "template-2", key="future", scheduled_at=THREAD_T0 + timedelta(hours=1)
            )
            supervisor = Supervisor(
                connection,
                clock,
                _fake_registry(_ShutdownThenExitScript(stop)),
                data_root=tmp_path,
                disk_free=lambda: _STABLE_DISK_FREE,
                shutdown=stop.is_set,
            )
            loop = ServeLoop(
                supervisor,
                runs,
                cast(HeartbeatWriter, _NoopHeartbeat()),
                _loop_options(clock, stop),
            )
            ready.set()
            outcome["code"] = loop.run()
        finally:
            connection.close()

    thread = threading.Thread(target=owner, name="serve-owner", daemon=True)
    thread.start()
    run_id = ""
    try:
        assert ready.wait(WAIT_SECONDS), "serve 循环没有就绪"
        run_id = seeded["run_id"]

        def interrupted_once() -> bool:
            return _interrupt_count(database, run_id) >= 1

        deadline = time.monotonic() + WAIT_SECONDS
        while time.monotonic() < deadline and not interrupted_once():
            time.sleep(0.05)
        assert interrupted_once(), "停机收尾事件没有写出"
    finally:
        stop.set()
        thread.join(WAIT_SECONDS)
    assert not thread.is_alive(), "循环没有收尾"
    assert outcome.get("code") == 0

    reader = sqlite3.connect(database)
    try:
        interrupted = reader.execute(
            "SELECT payload FROM run_events WHERE run_id = ? AND type = 'run.interrupted'",
            (run_id,),
        ).fetchall()
        lease = reader.execute(
            "SELECT state FROM resource_leases WHERE run_id = ?", (run_id,)
        ).fetchone()
        phases = reader.execute(
            "SELECT phase FROM run_phases WHERE run_id = ? ORDER BY rowid", (run_id,)
        ).fetchall()
        future = reader.execute(
            "SELECT state FROM job_runs WHERE id = ?", (seeded["future_id"],)
        ).fetchone()
    finally:
        reader.close()
    assert len(interrupted) == 1, f"run.interrupted 条数不对：{len(interrupted)}"
    assert json.loads(str(interrupted[0][0]))["reason"] == "shutdown"
    assert _run_state(database, run_id) == RunState.EXECUTING.value
    assert lease is not None and str(lease[0]) == "leased"
    assert [str(row[0]) for row in phases] == ["prepare", "execute"]
    assert future is not None and str(future[0]) == RunState.QUEUED.value


def _interrupt_count(database: Path, run_id: str) -> int:
    reader = sqlite3.connect(database)
    try:
        row = reader.execute(
            "SELECT COUNT(*) FROM run_events WHERE run_id = ? AND type = 'run.interrupted'",
            (run_id,),
        ).fetchone()
    finally:
        reader.close()
    return 0 if row is None else int(row[0])


def test_stuck_executor_times_out_bounded_wait_and_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """卡死的执行线程：等待到模块上限即放弃，记日志与事件，进程照常退出。"""
    monkeypatch.setattr(serve, "_EXECUTOR_JOIN_SECONDS", 0.2)
    database = tmp_path / "backstop.sqlite"
    log_path = tmp_path / "logs" / "core.jsonl"
    adapter = _GatedScript()
    stop = threading.Event()
    outcome: dict[str, object] = {}
    ready = threading.Event()
    seeded: dict[str, str] = {}

    def owner() -> None:
        connection = open_database(database)
        try:
            migrate(connection)
            _seed_template(connection, "template-1", "env-1")
            clock = VirtualClock(THREAD_T0)
            runs = JobRunService(connection, clock)
            seeded["run_id"] = _seed_run(runs, "template-1", key="stuck")
            supervisor = Supervisor(
                connection,
                clock,
                _fake_registry(adapter),
                data_root=tmp_path,
                log=JsonLineLogger(log_path),
                disk_free=lambda: _STABLE_DISK_FREE,
                shutdown=stop.is_set,
            )
            loop = ServeLoop(
                supervisor,
                runs,
                cast(HeartbeatWriter, _NoopHeartbeat()),
                _loop_options(clock, stop),
            )
            ready.set()
            outcome["code"] = loop.run()
        finally:
            connection.close()

    thread = threading.Thread(target=owner, name="serve-owner", daemon=True)
    thread.start()
    run_id = ""
    try:
        assert ready.wait(WAIT_SECONDS), "serve 循环没有就绪"
        run_id = seeded["run_id"]
        assert adapter.entered.wait(WAIT_SECONDS), "运行没有进入执行"
        stop.set()
    finally:
        thread.join(WAIT_SECONDS)
        adapter.release.set()
    assert not thread.is_alive(), "等待上限没有收口循环"
    assert outcome.get("code") == 0
    # 运行状态与租约原样留给下次启动回收
    assert _run_state(database, run_id) == RunState.EXECUTING.value
    reader = sqlite3.connect(database)
    try:
        lease = reader.execute(
            "SELECT state FROM resource_leases WHERE run_id = ?", (run_id,)
        ).fetchone()
        timeouts = reader.execute(
            "SELECT COUNT(*) FROM events_global WHERE payload LIKE '%executor_join_timeout%'"
        ).fetchone()
    finally:
        reader.close()
    assert lease is not None and str(lease[0]) == "leased"
    assert timeouts is not None and int(timeouts[0]) == 1
    logged = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    timeout_logs = [
        entry for entry in logged if entry.get("event") == "supervisor.executor_join_timeout"
    ]
    assert len(timeout_logs) == 1, f"超时记账日志条数不对：{timeout_logs}"
    assert timeout_logs[0]["run_ids"] == [run_id]
