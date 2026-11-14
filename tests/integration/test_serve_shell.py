"""serve 外壳：进程内注入时钟跑有界轮次；真子进程验证启动序与崩溃残留。"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.clock import VirtualClock
from core.hostops import instance, serve, tasks, timesync
from core.hostops.heartbeat import HEARTBEAT_FILENAME, read_heartbeat
from core.hostops.serve import (
    AUTOLOGON_CHECKED_EVENT,
    TASKS_REGISTERED_EVENT,
    ServeOptions,
    run_serve,
)
from core.logging import JsonLineLogger
from core.notify.sweep import SWEEP_ERROR
from storage import migrate, open_database
from storage.repositories.notifications import NotificationRepository
from tests.integration.serve_shell_support import (
    WAIT_SECONDS,
    autologon_registry,
    failing_task_runner,
    failing_timesync_runner,
    fake_task_runner,
    global_event_payloads,
    global_event_rows,
    global_events,
    missing_timesync_runner,
    skewed_timesync_runner,
    spawn_serve,
    stderr_of,
    wait_for_file,
    wait_for_heartbeat_pid,
)


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
