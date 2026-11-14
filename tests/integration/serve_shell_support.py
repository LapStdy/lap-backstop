"""serve 外壳测试家族共享的脚手架：假 runner、启动注入项、事件读数与子进程收口。

不带 test_ 前缀不进收集；只被测试模块取用，不反向 import 测试模块。
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from contracts import RunContext, ScriptHandle, ScriptObservation
from core.clock import VirtualClock
from core.hostops import tasks, timesync
from core.hostops.heartbeat import read_heartbeat
from core.hostops.serve import ServeOptions
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage.repositories.job_runs import NewRun

REPO_ROOT = Path(__file__).resolve().parents[2]
WAIT_SECONDS = 30.0
THREAD_T0 = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)


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


class _RecordingWriter:
    """记录每次心跳的在途运行；主线程按内容轮询，不以写入笔数置位。"""

    def __init__(self, writes: list[dict[str, Any]]) -> None:
        self._writes = writes

    def write(self, *, last_tick_at: datetime, active_run: str | None) -> None:
        self._writes.append({"last_tick": last_tick_at, "active_run": active_run})


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
