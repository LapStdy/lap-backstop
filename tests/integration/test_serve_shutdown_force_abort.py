"""停机收尾的强制终止：宽限到点结束承载卡死调用的 worker 进程树，调用以失败返回。

判据全部是外部可观测结果：进程是否存活（pid 文件加内核句柄查询）、serve 退出码、
全局与运行事件条数、运行状态与租约行；时序常量用 monkeypatch 缩到亚秒级，宽限先于
强制终止、总时长短于等待上限都按实测经过时间断言，不同步内部窗口。
"""

from __future__ import annotations

import ctypes
import json
import os
import shutil
import sqlite3
import sys
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    EnvironmentRef,
    Logger,
    RunContext,
    ScriptHandle,
    ScriptObservation,
)
from core.api.store import TOKEN_KEY
from core.clock import VirtualClock
from core.hostops import serve as serve_module
from core.hostops.serve import run_serve
from core.jobs.run_service import JobRunService
from core.logging import JsonLineLogger
from core.plugins.channel import WorkerChannel
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.plugins.rpc_host import EnvironmentProxy, RpcPluginHost
from core.plugins.worker_options import WorkerConfig
from core.supervisor import Supervisor
from core.supervisor_shutdown import SHUTDOWN_FORCE_ABORT_EVENT
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from tests.integration.test_serve_api import TOKEN, configure, serve_options
from tests.integration.test_serve_shell import global_event_payloads
from tests.supervisor_support import JOIN_TIMEOUT_SECONDS

REPO_ROOT = Path(__file__).resolve().parents[2]
PLUGIN_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "plugins"
T0 = datetime(2026, 11, 2, 9, 0, tzinfo=UTC)
WAIT_SECONDS = 30.0
_STABLE_DISK_FREE = 100 * 1024**3
# monkeypatch 后的时序常量：总等待上限 1 秒，收尾宽限 0.3 秒，全部亚秒到秒级。
JOIN_SECONDS = 1.0
GRACE_SECONDS = 0.3
_JOIN_TIMEOUT_EVENT = "supervisor.executor_join_timeout"
_SYNCHRONIZE = 0x00100000
_WAIT_TIMEOUT = 0x00000102


def _pid_alive(pid: int) -> bool:
    """进程是否仍在运行；Windows 用内核句柄查询，其它平台用信号 0。

    Windows 的 os.kill(pid, 0) 不是存活探测：0 会被当作 CTRL_C_EVENT 发往控制台
    进程组，连带打断 pytest 自身，只能走 OpenProcess + WaitForSingleObject。
    """
    if sys.platform == "win32":
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(_SYNCHRONIZE, False, pid)
        if not handle:
            return False
        try:
            return kernel32.WaitForSingleObject(handle, 0) == _WAIT_TIMEOUT
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_pid_gone(pid: int, *, timeout: float = WAIT_SECONDS) -> bool:
    """轮询等进程消失；进程退出后句柄短时仍可打开，单次探测会误判。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.05)
    return not _pid_alive(pid)


def _wait_for_file(path: Path, *, timeout: float = WAIT_SECONDS) -> None:
    """轮询等文件出现；超时抛 AssertionError。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.is_file():
            return
        time.sleep(0.05)
    raise AssertionError(f"{timeout:g} 秒内未出现 {path}")


def _seed_hang_run(data_root: Path, pids_file: Path) -> str:
    """种一条指向 hang 插件的排队运行；pid 文件路径随模板 config 进 worker。"""
    connection = open_database(data_root / "backstop.sqlite")
    try:
        connection.execute(
            "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
            "config_schema_version, verification_policy, recovery_policy, enabled, "
            "created_at, updated_at) VALUES (?, '日常', 'hang', 'env-1', ?, '1', ?, '{}', 1, ?, ?)",
            (
                "template-hang",
                json.dumps({"pids_file": str(pids_file)}),
                json.dumps(
                    {
                        "id": "hang-policy",
                        "verifier_plugin_id": "hang",
                        "required_evidence": ["log"],
                        "params": {},
                    }
                ),
                T0.isoformat(),
                T0.isoformat(),
            ),
        )
        runs = JobRunService(connection, VirtualClock(T0))
        record, _ = runs.create_scheduled(
            NewRun(
                template_id="template-hang",
                schedule_id=None,
                scheduled_at=T0,
                created_at=T0,
                idempotency_key="hang-run",
                priority=0,
                deadline_at=None,
            )
        )
        runs.enqueue(record.id)
        return record.id
    finally:
        connection.close()


@pytest.fixture
def captured_hosts(monkeypatch: pytest.MonkeyPatch) -> list[RpcPluginHost]:
    """截住 serve 装配的宿主实例；装配语义仍由真实 RpcPluginHost 执行。"""
    instances: list[RpcPluginHost] = []

    class CapturingHost(RpcPluginHost):
        def __init__(
            self,
            plugins_root: Path,
            plugin_ids: Any,
            *,
            log: Logger | None = None,
        ) -> None:
            super().__init__(plugins_root, plugin_ids, log=log)
            instances.append(self)

    monkeypatch.setattr(serve_module, "RpcPluginHost", CapturingHost)
    return instances


def test_force_abort_kills_worker_tree_and_drains_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_hosts: list[RpcPluginHost],
) -> None:
    """卡死调用下停机：宽限先到，强制终止后进程树消失、serve 提前收场且事件恰一条。"""
    monkeypatch.setattr(serve_module, "_EXECUTOR_JOIN_SECONDS", JOIN_SECONDS)
    monkeypatch.setattr(serve_module, "_EXECUTOR_ABORT_GRACE_SECONDS", GRACE_SECONDS)
    data_root = tmp_path / "data"
    plugins_root = tmp_path / "plugins"
    shutil.copytree(PLUGIN_FIXTURES / "hang", plugins_root / "hang")
    configure(data_root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    pids_file = tmp_path / "pids.json"
    run_id = _seed_hang_run(data_root, pids_file)
    stop = threading.Event()
    running: dict[str, Any] = {}
    outcome: dict[str, object] = {}

    def on_listening(host: str, port: int) -> None:
        running["worker_pid"] = captured_hosts[0].channels["hang"].pid

    def owner() -> None:
        outcome["code"] = run_serve(
            data_root,
            serve_options(
                max_ticks=None,
                stop=stop,
                plugins_root=plugins_root,
                on_api_listening=on_listening,
            ),
        )

    thread = threading.Thread(target=owner, name="serve-owner", daemon=True)
    thread.start()
    elapsed = float("inf")
    try:
        _wait_for_file(pids_file)
        pids = json.loads(pids_file.read_text(encoding="utf-8"))
        assert running["worker_pid"] is not None, "worker 进程没有随插件宿主启动"
        assert _pid_alive(int(pids["pid"])), "worker 进程应在被终止前存活"
        assert _pid_alive(int(pids["child_pid"])), "worker 的子进程应在被终止前存活"
        started = time.monotonic()
        stop.set()
        thread.join(WAIT_SECONDS)
        elapsed = time.monotonic() - started
    finally:
        stop.set()
        thread.join(WAIT_SECONDS)
    assert not thread.is_alive(), "强制终止没有让 serve 收场"
    assert outcome.get("code") == 0
    assert elapsed < 2.0, f"总时长 {elapsed:.2f} 秒顶到了等待上限，强制终止没有生效"
    assert elapsed >= GRACE_SECONDS - 0.05, "宽限期没有先于强制终止耗完"

    worker_pid = int(running["worker_pid"])
    assert _wait_pid_gone(worker_pid), "worker 进程在强制终止后仍存活"
    assert _wait_pid_gone(int(pids["child_pid"])), "worker 的子进程在强制终止后仍存活"

    database = data_root / "backstop.sqlite"
    aborts = [
        payload
        for payload in global_event_payloads(database)
        if payload.get("event") == SHUTDOWN_FORCE_ABORT_EVENT
    ]
    assert len(aborts) == 1, f"强制终止事件条数不对：{len(aborts)}"
    assert aborts[0]["payload"] == {"plugin_ids": ["hang"], "run_ids": [run_id]}
    timeouts = [
        payload
        for payload in global_event_payloads(database)
        if payload.get("event") == _JOIN_TIMEOUT_EVENT
    ]
    assert timeouts == [], "强制终止生效时不应再记等待超时"

    reader = sqlite3.connect(database)
    try:
        interrupted = reader.execute(
            "SELECT payload FROM run_events WHERE run_id = ? AND type = 'run.interrupted'",
            (run_id,),
        ).fetchall()
        state = reader.execute("SELECT state FROM job_runs WHERE id = ?", (run_id,)).fetchone()
        lease = reader.execute(
            "SELECT state FROM resource_leases WHERE run_id = ?", (run_id,)
        ).fetchone()
    finally:
        reader.close()
    assert len(interrupted) == 1, f"run.interrupted 条数不对：{len(interrupted)}"
    assert json.loads(str(interrupted[0][0]))["reason"] == "shutdown"
    assert state is not None and str(state[0]) == "preparing"
    assert lease is not None and str(lease[0]) == "leased"

    host = captured_hosts[0]
    assert host.channels == {}
    host.close()


def test_shutdown_without_stuck_call_skips_force_abort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_hosts: list[RpcPluginHost],
) -> None:
    """停机时没有在跑执行线程：不记强制终止事件，worker 也不被终止入口碰过。"""
    monkeypatch.setattr(serve_module, "_EXECUTOR_JOIN_SECONDS", JOIN_SECONDS)
    monkeypatch.setattr(serve_module, "_EXECUTOR_ABORT_GRACE_SECONDS", GRACE_SECONDS)
    data_root = tmp_path / "data"
    plugins_root = tmp_path / "plugins"
    shutil.copytree(PLUGIN_FIXTURES / "hang", plugins_root / "hang")
    configure(data_root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    running: dict[str, Any] = {}

    def on_listening(host: str, port: int) -> None:
        running["worker_pid"] = captured_hosts[0].channels["hang"].pid

    stop = threading.Event()
    stop.set()
    code = run_serve(
        data_root,
        serve_options(
            max_ticks=None,
            stop=stop,
            plugins_root=plugins_root,
            on_api_listening=on_listening,
        ),
    )
    assert code == 0
    assert running["worker_pid"] is not None, "worker 进程应随插件宿主启动"
    payloads = global_event_payloads(data_root / "backstop.sqlite")
    assert not any(payload.get("event") == SHUTDOWN_FORCE_ABORT_EVENT for payload in payloads)
    assert not any(payload.get("event") == _JOIN_TIMEOUT_EVENT for payload in payloads)


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


@dataclass(slots=True)
class _StuckSupervisor:
    """带卡死执行线程的 supervisor 脚手架：连接、停机事件、运行 id 与放行门都在这里。"""

    supervisor: Supervisor
    connection: sqlite3.Connection
    stop: threading.Event
    run_id: str
    adapter: _GatedScript

    def settle(self) -> None:
        """放行执行器并等线程收尾，测试失败时也不泄漏线程。"""
        self.stop.set()
        self.adapter.release.set()
        self.supervisor.join_executors(JOIN_TIMEOUT_SECONDS)


def _stuck_supervisor(
    tmp_path: Path,
    *,
    shutdown: Any,
    abort_workers: Any,
    log: JsonLineLogger | None = None,
) -> _StuckSupervisor:
    """造一个执行线程卡在 observe 上的 supervisor；停机在进入执行后才置位。

    shutdown 传 None 时内部接同一个事件：派发时停机未置位，置位后才算停机收尾。
    """
    stop = threading.Event()
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', 'env-1', ?, '1', ?, '{}', 1, ?, ?)",
        (
            "template-1",
            json.dumps({"scenario": "instant"}),
            json.dumps(
                {
                    "id": "fake-policy",
                    "verifier_plugin_id": "fake",
                    "required_evidence": ["log"],
                    "params": {},
                }
            ),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )
    runs = JobRunService(connection, VirtualClock(T0))
    record, _ = runs.create_scheduled(
        NewRun(
            template_id="template-1",
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key="stuck",
            priority=0,
            deadline_at=None,
        )
    )
    runs.enqueue(record.id)
    registry = PluginRegistry()
    adapter = _GatedScript()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=adapter,
            verifier=FakeVerifier(),
        )
    )
    supervisor = Supervisor(
        connection,
        VirtualClock(T0),
        registry,
        data_root=tmp_path,
        log=log,
        disk_free=lambda: _STABLE_DISK_FREE,
        shutdown=(lambda: stop.is_set()) if shutdown is None else shutdown,
        abort_workers=abort_workers,
    )
    supervisor.tick()
    assert adapter.entered.wait(WAIT_SECONDS), "运行没有进入执行"
    stop.set()
    return _StuckSupervisor(supervisor, connection, stop, record.id, adapter)


def _global_payloads(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """读全局事件载荷，按写入顺序。"""
    rows = connection.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    return [dict(json.loads(str(row[0]))) for row in rows]


def _log_entries(log_path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_force_abort_records_event_and_targets_stuck_plugin(tmp_path: Path) -> None:
    """宽限到点：终止入口只收到卡死运行的插件，日志与事件各恰一条。"""
    log_path = tmp_path / "logs" / "core.jsonl"
    aborted: list[tuple[str, ...]] = []
    stuck = _stuck_supervisor(
        tmp_path,
        shutdown=None,
        abort_workers=lambda plugin_ids: aborted.append(tuple(plugin_ids)),
        log=JsonLineLogger(log_path),
    )
    try:
        stuck.supervisor.join_executors(JOIN_SECONDS, abort_grace_seconds=GRACE_SECONDS)
    finally:
        stuck.settle()
    assert aborted == [("fake",)], f"终止入口收到的插件不对：{aborted}"
    aborts = [
        payload
        for payload in _global_payloads(stuck.connection)
        if payload.get("event") == SHUTDOWN_FORCE_ABORT_EVENT
    ]
    assert len(aborts) == 1, f"强制终止事件条数不对：{len(aborts)}"
    assert aborts[0]["payload"] == {"plugin_ids": ["fake"], "run_ids": [stuck.run_id]}
    logged = [
        entry
        for entry in _log_entries(log_path)
        if entry.get("event") == SHUTDOWN_FORCE_ABORT_EVENT
    ]
    assert len(logged) == 1, f"强制终止日志条数不对：{logged}"
    assert logged[0]["plugin_ids"] == ["fake"]
    assert logged[0]["run_ids"] == [stuck.run_id]
    assert logged[0]["level"] == "ERROR"


def test_force_abort_leaves_bounded_wait_accounting_when_thread_stays_stuck(
    tmp_path: Path,
) -> None:
    """终止后线程仍卡死（假脚本不因杀 worker 解开）：等待顶到上限，超时记账照记。"""
    stuck = _stuck_supervisor(tmp_path, shutdown=None, abort_workers=lambda _ids: None)
    try:
        stuck.supervisor.join_executors(JOIN_SECONDS, abort_grace_seconds=GRACE_SECONDS)
    finally:
        stuck.settle()
    timeouts = [
        payload
        for payload in _global_payloads(stuck.connection)
        if payload.get("event") == _JOIN_TIMEOUT_EVENT
    ]
    assert len(timeouts) == 1, f"等待超时记账条数不对：{len(timeouts)}"
    assert timeouts[0]["payload"] == {"run_ids": [stuck.run_id]}


def test_force_abort_skipped_on_non_shutdown_exit(tmp_path: Path) -> None:
    """磁盘退场等非停机路径：线程卡死也只等待，不终止 worker、不记事件。"""
    aborted: list[tuple[str, ...]] = []
    stuck = _stuck_supervisor(
        tmp_path,
        shutdown=lambda: False,
        abort_workers=lambda plugin_ids: aborted.append(tuple(plugin_ids)),
    )
    try:
        stuck.supervisor.join_executors(JOIN_SECONDS, abort_grace_seconds=GRACE_SECONDS)
    finally:
        stuck.settle()
    assert aborted == [], "非停机退场不应触达终止入口"
    aborts = [
        payload
        for payload in _global_payloads(stuck.connection)
        if payload.get("event") == SHUTDOWN_FORCE_ABORT_EVENT
    ]
    assert aborts == [], "非停机退场不应记强制终止事件"


def test_force_abort_skipped_without_executor_threads(tmp_path: Path) -> None:
    """停机但无在跑执行线程：不触达终止入口，也不记强制终止事件。"""
    aborted: list[tuple[str, ...]] = []
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    supervisor = Supervisor(
        connection,
        VirtualClock(T0),
        registry,
        data_root=tmp_path,
        disk_free=lambda: _STABLE_DISK_FREE,
        shutdown=lambda: True,
        abort_workers=lambda plugin_ids: aborted.append(tuple(plugin_ids)),
    )
    try:
        supervisor.join_executors(JOIN_SECONDS, abort_grace_seconds=GRACE_SECONDS)
    finally:
        connection.close()
    assert aborted == [], "无执行线程时不应触达终止入口"


class _Cancel:
    def is_cancelled(self) -> bool:
        return False


class _QuietLogger:
    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


def _run_context(work_dir: Path) -> RunContext:
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="child",
        environment_id="env-child",
        config={},
        work_dir=work_dir,
        evidence_dir=work_dir / "evidence",
        deadline_at=T0 + timedelta(minutes=30),
        cancel=_Cancel(),
        clock=VirtualClock(T0),
        secrets={},
        log=_QuietLogger(),
        emit=lambda event, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


def test_channel_abort_ends_worker_tree_and_fails_the_call(tmp_path: Path) -> None:
    """通道强制终止：卡死的调用以失败返回，worker 拉起的子进程一并结束，重复关闭无副作用。"""
    channel = WorkerChannel("child", WorkerConfig(plugins_root=PLUGIN_FIXTURES))
    channel.start()
    worker_pid = channel.pid
    assert worker_pid is not None
    context = _run_context(tmp_path / "work")
    outcome: dict[str, object] = {}

    def caller() -> None:
        try:
            EnvironmentProxy(channel).probe(
                EnvironmentRef(id="env-child", provider_id="child"), context
            )
        except Exception as error:  # 断言在主线程：这里只把失败形态带回
            outcome["error"] = error

    thread = threading.Thread(target=caller, name="channel-caller", daemon=True)
    thread.start()
    try:
        _wait_for_file(tmp_path / "work" / "child.pid")
        child_pid = int((tmp_path / "work" / "child.pid").read_text(encoding="utf-8"))
        assert _pid_alive(child_pid), "worker 的子进程应在被终止前存活"
        channel.abort()
        thread.join(WAIT_SECONDS)
    finally:
        channel.abort()
        thread.join(WAIT_SECONDS)
    assert not thread.is_alive(), "强制终止没有让调用返回"
    assert isinstance(outcome.get("error"), Exception), "强制终止后调用应以失败返回"
    assert _wait_pid_gone(child_pid), "worker 的子进程在强制终止后仍存活"
    assert _wait_pid_gone(worker_pid), "worker 进程在强制终止后仍存活"
    assert channel.pid is None
    channel.close()
    channel.close()
