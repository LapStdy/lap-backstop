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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    EnvironmentRef,
    Logger,
    RunContext,
)
from core.api.store import TOKEN_KEY
from core.clock import VirtualClock
from core.hostops import serve as serve_module
from core.hostops.serve import run_serve
from core.jobs.run_service import JobRunService
from core.plugins.channel import WorkerChannel
from core.plugins.rpc_host import EnvironmentProxy, RpcPluginHost
from core.plugins.worker_options import WorkerConfig
from core.supervisor_shutdown import SHUTDOWN_FORCE_ABORT_EVENT
from storage import open_database
from storage.repositories.job_runs import NewRun
from tests.integration.serve_shell_support import global_event_payloads
from tests.integration.test_serve_api import TOKEN, configure, serve_options

REPO_ROOT = Path(__file__).resolve().parents[2]
PLUGIN_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "plugins"
T0 = datetime(2026, 11, 2, 9, 0, tzinfo=UTC)
WAIT_SECONDS = 30.0
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


def _prepared_hang_serve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path, Path, str]:
    """钉住停机时序常量、装好 hang 插件并种一条排队运行，返回数据根、插件根、pid 文件与运行 id。"""
    monkeypatch.setattr(serve_module, "_EXECUTOR_JOIN_SECONDS", JOIN_SECONDS)
    monkeypatch.setattr(serve_module, "_EXECUTOR_ABORT_GRACE_SECONDS", GRACE_SECONDS)
    data_root = tmp_path / "data"
    plugins_root = tmp_path / "plugins"
    shutil.copytree(PLUGIN_FIXTURES / "hang", plugins_root / "hang")
    configure(data_root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    pids_file = tmp_path / "pids.json"
    return data_root, plugins_root, pids_file, _seed_hang_run(data_root, pids_file)


def _serve_owner_thread(
    data_root: Path,
    plugins_root: Path,
    stop: threading.Event,
    captured_hosts: list[RpcPluginHost],
) -> tuple[threading.Thread, dict[str, Any], dict[str, object]]:
    """后台线程里跑 serve，返回线程与 worker 进程号、退出码的带回容器。"""
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
    return thread, running, outcome


def _confirmed_pids(pids_file: Path, running: dict[str, Any]) -> tuple[int, int]:
    """等 pid 文件落地并确认 worker 与其子进程在终止前存活，返回 worker 与子进程号。"""
    _wait_for_file(pids_file)
    pids = json.loads(pids_file.read_text(encoding="utf-8"))
    assert running["worker_pid"] is not None, "worker 进程没有随插件宿主启动"
    assert _pid_alive(int(pids["pid"])), "worker 进程应在被终止前存活"
    assert _pid_alive(int(pids["child_pid"])), "worker 的子进程应在被终止前存活"
    return int(running["worker_pid"]), int(pids["child_pid"])


def _measured_abort(
    thread: threading.Thread,
    stop: threading.Event,
    pids_file: Path,
    running: dict[str, Any],
) -> tuple[float, int, int]:
    """发停机量总时长，返回经过时间与 worker、子进程号；前置断言失败也要把 serve 收场。"""
    worker_pid, child_pid = 0, 0
    elapsed = float("inf")
    try:
        worker_pid, child_pid = _confirmed_pids(pids_file, running)
        started = time.monotonic()
        stop.set()
        thread.join(WAIT_SECONDS)
        elapsed = time.monotonic() - started
    finally:
        stop.set()
        thread.join(WAIT_SECONDS)
    return elapsed, worker_pid, child_pid


def _assert_shutdown_events(database: Path, run_id: str) -> None:
    """全局事件记账：强制终止恰一条且目标正确，不再记执行器等待超时。"""
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


def _assert_run_drain_state(database: Path, run_id: str) -> None:
    """运行收尾记账：一条 shutdown 原因的 run.interrupted，运行停在 preparing，租约保持 leased。"""
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


def test_force_abort_kills_worker_tree_and_drains_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    captured_hosts: list[RpcPluginHost],
) -> None:
    """卡死调用下停机：宽限先到，强制终止后进程树消失、serve 提前收场且事件恰一条。"""
    data_root, plugins_root, pids_file, run_id = _prepared_hang_serve(tmp_path, monkeypatch)
    stop = threading.Event()
    thread, running, outcome = _serve_owner_thread(data_root, plugins_root, stop, captured_hosts)
    elapsed, worker_pid, child_pid = _measured_abort(thread, stop, pids_file, running)
    assert not thread.is_alive(), "强制终止没有让 serve 收场"
    assert outcome.get("code") == 0
    assert elapsed < 2.0, f"总时长 {elapsed:.2f} 秒顶到了等待上限，强制终止没有生效"
    assert elapsed >= GRACE_SECONDS - 0.05, "宽限期没有先于强制终止耗完"
    assert _wait_pid_gone(worker_pid), "worker 进程在强制终止后仍存活"
    assert _wait_pid_gone(child_pid), "worker 的子进程在强制终止后仍存活"
    database = data_root / "backstop.sqlite"
    _assert_shutdown_events(database, run_id)
    _assert_run_drain_state(database, run_id)
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
