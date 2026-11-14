"""supervisor 排水阶段收尾的强制终止：宽限到点终止卡死插件的 worker 入口并留下记账。

判据全部是外部可观测结果：终止入口收到的插件名单、全局事件条数、结构化日志条数与
运行 id；执行线程卡在放行门上做确定性卡死，时序常量缩到亚秒级（等待上限 1 秒、收尾
宽限 0.3 秒）。非停机退场与无在跑线程时不触达终止入口。
serve 进程级的杀树场景见 test_serve_shutdown_force_abort.py。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from contracts import RunContext, ScriptHandle, ScriptObservation
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.logging import JsonLineLogger
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from core.supervisor_shutdown import SHUTDOWN_FORCE_ABORT_EVENT
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from tests.integration.test_serve_shutdown_force_abort import (
    _JOIN_TIMEOUT_EVENT,
    GRACE_SECONDS,
    JOIN_SECONDS,
    T0,
    WAIT_SECONDS,
)
from tests.supervisor_support import JOIN_TIMEOUT_SECONDS

_STABLE_DISK_FREE = 100 * 1024**3


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


def _seed_stuck_run(connection: sqlite3.Connection) -> str:
    """种一条指向 fake 插件的排队运行，返回运行 id。"""
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
    return record.id


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
    run_id = _seed_stuck_run(connection)
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
    return _StuckSupervisor(supervisor, connection, stop, run_id, adapter)


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
