"""supervisor 排水阶段收尾的强制终止：宽限到点终止卡死插件的 worker 入口并留下记账。

判据全部是外部可观测结果：终止入口收到的插件名单、全局事件条数、结构化日志条数与
运行 id；执行线程卡在放行门上做确定性卡死，时序常量缩到亚秒级（等待上限 1 秒、收尾
宽限 0.3 秒）。非停机退场与无在跑线程时不触达终止入口。
serve 进程级的杀树场景见 test_serve_shutdown_force_abort.py。
"""

from __future__ import annotations

from pathlib import Path

from core.clock import VirtualClock
from core.logging import JsonLineLogger
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from core.supervisor_shutdown import SHUTDOWN_FORCE_ABORT_EVENT
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from tests.integration.test_serve_shutdown_force_abort import (
    _JOIN_TIMEOUT_EVENT,
    _STABLE_DISK_FREE,
    GRACE_SECONDS,
    JOIN_SECONDS,
    T0,
    _global_payloads,
    _log_entries,
    _stuck_supervisor,
)


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
