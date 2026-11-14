"""ServeLoop 循环级运行时：执行阻塞期间的心跳节律、停机阶段边界与卡死执行线程收口。

三条用例都在属主线程里装配真 Supervisor 与 ServeLoop，主线程只做确定性同步与
外部可观测断言：门上脚本控制执行进度、心跳写手记录在途运行、时序常量缩到亚秒级。
启动序与真子进程场景见 test_serve_shell.py，停机强制终止见 test_serve_shutdown_force_abort.py。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from contracts import PhaseName, RunState
from core.clock import VirtualClock
from core.hostops import serve
from core.hostops.serve import (
    HeartbeatWriter,
    ServeLoop,
    ServeLoopParts,
)
from core.jobs.run_service import JobRunService
from core.logging import JsonLineLogger
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeScriptAdapter
from storage import migrate, open_database
from tests.integration.serve_shell_support import (
    THREAD_T0,
    WAIT_SECONDS,
    _fake_registry,
    _GatedScript,
    _loop_options,
    _NoopHeartbeat,
    _RecordingWriter,
    _run_state,
    _seed_run,
    _seed_template,
    _ShutdownThenExitScript,
)

_STABLE_DISK_FREE = 100 * 1024**3


@dataclass(frozen=True, slots=True)
class _LoopPlan:
    """循环级用例的装配单：心跳写手、模板数、各运行的发生点与监督器日志。"""

    writer: _RecordingWriter | _NoopHeartbeat
    templates: int = 1
    runs: tuple[tuple[str, datetime], ...] = ()
    supervisor_log: JsonLineLogger | None = None


@dataclass(slots=True)
class _LoopFixture:
    """属主线程装配出的句柄：主线程等 ready、读种子 id、看退出码。"""

    thread: threading.Thread
    ready: threading.Event
    outcome: dict[str, object]
    seeded: dict[str, str]


def _start_loop(
    tmp_path: Path, script: FakeScriptAdapter, plan: _LoopPlan, stop: threading.Event
) -> _LoopFixture:
    """在属主线程里装配 ServeLoop 并启动：ready 置位即装配完成，退出码落 outcome。"""
    ready = threading.Event()
    outcome: dict[str, object] = {}
    seeded: dict[str, str] = {}
    database = tmp_path / "backstop.sqlite"

    def owner() -> None:
        connection = open_database(database)
        try:
            migrate(connection)
            clock = VirtualClock(THREAD_T0)
            for index in range(1, plan.templates + 1):
                _seed_template(connection, f"template-{index}", f"env-{index}")
            runs = JobRunService(connection, clock)
            for index, (key, scheduled_at) in enumerate(plan.runs, start=1):
                seeded[f"run_{index}"] = _seed_run(
                    runs, f"template-{index}", key=key, scheduled_at=scheduled_at
                )
            supervisor = Supervisor(
                connection,
                clock,
                _fake_registry(script),
                data_root=tmp_path,
                log=plan.supervisor_log,
                disk_free=lambda: _STABLE_DISK_FREE,
                shutdown=stop.is_set,
            )
            loop = ServeLoop(
                ServeLoopParts(
                    supervisor,
                    runs,
                    cast(HeartbeatWriter, plan.writer),
                    _loop_options(clock, stop),
                )
            )
            ready.set()
            outcome["code"] = loop.run()
        finally:
            connection.close()

    thread = threading.Thread(target=owner, name="serve-owner", daemon=True)
    thread.start()
    return _LoopFixture(thread=thread, ready=ready, outcome=outcome, seeded=seeded)


@dataclass(frozen=True, slots=True)
class _BoundaryRows:
    """停机边界一次读齐的外部可观测行：中断事件、租约、阶段与未来运行。"""

    interrupted_payloads: list[str]
    lease_state: str | None
    phases: list[str]
    future_state: str | None
    future_phases: list[str]


def _read_boundary(database: Path, run_id: str, future_id: str) -> _BoundaryRows:
    """读停机边界用例断言所需的全部行：run_events、租约、阶段表与未来运行。"""
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
        future = reader.execute("SELECT state FROM job_runs WHERE id = ?", (future_id,)).fetchone()
        future_phases = reader.execute(
            "SELECT phase FROM run_phases WHERE run_id = ? ORDER BY rowid", (future_id,)
        ).fetchall()
    finally:
        reader.close()
    return _BoundaryRows(
        interrupted_payloads=[str(row[0]) for row in interrupted],
        lease_state=None if lease is None else str(lease[0]),
        phases=[str(row[0]) for row in phases],
        future_state=None if future is None else str(future[0]),
        future_phases=[str(row[0]) for row in future_phases],
    )


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


def test_serve_loop_writes_heartbeat_while_run_blocks_in_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """一次长运行不占住 tick：阻塞期间心跳照写、轮次照进，运行仍处 executing。"""
    monkeypatch.setattr(serve, "_HEARTBEAT_INTERVAL", timedelta(0))
    database = tmp_path / "backstop.sqlite"
    adapter = _GatedScript()
    writes: list[dict[str, Any]] = []
    stop = threading.Event()
    fixture = _start_loop(
        tmp_path,
        adapter,
        _LoopPlan(writer=_RecordingWriter(writes), runs=(("blocked", THREAD_T0),)),
        stop,
    )
    run_id = ""
    try:
        assert fixture.ready.wait(WAIT_SECONDS), "serve 循环没有就绪"
        run_id = fixture.seeded["run_1"]
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
        fixture.thread.join(WAIT_SECONDS)
    assert not fixture.thread.is_alive(), "循环没有收尾"
    assert fixture.outcome.get("code") == 0


def test_shutdown_writes_single_interrupt_at_phase_boundary(tmp_path: Path) -> None:
    """执行中停机：阶段跑完写恰一条 run.interrupted；状态与租约留给下次启动回收。"""
    database = tmp_path / "backstop.sqlite"
    stop = threading.Event()
    adapter = _ShutdownThenExitScript(stop)
    fixture = _start_loop(
        tmp_path,
        adapter,
        _LoopPlan(
            writer=_NoopHeartbeat(),
            templates=2,
            runs=(("interrupt", THREAD_T0), ("future", THREAD_T0 + timedelta(hours=1))),
        ),
        stop,
    )
    run_id = ""
    try:
        assert fixture.ready.wait(WAIT_SECONDS), "serve 循环没有就绪"
        run_id = fixture.seeded["run_1"]

        def interrupted_once() -> bool:
            return _interrupt_count(database, run_id) >= 1

        deadline = time.monotonic() + WAIT_SECONDS
        while time.monotonic() < deadline and not interrupted_once():
            time.sleep(0.05)
        assert interrupted_once(), "停机收尾事件没有写出"
    finally:
        stop.set()
        fixture.thread.join(WAIT_SECONDS)
    assert not fixture.thread.is_alive(), "循环没有收尾"
    assert fixture.outcome.get("code") == 0

    rows = _read_boundary(database, run_id, fixture.seeded["run_2"])
    assert len(rows.interrupted_payloads) == 1, (
        f"run.interrupted 条数不对：{len(rows.interrupted_payloads)}"
    )
    assert json.loads(rows.interrupted_payloads[0])["reason"] == "shutdown"
    assert _run_state(database, run_id) == RunState.EXECUTING.value
    assert rows.lease_state == "leased"
    assert rows.phases == ["prepare", "execute"]
    # 停机置位前 tick 可能已把未来运行派发出去：它最多走完 prepare 停在 waiting
    # 等到点，不会进入执行；停机本身不推进它。
    assert rows.future_state in {RunState.QUEUED.value, RunState.WAITING.value}, (
        f"未来运行被推进到 {rows.future_state}；"
        f"阶段行 {len(rows.future_phases)} 条：{rows.future_phases}"
    )
    assert set(rows.future_phases) <= {PhaseName.PREPARE.value}, (
        f"未来运行出现准备之后的阶段行：{rows.future_phases}（状态 {rows.future_state}）"
    )


def test_stuck_executor_times_out_bounded_wait_and_exits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """卡死的执行线程：等待到模块上限即放弃，记日志与事件，进程照常退出。"""
    monkeypatch.setattr(serve, "_EXECUTOR_JOIN_SECONDS", 0.2)
    database = tmp_path / "backstop.sqlite"
    log_path = tmp_path / "logs" / "core.jsonl"
    adapter = _GatedScript()
    stop = threading.Event()
    fixture = _start_loop(
        tmp_path,
        adapter,
        _LoopPlan(
            writer=_NoopHeartbeat(),
            runs=(("stuck", THREAD_T0),),
            supervisor_log=JsonLineLogger(log_path),
        ),
        stop,
    )
    run_id = ""
    try:
        assert fixture.ready.wait(WAIT_SECONDS), "serve 循环没有就绪"
        run_id = fixture.seeded["run_1"]
        assert adapter.entered.wait(WAIT_SECONDS), "运行没有进入执行"
        stop.set()
    finally:
        fixture.thread.join(WAIT_SECONDS)
        adapter.release.set()
    assert not fixture.thread.is_alive(), "等待上限没有收口循环"
    assert fixture.outcome.get("code") == 0
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
