"""停机收尾：当前阶段跑完写清结果，在阶段边界写 run.interrupted；不改状态、不动租约。"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

from contracts import (
    EnvironmentRef,
    PhaseResult,
    PhaseState,
    RunContext,
    RunState,
    ScriptHandle,
    ScriptObservation,
    ScriptState,
)
from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest
from core.clock import VirtualClock
from core.hostops.serve import run_serve
from core.jobs.phases import PhaseTracker
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from core.worker.executor import WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository
from tests.integration.test_serve_api import TOKEN, configure, serve_options
from tests.supervisor_support import tick_and_settle

T0 = datetime(2026, 10, 30, 3, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
_STABLE_DISK_FREE = 100 * 1024**3


def _insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
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


def _create_run(runs: JobRunService, *, scheduled_at: datetime, key: str) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    return record.id


class _ShutdownOnObserveScript(FakeScriptAdapter):
    """第一次观察时置位停机事件，模拟 executing 阶段收到停机信号。"""

    def __init__(self, stop: threading.Event) -> None:
        super().__init__()
        self._stop = stop

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self._stop.set()
        return ScriptObservation(state=ScriptState.RUNNING, since=ctx.clock.utc_now())


class _ShutdownThenExitScript(FakeScriptAdapter):
    """第一次观察时置位停机事件，脚本随即正常结束：停机要等执行阶段跑完。"""

    def __init__(self, stop: threading.Event) -> None:
        super().__init__()
        self._stop = stop

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self._stop.set()
        return super().observe(handle, ctx)


class _ShutdownAfterPrepareProvider(FakeEnvironmentProvider):
    """准备成功后置位停机事件，模拟两次阶段之间收到停机信号。"""

    def __init__(self, stop: threading.Event) -> None:
        super().__init__()
        self._stop = stop

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        result = super().prepare(env, ctx)
        self._stop.set()
        return result


def test_shutdown_during_execution_writes_interrupted_and_keeps_lease(
    tmp_path: Path,
) -> None:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    try:
        _insert_template(connection)
        run_id = _create_run(runs, scheduled_at=T0, key="shutdown-drain")
        runs.enqueue(run_id)
        stop = threading.Event()
        registry = PluginRegistry()
        registry.register(
            PluginCapabilities(
                plugin_id="fake",
                environment=FakeEnvironmentProvider(),
                script=_ShutdownOnObserveScript(stop),
                verifier=FakeVerifier(),
            )
        )
        supervisor = Supervisor(
            connection,
            clock,
            registry,
            data_root=tmp_path,
            disk_free=lambda: _STABLE_DISK_FREE,
            shutdown=stop.is_set,
        )
        tick_and_settle(supervisor)

        record = runs.get(run_id)
        assert record.state is RunState.EXECUTING, f"停机后运行状态被改写：{record.state.value}"
        assert record.finished_at is None
        events = runs.events(run_id)
        interrupted = [event for event in events if event.type == "run.interrupted"]
        assert len(interrupted) == 1, f"run.interrupted 条数不对：{[e.type for e in events]}"
        assert interrupted[0].payload == {"run_id": run_id, "reason": "shutdown"}
        assert not runs.cancel_requested(run_id), "停机收尾被当成用户取消"
        # 租约活到 cleanup 释放（docs/09）：停机不动它，由下次启动扫描回收孤儿租约。
        assert LeaseService(connection, clock).active_for_run(run_id), "停机释放了运行租约"
        assert not any(event.type == "run.finished" for event in events)
    finally:
        connection.close()


def _waiting_run_with_lease(data_root: Path) -> str:
    """建一条等待到点、持有租约的运行：停机时没有执行中的运行。"""
    connection = open_database(data_root / "backstop.sqlite")
    clock = VirtualClock(T0)
    try:
        _insert_template(connection)
        runs = JobRunService(connection, clock)
        run_id = _create_run(runs, scheduled_at=T0 + timedelta(hours=1), key="shutdown-idle")
        runs.enqueue(run_id)
        runs.transition(run_id, RunState.PREPARING)
        ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
        LeaseService(connection, clock).acquire(run_id, (ENV_ID,))
        runs.transition(run_id, RunState.WAITING)
        return run_id
    finally:
        connection.close()


def test_shutdown_without_active_run_exits_zero_and_keeps_waiting_run(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    configure(data_root, {"api.host": "127.0.0.1", "api.port": 0})
    run_id = _waiting_run_with_lease(data_root)
    stop = threading.Event()
    stop.set()
    code = run_serve(data_root, serve_options(clock=VirtualClock(T0), stop=stop))
    assert code == 0
    reader = sqlite3.connect(data_root / "backstop.sqlite")
    try:
        active = reader.execute(
            "SELECT COUNT(*) FROM resource_leases WHERE state IN ('leased', 'expiring')"
        ).fetchone()
        interrupted = reader.execute(
            "SELECT COUNT(*) FROM run_events WHERE type = 'run.interrupted'"
        ).fetchone()
        state = reader.execute("SELECT state FROM job_runs WHERE id = ?", (run_id,)).fetchone()
    finally:
        reader.close()
    # 等待中的运行不参与收尾：租约继续持有，重启后接着跑（docs/09 租约）。
    assert active is not None and int(active[0]) == 1
    assert interrupted is not None and int(interrupted[0]) == 0
    assert state is not None and str(state[0]) == "waiting"


def test_drain_endpoint_sets_stop_event_and_due_run_stays_queued(tmp_path: Path) -> None:
    """排水端点置位的停机事件与 SIGINT 同源：不接新作业，在跑的到边界收尾。"""
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=T0)
        _insert_template(connection)
        runs = JobRunService(connection, VirtualClock(T0))
        run_id = _create_run(runs, scheduled_at=T0, key="drain-queue")
        stop = threading.Event()

        def request() -> int:
            stop.set()
            return 0

        app = ApiApp(tmp_path / "state.db", clock=VirtualClock(T0), drain_requester=request)
        response = app.handle(
            ApiRequest(
                method="POST",
                path="/api/v1/host/drain",
                query={},
                headers={"authorization": f"Bearer {TOKEN}"},
                body=b"",
            )
        )
        assert response.status == 200
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
            shutdown=stop.is_set,
        )
        tick_and_settle(supervisor)

        record = runs.get(run_id)
        assert record.state is RunState.SCHEDULED, f"排水后到点的运行仍被执行：{record.state.value}"
    finally:
        connection.close()


def test_shutdown_during_execution_finishes_phase_then_interrupts(tmp_path: Path) -> None:
    """执行中收到停机：当前阶段跑完写清结果再退出，不开验证，interrupt 只一条。"""
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    try:
        _insert_template(connection)
        run_id = _create_run(runs, scheduled_at=T0, key="shutdown-phase-done")
        runs.enqueue(run_id)
        stop = threading.Event()
        registry = PluginRegistry()
        registry.register(
            PluginCapabilities(
                plugin_id="fake",
                environment=FakeEnvironmentProvider(),
                script=_ShutdownThenExitScript(stop),
                verifier=FakeVerifier(),
            )
        )
        supervisor = Supervisor(
            connection,
            clock,
            registry,
            data_root=tmp_path,
            disk_free=lambda: _STABLE_DISK_FREE,
            shutdown=stop.is_set,
        )
        tick_and_settle(supervisor)

        record = runs.get(run_id)
        assert record.state is RunState.EXECUTING, f"停机后运行状态被改写：{record.state.value}"
        assert record.finished_at is None
        phases = PhaseTracker(connection, clock).for_run(run_id)
        assert sorted(row.phase for row in phases) == ["execute", "prepare"], (
            f"停机后开了验证或清理：{phases}"
        )
        execute = next(row for row in phases if row.phase == "execute")
        assert execute.state is PhaseState.OK, f"执行阶段没有写清结果：{execute.state.value}"
        assert execute.finished_at is not None, "执行阶段行还开着"
        events = runs.events(run_id)
        interrupted = [event for event in events if event.type == "run.interrupted"]
        assert len(interrupted) == 1, f"run.interrupted 条数不对：{[e.type for e in events]}"
        assert interrupted[0].payload == {"run_id": run_id, "reason": "shutdown"}
        assert not any(event.type == "run.finished" for event in events)
        assert LeaseService(connection, clock).active_for_run(run_id), "停机释放了运行租约"
    finally:
        connection.close()


def test_shutdown_between_phases_starts_no_new_phase(tmp_path: Path) -> None:
    """两次阶段之间收到停机：当前阶段已收尾，不再开新阶段，直接写 interrupt 退出。"""
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    try:
        _insert_template(connection)
        run_id = _create_run(runs, scheduled_at=T0, key="shutdown-between-phases")
        runs.enqueue(run_id)
        stop = threading.Event()
        registry = PluginRegistry()
        registry.register(
            PluginCapabilities(
                plugin_id="fake",
                environment=_ShutdownAfterPrepareProvider(stop),
                script=FakeScriptAdapter(),
                verifier=FakeVerifier(),
            )
        )
        supervisor = Supervisor(
            connection,
            clock,
            registry,
            data_root=tmp_path,
            disk_free=lambda: _STABLE_DISK_FREE,
            shutdown=stop.is_set,
        )
        tick_and_settle(supervisor)

        record = runs.get(run_id)
        assert record.state is RunState.WAITING, f"停机后运行状态被改写：{record.state.value}"
        phases = PhaseTracker(connection, clock).for_run(run_id)
        assert [row.phase for row in phases] == ["prepare"], f"停机后开了新阶段：{phases}"
        assert phases[0].state is PhaseState.OK and phases[0].finished_at is not None
        events = runs.events(run_id)
        interrupted = [event for event in events if event.type == "run.interrupted"]
        assert len(interrupted) == 1, f"run.interrupted 条数不对：{[e.type for e in events]}"
        assert interrupted[0].payload == {"run_id": run_id, "reason": "shutdown"}
        assert LeaseService(connection, clock).active_for_run(run_id), "停机释放了运行租约"
    finally:
        connection.close()


def test_repeated_shutdown_signal_writes_single_interrupt(tmp_path: Path) -> None:
    """重复收到停机信号：幂等，同一运行不写第二条 interrupt。"""
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    try:
        _insert_template(connection)
        run_id = _create_run(runs, scheduled_at=T0, key="shutdown-repeat")
        runs.enqueue(run_id)
        stop = threading.Event()
        stop.set()
        registry = PluginRegistry()
        registry.register(
            PluginCapabilities(
                plugin_id="fake",
                environment=FakeEnvironmentProvider(),
                script=FakeScriptAdapter(),
                verifier=FakeVerifier(),
            )
        )
        executor = WorkerExecutor(
            connection,
            clock,
            registry,
            data_root=tmp_path,
            shutdown=stop.is_set,
        )
        executor.execute(run_id)
        executor.execute(run_id)

        record = runs.get(run_id)
        assert record.state is RunState.QUEUED, f"停机后运行状态被改写：{record.state.value}"
        interrupted = [event for event in runs.events(run_id) if event.type == "run.interrupted"]
        assert len(interrupted) == 1, (
            f"重复信号写了多条 interrupt：{[e.payload for e in interrupted]}"
        )
        assert interrupted[0].payload == {"run_id": run_id, "reason": "shutdown"}
    finally:
        connection.close()
