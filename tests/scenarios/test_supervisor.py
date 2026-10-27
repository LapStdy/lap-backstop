"""场景：核心 supervisor 的启动扫描、租约、保温与队列推进。"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    EnvironmentState,
    RunContext,
    RunState,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    StopMode,
    StopResult,
)
from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from core.environments.errors import EnvironmentNotFoundError, EnvironmentStateError
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.errors import PluginNotFoundError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.plugins.rpc_host import RpcPluginHost
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.environments import EnvironmentRepository
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TOKEN = "secret-token"

_STABLE_DISK_FREE = 100 * 1024**3
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
SCHEDULE_ID = "schedule-1"


def insert_template(connection: sqlite3.Connection) -> None:
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


def insert_schedule(connection: sqlite3.Connection, *, keep_warm_minutes: int) -> None:
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, window_minutes, "
        "coalesce, priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
        "job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每小时', 'interval', '60', NULL, 'UTC', 'run_once', 120, 1, 0, 180, 0, ?, ?, "
        "0, NULL, ?, ?)",
        (SCHEDULE_ID, keep_warm_minutes, TEMPLATE_ID, T0.isoformat(), T0.isoformat()),
    )


def make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    return registry


class _CancellingScriptAdapter(FakeScriptAdapter):
    """首次观察时请求叫停，模拟用户在执行中按下停止。"""

    def __init__(self) -> None:
        super().__init__()
        self.supervisor: Supervisor | None = None
        self.run_id = ""
        self.stop_modes: list[StopMode] = []

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        assert self.supervisor is not None
        self.supervisor.request_cancel(self.run_id, reason="user_stop")
        return ScriptObservation(state=ScriptState.EXITED, since=ctx.clock.utc_now())

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult:
        self.stop_modes.append(mode)
        return super().stop(handle, ctx, mode)


class _GatedScriptAdapter(FakeScriptAdapter):
    """launch 后停在 observe，直到测试放行；用来观察取消是否到场。"""

    def __init__(self) -> None:
        super().__init__()
        self.launched = threading.Event()
        self.release = threading.Event()

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        handle = super().launch(script, ctx)
        self.launched.set()
        return handle

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self.calls.append("observe")
        assert self.release.wait(timeout=10), "测试没有放行执行器"
        return ScriptObservation(state=ScriptState.RUNNING, since=ctx.clock.utc_now())


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    put_settings(connection, {TOKEN_KEY: TOKEN}, at=T0)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    data_root = tmp_path
    try:
        yield connection, clock, runs, data_root
    finally:
        connection.close()


def supervisor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
) -> Supervisor:
    return Supervisor(
        connection, clock, make_registry(), data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )


def _supervisor_with_script(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    script: FakeScriptAdapter,
) -> Supervisor:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=script,
            verifier=FakeVerifier(),
        )
    )
    return Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )


def _stop_over_http(
    database: Path, clock: VirtualClock, supervisor: Supervisor, run_id: str
) -> ApiResponse:
    """走与 serve 相同的注入面：ApiApp 只拿到 RunStopper，不摸 Supervisor 内部。"""
    app = ApiApp(database, clock=clock, run_stopper=supervisor)
    return app.handle(
        ApiRequest(
            "POST",
            f"/api/v1/runs/{run_id}/stop",
            {},
            {"authorization": f"Bearer {TOKEN}"},
            b"",
        )
    )


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    *,
    schedule_id: str | None = None,
    scheduled_at: datetime = T0,
    deadline_at: datetime | None = None,
    key: str = "run-key",
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=schedule_id,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=deadline_at,
        )
    )
    return record.id


def test_expired_lease_moves_run_to_recovering(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
    leases = LeaseService(connection, clock)
    leases.acquire(run_id, (ENV_ID,), ttl=timedelta(minutes=10))
    runs.transition(run_id, RunState.PREPARING)
    supervisor = supervisor_for(connection, clock, data_root)
    clock.advance(timedelta(minutes=11))
    supervisor.tick()
    record = runs.get(run_id)
    assert record.state is RunState.RECOVERING
    assert record.error_code == "E_RESOURCE_LEASE_LOST"
    assert record.attempt == 1
    assert leases.active_for_run(run_id) == ()


def test_cleaning_run_holds_lease_across_renewal_ticks(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/09：租约一直持有到 cleanup 完成；cleaning 中的长清理靠周期续租保住。"""
    connection, clock, runs, data_root = env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
    leases = LeaseService(connection, clock)
    leases.acquire(run_id, (ENV_ID,), ttl=timedelta(minutes=10))
    runs.transition(run_id, RunState.PREPARING)
    runs.transition(run_id, RunState.EXECUTING)
    runs.transition(run_id, RunState.CLEANING)
    supervisor = supervisor_for(connection, clock, data_root)
    for _ in range(11):
        clock.advance(timedelta(minutes=1))
        supervisor.tick()
    assert runs.get(run_id).state is RunState.CLEANING
    assert len(leases.active_for_run(run_id)) == 1


def test_expired_warm_window_closes_environment(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = env
    insert_template(connection)
    repository = EnvironmentRepository(connection)
    repository.ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    repository.set_state(ENV_ID, EnvironmentState.READY, at=T0, detail={"action": "test"})
    repository.set_warm_until(ENV_ID, T0 - timedelta(minutes=1))
    supervisor_for(connection, clock, data_root).tick()
    record = EnvironmentService(connection, clock).get(ENV_ID)
    assert record.state is EnvironmentState.OFF
    assert record.warm_until is None


def test_dispatch_passes_schedule_keep_warm_to_executor(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(connection)
    insert_schedule(connection, keep_warm_minutes=30)
    run_id = prepare_run(connection, runs, schedule_id=SCHEDULE_ID)
    runs.enqueue(run_id)
    supervisor_for(connection, clock, data_root).tick()
    assert runs.get(run_id).state is RunState.SUCCEEDED
    environment = EnvironmentService(connection, clock).get(ENV_ID)
    assert environment.state is EnvironmentState.READY
    assert environment.warm_until == T0 + timedelta(minutes=30)


def test_startup_marks_orphan_executing_run_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
    leases = LeaseService(connection, clock)
    leases.acquire(run_id, (ENV_ID,))
    runs.transition(run_id, RunState.PREPARING)
    runs.transition(run_id, RunState.EXECUTING)
    supervisor_for(connection, clock, data_root).startup()
    assert runs.get(run_id).state is RunState.UNKNOWN
    assert leases.active_for_run(run_id) == ()


def test_tick_fails_overdue_run(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(connection)
    run_id = prepare_run(connection, runs, deadline_at=T0 + timedelta(minutes=1))
    runs.enqueue(run_id)
    supervisor = supervisor_for(connection, clock, data_root)
    clock.advance(timedelta(minutes=2))
    supervisor.tick()
    record = runs.get(run_id)
    assert record.state is RunState.FAILED
    assert record.error_code == "E_DEADLINE"


def test_cancel_during_execute_stops_script_gracefully(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    adapter = _CancellingScriptAdapter()
    adapter.run_id = run_id
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=adapter,
            verifier=FakeVerifier(),
        )
    )
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    adapter.supervisor = supervisor
    supervisor.tick()
    assert runs.get(run_id).state is RunState.CANCELLED
    assert adapter.stop_modes == [StopMode.GRACEFUL]
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF
    interrupted = [event for event in runs.events(run_id) if event.type == "run.interrupted"]
    assert len(interrupted) == 1


def test_dispatch_uses_rpc_registry(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """supervisor 拿到 RPC 能力表时，一次 tick 就能把运行推到终态。"""
    connection, clock, runs, data_root = env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    plugins_root = Path(__file__).resolve().parents[2] / "plugins"
    with RpcPluginHost(plugins_root, ["fake"]) as host:
        supervisor = Supervisor(
            connection,
            clock,
            host.registry,
            data_root=data_root,
            disk_free=lambda: _STABLE_DISK_FREE,
        )
        supervisor.tick()
        assert runs.get(run_id).state is RunState.SUCCEEDED
        assert host.channels["fake"].pid is not None


def test_prepare_environment_moves_unknown_to_ready(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """手动预热与 tick 调度无关：provider 被调到，环境按 service 规则迁到 ready。"""
    connection, clock, _runs, data_root = env
    provider = FakeEnvironmentProvider()
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    EnvironmentRepository(connection).ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    result = supervisor.prepare_environment(ENV_ID)
    assert result.ok
    assert provider.calls == ["prepare"]
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.READY


def test_reset_environment_stays_booting_until_probe(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/02：reset 复位成功后不直接 ready，等下一次探针确认。"""
    connection, clock, _runs, data_root = env
    provider = FakeEnvironmentProvider()
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=provider))
    repository = EnvironmentRepository(connection)
    repository.ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    repository.set_state(ENV_ID, EnvironmentState.READY, at=T0, detail={"action": "test"})
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    result = supervisor.reset_environment(ENV_ID)
    assert result.ok
    assert provider.calls == ["reset"]
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.BOOTING


def test_prepare_environment_rejects_busy_state(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = env
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=FakeEnvironmentProvider()))
    repository = EnvironmentRepository(connection)
    repository.ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    repository.set_state(ENV_ID, EnvironmentState.BUSY, at=T0, detail={"action": "test"})
    supervisor = Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    with pytest.raises(EnvironmentStateError):
        supervisor.prepare_environment(ENV_ID)
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.BUSY


def test_prepare_environment_raises_for_missing_provider(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = env
    EnvironmentRepository(connection).ensure(ENV_ID, provider_id="ghost", name=ENV_ID)
    supervisor = supervisor_for(connection, clock, data_root)
    with pytest.raises(PluginNotFoundError):
        supervisor.prepare_environment(ENV_ID)


def test_reset_environment_raises_for_missing_environment(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = env
    supervisor = supervisor_for(connection, clock, data_root)
    with pytest.raises(EnvironmentNotFoundError):
        supervisor.reset_environment("no-such-env")


def test_stop_during_execute_reaches_script_and_lands_cancelled(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """执行中的叫停必须置位执行器侧的取消标记：脚本被停掉，最终 cancelled。"""
    connection, clock, runs, data_root = env
    insert_template(connection)
    adapter = _GatedScriptAdapter()
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    supervisor = _supervisor_with_script(connection, clock, data_root, adapter)
    outcome: dict[str, str] = {}

    def request_stop() -> None:
        if not adapter.launched.wait(timeout=10):
            outcome["failure"] = "执行器没有启动脚本"
            adapter.release.set()
            return
        try:
            response = _stop_over_http(data_root / "state.db", clock, supervisor, run_id)
            assert response.status == 200
            outcome["state"] = str(body_of(response)["run"]["state"])
        except Exception as error:
            outcome["failure"] = repr(error)
        finally:
            adapter.release.set()

    requester = threading.Thread(target=request_stop, daemon=True)
    requester.start()
    supervisor.tick()
    requester.join(timeout=10)
    assert not requester.is_alive(), "叫停线程没有结束"
    assert outcome == {"state": "executing"}
    assert runs.get(run_id).state is RunState.CANCELLED
    assert "stop" in adapter.calls
    assert [event.type for event in runs.events(run_id)].count("run.interrupted") == 1
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF


def test_stop_during_waiting_cancels_on_next_dispatch_without_launch(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """waiting 期间没有取消标记：下一轮 dispatch 按 run.interrupted 收敛，不执行脚本。"""
    connection, clock, runs, data_root = env
    insert_template(connection)
    adapter = FakeScriptAdapter()
    run_id = prepare_run(
        connection,
        runs,
        scheduled_at=T0 + timedelta(minutes=5),
        deadline_at=T0 + timedelta(minutes=30),
    )
    runs.enqueue(run_id)
    supervisor = _supervisor_with_script(connection, clock, data_root, adapter)
    supervisor.tick()
    assert runs.get(run_id).state is RunState.WAITING
    response = _stop_over_http(data_root / "state.db", clock, supervisor, run_id)
    assert response.status == 200
    assert body_of(response)["run"]["state"] == "waiting"
    clock.advance(timedelta(minutes=6))
    supervisor.tick()
    record = runs.get(run_id)
    assert record.state is RunState.CANCELLED
    assert "launch" not in adapter.calls
    assert [event.type for event in runs.events(run_id)].count("run.interrupted") == 1
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
