"""场景：运行中与等待期的叫停：取消标记经执行器到达脚本，停止经 HTTP 端点落到 cancelled。"""

from __future__ import annotations

import sqlite3
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

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
from core.api.app import ApiApp, ApiDeps
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from tests.scenarios.supervisor_scenario_support import (
    _STABLE_DISK_FREE,
    ENV_ID,
    T0,
    TOKEN,
    RunOverrides,
    insert_template,
    prepare_run,
)
from tests.supervisor_support import tick_and_settle


class _CancellingScriptAdapter(FakeScriptAdapter):
    """首次观察时请求叫停，模拟用户在执行中按下停止。

    走 ``stop``：叫停按请求线程口径自开连接，执行线程内不碰 tick 的连接（docs/11）。
    """

    def __init__(self) -> None:
        super().__init__()
        self.supervisor: Supervisor | None = None
        self.run_id = ""
        self.stop_modes: list[StopMode] = []

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        assert self.supervisor is not None
        self.supervisor.stop(self.run_id, reason="user_stop")
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
    app = ApiApp(database, clock=clock, deps=ApiDeps(run_stopper=supervisor))
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
    tick_and_settle(supervisor)
    assert runs.get(run_id).state is RunState.CANCELLED
    assert adapter.stop_modes == [StopMode.GRACEFUL]
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF
    interrupted = [event for event in runs.events(run_id) if event.type == "run.interrupted"]
    assert len(interrupted) == 1


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
    tick_and_settle(supervisor)
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
        RunOverrides(
            scheduled_at=T0 + timedelta(minutes=5), deadline_at=T0 + timedelta(minutes=30)
        ),
    )
    runs.enqueue(run_id)
    supervisor = _supervisor_with_script(connection, clock, data_root, adapter)
    tick_and_settle(supervisor)
    assert runs.get(run_id).state is RunState.WAITING
    response = _stop_over_http(data_root / "state.db", clock, supervisor, run_id)
    assert response.status == 200
    assert body_of(response)["run"]["state"] == "waiting"
    clock.advance(timedelta(minutes=6))
    tick_and_settle(supervisor)
    record = runs.get(run_id)
    assert record.state is RunState.CANCELLED
    assert "launch" not in adapter.calls
    assert [event.type for event in runs.events(run_id)].count("run.interrupted") == 1
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
