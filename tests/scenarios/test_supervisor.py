"""场景：核心 supervisor 的启动扫描、租约、保温与队列推进。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import (
    EnvironmentState,
    RunContext,
    RunState,
    ScriptHandle,
    ScriptObservation,
    ScriptState,
    StopMode,
    StopResult,
)
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.environments import EnvironmentRepository
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
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


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    data_root = tmp_path / "data"
    try:
        yield connection, clock, runs, data_root
    finally:
        connection.close()


def supervisor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
) -> Supervisor:
    return Supervisor(connection, clock, make_registry(), data_root=data_root)


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    *,
    schedule_id: str | None = None,
    deadline_at: datetime | None = None,
    key: str = "run-key",
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=schedule_id,
            scheduled_at=T0,
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
    supervisor = Supervisor(connection, clock, registry, data_root=data_root)
    adapter.supervisor = supervisor
    supervisor.tick()
    assert runs.get(run_id).state is RunState.CANCELLED
    assert adapter.stop_modes == [StopMode.GRACEFUL]
    assert LeaseService(connection, clock).active_for_run(run_id) == ()
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF
    interrupted = [event for event in runs.events(run_id) if event.type == "run.interrupted"]
    assert len(interrupted) == 1
