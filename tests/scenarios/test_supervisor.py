"""场景：核心 supervisor 的启动扫描、租约、保温与队列推进。"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path

from contracts import EnvironmentState, RunState
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.rpc_host import RpcPluginHost
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from storage.repositories.environments import EnvironmentRepository
from storage.repositories.resources import ResourceRepository, ResourceSpec
from tests.scenarios.supervisor_scenario_support import (
    _STABLE_DISK_FREE,
    ENV_ID,
    T0,
    TEMPLATE_ID,
    RunOverrides,
    insert_template,
    prepare_run,
    supervisor_for,
)
from tests.supervisor_support import tick_and_settle

SCHEDULE_ID = "schedule-1"


def insert_schedule(connection: sqlite3.Connection, *, keep_warm_minutes: int) -> None:
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, window_minutes, "
        "coalesce, priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
        "job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每小时', 'interval', '60', NULL, 'UTC', 'run_once', 120, 1, 0, 180, 0, ?, ?, "
        "0, NULL, ?, ?)",
        (SCHEDULE_ID, keep_warm_minutes, TEMPLATE_ID, T0.isoformat(), T0.isoformat()),
    )


def test_expired_lease_moves_run_to_recovering(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = supervisor_env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    ResourceRepository(connection).ensure(
        ENV_ID, ResourceSpec(resource_type="environment", name=ENV_ID)
    )
    leases = LeaseService(connection, clock)
    leases.acquire(run_id, (ENV_ID,), ttl=timedelta(minutes=10))
    runs.transition(run_id, RunState.PREPARING)
    supervisor = supervisor_for(connection, clock, data_root)
    clock.advance(timedelta(minutes=11))
    tick_and_settle(supervisor)
    record = runs.get(run_id)
    assert record.state is RunState.RECOVERING
    assert record.error_code == "E_RESOURCE_LEASE_LOST"
    assert record.attempt == 1
    assert leases.active_for_run(run_id) == ()


def test_cleaning_run_holds_lease_across_renewal_ticks(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/09：租约一直持有到 cleanup 完成；cleaning 中的长清理靠周期续租保住。"""
    connection, clock, runs, data_root = supervisor_env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    ResourceRepository(connection).ensure(
        ENV_ID, ResourceSpec(resource_type="environment", name=ENV_ID)
    )
    leases = LeaseService(connection, clock)
    leases.acquire(run_id, (ENV_ID,), ttl=timedelta(minutes=10))
    runs.transition(run_id, RunState.PREPARING)
    runs.transition(run_id, RunState.EXECUTING)
    runs.transition(run_id, RunState.CLEANING)
    supervisor = supervisor_for(connection, clock, data_root)
    for _ in range(11):
        clock.advance(timedelta(minutes=1))
        tick_and_settle(supervisor)
    assert runs.get(run_id).state is RunState.CLEANING
    assert len(leases.active_for_run(run_id)) == 1


def test_expired_warm_window_closes_environment(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, _runs, data_root = supervisor_env
    insert_template(connection)
    repository = EnvironmentRepository(connection)
    repository.ensure(ENV_ID, provider_id="fake", name=ENV_ID)
    repository.set_state(ENV_ID, EnvironmentState.READY, at=T0, detail={"action": "test"})
    repository.set_warm_until(ENV_ID, T0 - timedelta(minutes=1))
    tick_and_settle(supervisor_for(connection, clock, data_root))
    record = EnvironmentService(connection, clock).get(ENV_ID)
    assert record.state is EnvironmentState.OFF
    assert record.warm_until is None


def test_dispatch_passes_schedule_keep_warm_to_executor(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = supervisor_env
    insert_template(connection)
    insert_schedule(connection, keep_warm_minutes=30)
    run_id = prepare_run(connection, runs, RunOverrides(schedule_id=SCHEDULE_ID))
    runs.enqueue(run_id)
    tick_and_settle(supervisor_for(connection, clock, data_root))
    assert runs.get(run_id).state is RunState.SUCCEEDED
    environment = EnvironmentService(connection, clock).get(ENV_ID)
    assert environment.state is EnvironmentState.READY
    assert environment.warm_until == T0 + timedelta(minutes=30)


def test_startup_marks_orphan_executing_run_unknown(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = supervisor_env
    insert_template(connection)
    run_id = prepare_run(connection, runs)
    runs.enqueue(run_id)
    ResourceRepository(connection).ensure(
        ENV_ID, ResourceSpec(resource_type="environment", name=ENV_ID)
    )
    leases = LeaseService(connection, clock)
    leases.acquire(run_id, (ENV_ID,))
    runs.transition(run_id, RunState.PREPARING)
    runs.transition(run_id, RunState.EXECUTING)
    supervisor_for(connection, clock, data_root).startup()
    assert runs.get(run_id).state is RunState.UNKNOWN
    assert leases.active_for_run(run_id) == ()


def test_tick_fails_overdue_run(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = supervisor_env
    insert_template(connection)
    run_id = prepare_run(connection, runs, RunOverrides(deadline_at=T0 + timedelta(minutes=1)))
    runs.enqueue(run_id)
    supervisor = supervisor_for(connection, clock, data_root)
    clock.advance(timedelta(minutes=2))
    tick_and_settle(supervisor)
    record = runs.get(run_id)
    assert record.state is RunState.FAILED
    assert record.error_code == "E_DEADLINE"


def test_dispatch_uses_rpc_registry(
    supervisor_env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """supervisor 拿到 RPC 能力表时，一次 tick 就能把运行推到终态。"""
    connection, clock, runs, data_root = supervisor_env
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
        tick_and_settle(supervisor)
        assert runs.get(run_id).state is RunState.SUCCEEDED
        assert host.channels["fake"].pid is not None
