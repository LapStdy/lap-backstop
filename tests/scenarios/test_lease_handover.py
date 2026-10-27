"""场景：同一环境上的运行排队执行，租约过期后由排队者接手。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import EnvironmentState, RunState
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)

_STABLE_DISK_FREE = 100 * 1024**3
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
LEASE_TTL = timedelta(minutes=10)


def insert_template(connection: sqlite3.Connection, recovery: dict[str, Any] | None = None) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, ?, 1, ?, ?)",
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
            json.dumps(recovery or {}),
            T0.isoformat(),
            T0.isoformat(),
        ),
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


def prepare_run(
    runs: JobRunService,
    *,
    key: str,
    priority: int = 0,
    scheduled_at: datetime = T0,
    deadline_at: datetime | None = None,
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=priority,
            deadline_at=deadline_at,
        )
    )
    runs.enqueue(record.id)
    return record.id


def event_types(runs: JobRunService, run_id: str) -> list[str]:
    return [event.type for event in runs.events(run_id)]


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    insert_template(connection)
    clock = VirtualClock(T0)
    try:
        yield connection, clock, JobRunService(connection, clock), tmp_path / "data"
    finally:
        connection.close()


def test_second_run_waits_for_environment_until_first_releases_lease(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    first = prepare_run(runs, key="first", priority=10)
    second = prepare_run(runs, key="second")
    supervisor = Supervisor(
        connection, clock, make_registry(), data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )

    supervisor.tick()
    assert runs.get(first).state is RunState.SUCCEEDED
    assert runs.get(second).state is RunState.QUEUED
    assert "resource.leased" not in event_types(runs, second)

    supervisor.tick()
    assert runs.get(second).state is RunState.SUCCEEDED
    assert "resource.leased" in event_types(runs, second)
    assert LeaseService(connection, clock).active_for_run(second) == ()
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF


def test_expired_lease_is_handed_to_queued_run_and_holder_retries(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    first = prepare_run(runs, key="first", priority=10)
    runs.transition(first, RunState.PREPARING)
    ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
    LeaseService(connection, clock).acquire(first, (ENV_ID,), ttl=LEASE_TTL)
    second = prepare_run(runs, key="second")

    supervisor = Supervisor(
        connection, clock, make_registry(), data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    clock.jump_to(T0 + LEASE_TTL + timedelta(minutes=1))
    supervisor.tick()

    lost = runs.get(first)
    assert lost.state is RunState.RECOVERING
    assert lost.error_code == "E_RESOURCE_LEASE_LOST"
    assert lost.detail["retry_at"] == (T0 + LEASE_TTL + timedelta(minutes=2)).isoformat()
    assert LeaseService(connection, clock).active_for_run(first) == ()
    assert runs.get(second).state is RunState.SUCCEEDED

    clock.jump_to(T0 + LEASE_TTL + timedelta(minutes=2))
    supervisor.tick()
    retried = runs.get(first)
    assert retried.state is RunState.SUCCEEDED
    assert retried.attempt == 1


def test_exhausted_lease_recovery_marks_run_failed(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    connection.execute("DELETE FROM job_templates")
    insert_template(connection, {"max_attempts": 1})
    run_id = prepare_run(runs, key="only")
    runs.transition(run_id, RunState.PREPARING)
    ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
    LeaseService(connection, clock).acquire(run_id, (ENV_ID,), ttl=LEASE_TTL)

    supervisor = Supervisor(
        connection, clock, make_registry(), data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    clock.jump_to(T0 + LEASE_TTL + timedelta(seconds=1))
    supervisor.tick()

    failed = runs.get(run_id)
    assert failed.state is RunState.FAILED
    assert failed.error_code == "E_RESOURCE_LEASE_LOST"
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_recovery_retry_with_busy_lease_stays_queued_until_deadline(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """docs/09「资源租约·过期」：重新申请时租约仍被占用只继续排队。

    docs/02 的 queued 行只允许因截止时间迁入 failed，因此这里钉住
    「继续排队 → 截止时间以 E_DEADLINE 收尾」，而不是立刻记 E_RESOURCE_LEASE_LOST。
    """
    connection, clock, runs, data_root = env
    deadline = T0 + timedelta(minutes=90)
    first = prepare_run(runs, key="first", priority=10, deadline_at=deadline)
    runs.transition(first, RunState.PREPARING)
    ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
    leases = LeaseService(connection, clock)
    leases.acquire(first, (ENV_ID,), ttl=LEASE_TTL)

    supervisor = Supervisor(
        connection, clock, make_registry(), data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )
    clock.jump_to(T0 + LEASE_TTL + timedelta(minutes=1))
    supervisor.tick()
    assert runs.get(first).state is RunState.RECOVERING

    blocker = prepare_run(
        runs,
        key="blocker",
        scheduled_at=T0 + timedelta(hours=1),
    )
    runs.transition(blocker, RunState.PREPARING)
    runs.transition(blocker, RunState.WAITING)
    leases.acquire(blocker, (ENV_ID,))

    clock.jump_to(T0 + LEASE_TTL + timedelta(minutes=2))
    supervisor.tick()
    retried = runs.get(first)
    assert retried.state is RunState.QUEUED
    assert leases.active_for_run(first) == ()
    assert leases.active_for_run(blocker) != ()

    clock.jump_to(deadline)
    supervisor.tick()
    overdue = runs.get(first)
    assert overdue.state is RunState.FAILED
    assert overdue.error_code == "E_DEADLINE"
