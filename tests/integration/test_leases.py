"""资源租约：容量、获取顺序、续租、释放与过期回收。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import LeaseState
from core.clock import VirtualClock
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import JobRunService
from core.resources.errors import ResourceBusyError, ResourceNotFoundError
from core.resources.service import DEFAULT_TTL, LeaseService
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'plugin-1', 'env-1', '1', '{}', "
        "'{}', 1, ?, ?)",
        (TEMPLATE_ID, "2026-10-23T00:00:00+00:00", "2026-10-23T00:00:00+00:00"),
    )


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[
    tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository]
]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    insert_template(connection)
    clock = VirtualClock(T0)
    yield (
        connection,
        clock,
        JobRunService(connection, clock),
        LeaseService(connection, clock),
        ResourceRepository(connection),
    )
    connection.close()


def make_run(runs: JobRunService, key: str) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    return record.id


def event_count(connection: sqlite3.Connection, event_type: str) -> int:
    row = connection.execute(
        "SELECT COUNT(*) FROM run_events WHERE type = ?",
        (event_type,),
    ).fetchone()
    return 0 if row is None else int(row[0])


def test_acquire_environment_lease(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    connection, _clock, runs, leases, resources = env
    resources.ensure("env-res", resource_type="environment", name="模拟器")
    run_id = make_run(runs, "run-1")
    acquired = leases.acquire(run_id, ("env-res",))
    assert len(acquired) == 1
    lease = acquired[0]
    assert lease.state is LeaseState.LEASED
    assert lease.owner == "supervisor"
    assert lease.expires_at == T0 + DEFAULT_TTL
    assert [item.id for item in leases.active_for_run(run_id)] == [lease.id]
    assert event_count(connection, "resource.leased") == 1


def test_environment_capacity_is_always_one(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    _connection, _clock, runs, leases, resources = env
    resources.ensure("env-res", resource_type="environment", name="模拟器", capacity=5)
    row = resources.get("env-res")
    assert row is not None
    assert row.capacity == 1
    first = make_run(runs, "run-1")
    second = make_run(runs, "run-2")
    leases.acquire(first, ("env-res",))
    with pytest.raises(ResourceBusyError):
        leases.acquire(second, ("env-res",))
    assert leases.active_for_run(second) == ()


def test_custom_resource_allows_declared_capacity(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    _connection, _clock, runs, leases, resources = env
    resources.ensure("gpu", resource_type="gpu", name="显卡", capacity=2)
    first = make_run(runs, "run-1")
    second = make_run(runs, "run-2")
    third = make_run(runs, "run-3")
    leases.acquire(first, ("gpu",))
    leases.acquire(second, ("gpu",))
    with pytest.raises(ResourceBusyError):
        leases.acquire(third, ("gpu",))


def test_acquire_sorts_resource_ids(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    _connection, _clock, runs, leases, resources = env
    resources.ensure("res-b", resource_type="custom", name="B")
    resources.ensure("res-a", resource_type="custom", name="A")
    run_id = make_run(runs, "run-1")
    acquired = leases.acquire(run_id, ("res-b", "res-a"))
    assert [lease.resource_id for lease in acquired] == ["res-a", "res-b"]


def test_renew_extends_expiry_and_writes_event(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    connection, clock, runs, leases, resources = env
    resources.ensure("env-res", resource_type="environment", name="模拟器")
    run_id = make_run(runs, "run-1")
    leases.acquire(run_id, ("env-res",))
    clock.advance(timedelta(minutes=5))
    renewed = leases.renew(run_id)
    assert len(renewed) == 1
    assert renewed[0].expires_at == T0 + timedelta(minutes=15)
    assert renewed[0].renewed_at == T0 + timedelta(minutes=5)
    assert event_count(connection, "resource.renewed") == 1


def test_release_frees_the_resource(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    connection, _clock, runs, leases, resources = env
    resources.ensure("env-res", resource_type="environment", name="模拟器")
    run_id = make_run(runs, "run-1")
    leases.acquire(run_id, ("env-res",))
    released = leases.release(run_id, reason="cleanup")
    assert [lease.state for lease in released] == [LeaseState.RELEASED]
    assert leases.active_for_run(run_id) == ()
    payload = json.loads(
        str(
            connection.execute(
                "SELECT payload FROM run_events WHERE type = 'resource.released'"
            ).fetchone()[0]
        )
    )
    assert payload["reason"] == "cleanup"


def test_expire_reclaims_leases_and_writes_core_event(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    connection, clock, runs, leases, resources = env
    resources.ensure("env-res", resource_type="environment", name="模拟器")
    run_id = make_run(runs, "run-1")
    leases.acquire(run_id, ("env-res",))
    clock.advance(DEFAULT_TTL + timedelta(seconds=1))
    expired = leases.expire()
    assert [lease.state for lease in expired] == [LeaseState.RELEASED]
    assert leases.active_for_run(run_id) == ()
    assert event_count(connection, "resource.released") == 1
    global_row = connection.execute(
        "SELECT COUNT(*) FROM events_global WHERE type = 'core.event'"
    ).fetchone()
    assert global_row is not None
    assert int(global_row[0]) == 1


def test_resource_can_be_reacquired_after_release(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    _connection, _clock, runs, leases, resources = env
    resources.ensure("env-res", resource_type="environment", name="模拟器")
    first = make_run(runs, "run-1")
    second = make_run(runs, "run-2")
    leases.acquire(first, ("env-res",))
    leases.release(first)
    acquired = leases.acquire(second, ("env-res",))
    assert [lease.run_id for lease in acquired] == [second]


def test_unknown_resource_and_run_are_reported(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    _connection, _clock, runs, leases, resources = env
    run_id = make_run(runs, "run-1")
    with pytest.raises(ResourceNotFoundError):
        leases.acquire(run_id, ("missing",))
    resources.ensure("res-a", resource_type="custom", name="A")
    with pytest.raises(RunNotFoundError):
        leases.acquire("missing-run", ("res-a",))


def test_acquire_without_resources_is_a_noop(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, LeaseService, ResourceRepository],
) -> None:
    _connection, _clock, runs, leases, _resources = env
    run_id = make_run(runs, "run-1")
    assert leases.acquire(run_id, ()) == ()
