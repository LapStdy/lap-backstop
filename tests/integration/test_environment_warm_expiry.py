"""保温到期关闭：有在途运行（活跃租约）的环境不关，运行收尾后再关。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.resources.service import LeaseService
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository, ResourceSpec
from tests.integration.test_environments import ENV_ID, StubProvider, make_call

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
_ISO = "2026-10-23T12:00:00+00:00"


@pytest.fixture
def env(tmp_path: Path) -> Iterator[tuple[sqlite3.Connection, VirtualClock, EnvironmentService]]:
    """一个已注册的环境；库里空着，用例自行造运行与租约。"""
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    service = EnvironmentService(connection, clock)
    service.register(ENV_ID, provider_id="fake", name="模拟器")
    try:
        yield connection, clock, service
    finally:
        connection.close()


def test_warm_expired_skips_environment_with_active_lease(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """保温到期但还有在途运行（持活跃租约）时不关闭，等运行收尾后再关。"""
    connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.warm(make_call(StubProvider(), clock), minutes=30)
    leases, run_id = _seed_active_lease(connection, clock)
    assert service.warm_expired(T0 + timedelta(minutes=31)) == ()
    leases.release(run_id)
    due = service.warm_expired(T0 + timedelta(minutes=31))
    assert [row.id for row in due] == [ENV_ID]


def test_warm_expired_ignores_released_lease(
    env: tuple[sqlite3.Connection, VirtualClock, EnvironmentService],
) -> None:
    """租约已释放的环境照常按到期进入关闭清单，不因历史租约占住。"""
    connection, clock, service = env
    service.prepare(make_call(StubProvider(), clock))
    service.warm(make_call(StubProvider(), clock), minutes=30)
    leases, run_id = _seed_active_lease(connection, clock)
    leases.release(run_id)
    assert [row.id for row in service.warm_expired(T0 + timedelta(minutes=31))] == [ENV_ID]


def _seed_active_lease(
    connection: sqlite3.Connection,
    clock: VirtualClock,
) -> tuple[LeaseService, str]:
    """造一条持有 ENV_ID 租约的在途运行：模板、资源与运行行都补齐。"""
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, '1', '{}', '{}', 1, ?, ?)",
        (TEMPLATE_ID, ENV_ID, _ISO, _ISO),
    )
    ResourceRepository(connection).ensure(
        ENV_ID, ResourceSpec(resource_type="environment", name="模拟器")
    )
    runs = JobRunService(connection, clock)
    record, _created = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key="run-warm-expiry",
            priority=0,
            deadline_at=None,
        )
    )
    leases = LeaseService(connection, clock)
    leases.acquire(record.id, (ENV_ID,))
    return leases, record.id
