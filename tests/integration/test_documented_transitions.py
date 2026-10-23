"""docs/02 迁移表里每条被拒绝的边：状态不变、抛领域异常、写 run.state.changed。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from contracts import EventType, RunState
from core.clock import VirtualClock
from core.jobs.errors import IllegalRunTransitionError
from core.jobs.run_service import JobRunService
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from tests.state_tables import run_table, shortest_paths

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"

TABLE = run_table()
PATHS = shortest_paths(TABLE, RunState.SCHEDULED)


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'plugin-1', 'env-1', '1', '{}', "
        "'{}', 1, ?, ?)",
        (TEMPLATE_ID, "2026-10-23T00:00:00+00:00", "2026-10-23T00:00:00+00:00"),
    )


@pytest.fixture(scope="module")
def service(tmp_path_factory: pytest.TempPathFactory) -> Iterator[JobRunService]:
    connection = open_database(tmp_path_factory.mktemp("transitions") / "state.db")
    migrate(connection)
    insert_template(connection)
    try:
        yield JobRunService(connection, VirtualClock(NOW))
    finally:
        connection.close()


def create_in(service: JobRunService, source: RunState, key: str) -> tuple[str, int]:
    """建一个运行，沿文档迁移表走到指定状态，返回运行 id 与当时的事件数。"""
    record, _ = service.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=NOW,
            created_at=NOW,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    for state in PATHS[source]:
        service.transition(record.id, state)
    return record.id, len(service.events(record.id))


def test_every_documented_state_is_reachable_from_scheduled() -> None:
    assert set(PATHS) == set(TABLE.states)


@pytest.mark.parametrize(("source", "target"), TABLE.rejected_edges())
def test_rejected_edge_is_refused_with_event(
    service: JobRunService,
    source: RunState,
    target: RunState,
) -> None:
    run_id, events_before = create_in(service, source, f"{source.value}:{target.value}")
    with pytest.raises(IllegalRunTransitionError):
        service.transition(run_id, target)
    assert service.get(run_id).state is source
    events = service.events(run_id)
    assert len(events) == events_before + 1
    rejected = events[-1]
    assert rejected.type == EventType.RUN_STATE_CHANGED
    assert rejected.payload["from_state"] == source.value
    assert rejected.payload["to_state"] == target.value
    assert rejected.payload["reason"] == "illegal_transition"
