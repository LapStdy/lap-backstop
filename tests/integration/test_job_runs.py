"""JobRun 幂等创建、状态迁移、事件、队列与截止时间。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import EventType, RunState
from core.clock import VirtualClock
from core.jobs.errors import IllegalRunTransitionError, RunNotFoundError
from core.jobs.run_service import DiskFullError, JobRunService
from storage import migrate, open_database
from storage.repositories.events import EventLog
from storage.repositories.job_runs import (
    JobRunRepository,
    NewRun,
    StateChange,
)

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
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
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    database = open_database(tmp_path / "state.db")
    migrate(database)
    insert_template(database)
    try:
        yield database
    finally:
        database.close()


@pytest.fixture
def service(connection: sqlite3.Connection) -> JobRunService:
    return JobRunService(connection, VirtualClock(NOW))


def make_run(
    key: str = "schedule-1:2026-10-23T12:00:00+00:00",
    *,
    priority: int = 0,
    scheduled_at: datetime | None = None,
    deadline_at: datetime | None = None,
) -> NewRun:
    moment = NOW if scheduled_at is None else scheduled_at
    return NewRun(
        template_id=TEMPLATE_ID,
        schedule_id=None,
        scheduled_at=moment,
        created_at=NOW,
        idempotency_key=key,
        priority=priority,
        deadline_at=deadline_at,
    )


def event_types(connection: sqlite3.Connection, run_id: str) -> list[str]:
    rows = connection.execute(
        "SELECT type FROM run_events WHERE run_id = ? ORDER BY seq",
        (run_id,),
    ).fetchall()
    return [str(row[0]) for row in rows]


def test_create_is_idempotent(service: JobRunService, connection: sqlite3.Connection) -> None:
    record, created = service.create_scheduled(make_run())
    again, created_again = service.create_scheduled(make_run())
    assert created
    assert not created_again
    assert again.id == record.id
    assert event_types(connection, record.id) == ["run.created"]


def test_created_event_records_merged_points(service: JobRunService) -> None:
    merged = (NOW - timedelta(hours=2), NOW - timedelta(hours=1))
    record, _ = service.create_scheduled(make_run(), merged_scheduled_ats=merged)
    created = service.events(record.id)[0]
    assert created.type == "run.created"
    assert created.payload["merged_scheduled_ats"] == [moment.isoformat() for moment in merged]


def test_missing_run_raises(service: JobRunService) -> None:
    with pytest.raises(RunNotFoundError):
        service.get("missing")


def test_enqueue_writes_queued_event(
    service: JobRunService, connection: sqlite3.Connection
) -> None:
    record, _ = service.create_scheduled(make_run())
    queued = service.enqueue(record.id)
    assert queued.state is RunState.QUEUED
    assert event_types(connection, record.id) == ["run.created", "run.state.changed", "run.queued"]


def test_repeat_transition_to_current_state_is_idempotent(
    service: JobRunService,
    connection: sqlite3.Connection,
) -> None:
    record, _ = service.create_scheduled(make_run())
    service.enqueue(record.id)
    again = service.transition(record.id, RunState.QUEUED)
    assert again.state is RunState.QUEUED
    assert event_types(connection, record.id) == [
        "run.created",
        "run.state.changed",
        "run.queued",
    ]


def test_event_ids_are_run_scoped_and_monotonic(
    service: JobRunService,
    connection: sqlite3.Connection,
) -> None:
    record, _ = service.create_scheduled(make_run())
    service.enqueue(record.id)
    service.transition(record.id, RunState.PREPARING)
    events = service.events(record.id)
    assert [event.seq for event in events] == [1, 2, 3, 4]
    assert events[0].id == f"r:{record.id}:1"
    assert all(event.run_id == record.id for event in events)


def test_terminal_transition_writes_finished_event(
    service: JobRunService,
    connection: sqlite3.Connection,
) -> None:
    record, _ = service.create_scheduled(make_run())
    service.enqueue(record.id)
    for state in (
        RunState.PREPARING,
        RunState.WAITING,
        RunState.EXECUTING,
        RunState.VERIFYING,
        RunState.CLEANING,
        RunState.SUCCEEDED,
    ):
        service.transition(record.id, state)
    finished = service.get(record.id)
    assert finished.state is RunState.SUCCEEDED
    assert finished.finished_at is not None
    last = service.events(record.id)[-1]
    assert last.type == "run.finished"
    assert last.payload["status"] == "succeeded"


def test_queue_orders_by_priority_then_schedule(
    service: JobRunService,
) -> None:
    low, _ = service.create_scheduled(make_run("key-low", priority=1, scheduled_at=NOW))
    high, _ = service.create_scheduled(
        make_run("key-high", priority=9, scheduled_at=NOW + timedelta(hours=2))
    )
    middle, _ = service.create_scheduled(
        make_run("key-middle", priority=5, scheduled_at=NOW + timedelta(hours=1))
    )
    for record in (low, high, middle):
        service.enqueue(record.id)
    queued = service.next_queued(3)
    assert [record.id for record in queued] == [high.id, middle.id, low.id]


def test_overdue_run_fails_with_deadline_code(
    service: JobRunService,
    connection: sqlite3.Connection,
) -> None:
    scheduled = NOW - timedelta(hours=2)
    record, _ = service.create_scheduled(
        make_run(scheduled_at=scheduled, deadline_at=scheduled + timedelta(minutes=30))
    )
    failed = service.fail_overdue()
    assert [item.id for item in failed] == [record.id]
    current = service.get(record.id)
    assert current.state is RunState.FAILED
    assert current.error_code == "E_DEADLINE"
    assert "run.finished" in event_types(connection, record.id)


def test_cancel_queued_run_takes_effect_immediately(
    service: JobRunService,
    connection: sqlite3.Connection,
) -> None:
    record, _ = service.create_scheduled(make_run())
    service.enqueue(record.id)
    cancelled = service.request_cancel(record.id, "用户停手")
    assert cancelled.state is RunState.CANCELLED
    assert "run.interrupted" in event_types(connection, record.id)


def test_cancel_executing_run_waits_for_phase_boundary(service: JobRunService) -> None:
    record, _ = service.create_scheduled(make_run())
    service.enqueue(record.id)
    service.transition(record.id, RunState.PREPARING)
    service.transition(record.id, RunState.WAITING)
    service.transition(record.id, RunState.EXECUTING)
    current = service.request_cancel(record.id)
    assert current.state is RunState.EXECUTING
    assert service.events(record.id)[-1].type == "run.interrupted"


def test_terminal_run_cannot_be_cancelled(service: JobRunService) -> None:
    record, _ = service.create_scheduled(make_run())
    service.request_cancel(record.id)
    with pytest.raises(IllegalRunTransitionError):
        service.request_cancel(record.id)


def test_global_events_get_monotonic_ids(connection: sqlite3.Connection) -> None:
    log = EventLog(connection)
    first = log.append_global(
        EventType.CORE_EVENT,
        {"event": "core.second_instance", "detail": "拒绝第二个实例"},
        at=NOW,
    )
    second = log.append_global(EventType.HOST_STATE, {"state": "running"}, at=NOW)
    assert first.id == "g:1"
    assert second.id == "g:2"


def test_cas_transition_ignores_stale_source(connection: sqlite3.Connection) -> None:
    repository = JobRunRepository(connection)
    record, _ = JobRunService(connection, VirtualClock(NOW)).create_scheduled(make_run())
    stale = StateChange(
        run_id=record.id,
        source=RunState.QUEUED,
        target=RunState.PREPARING,
        at=NOW,
    )
    assert not repository.transition(stale)
    assert repository.get(record.id) is not None


def test_create_rejected_below_disk_guard(connection: sqlite3.Connection) -> None:
    limit = 2 * 1024**3
    service = JobRunService(connection, VirtualClock(NOW), disk_free=lambda: limit - 1)
    with pytest.raises(DiskFullError) as excinfo:
        service.create_scheduled(make_run())
    assert excinfo.value.code == "E_DISK"
    assert excinfo.value.free_bytes == limit - 1
    assert connection.execute("SELECT COUNT(*) FROM job_runs").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM run_events").fetchone()[0] == 0


def test_create_at_disk_guard_boundary_is_allowed(connection: sqlite3.Connection) -> None:
    service = JobRunService(connection, VirtualClock(NOW), disk_free=lambda: 2 * 1024**3)
    record, created = service.create_scheduled(make_run())
    assert created
    assert event_types(connection, record.id) == ["run.created"]


def test_disk_guard_does_not_reject_idempotent_replay(connection: sqlite3.Connection) -> None:
    free = {"bytes": 6 * 1024**3}
    service = JobRunService(connection, VirtualClock(NOW), disk_free=lambda: free["bytes"])
    record, _ = service.create_scheduled(make_run())
    free["bytes"] = 1024**3
    again, created = service.create_scheduled(make_run())
    assert not created
    assert again.id == record.id
