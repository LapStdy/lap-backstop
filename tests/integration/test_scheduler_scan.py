"""调度扫描：next_run 推进、预热点、补跑与幂等。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import RunState
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.scheduler.scan import ScheduleScanner
from storage import migrate, open_database
from storage.repositories.schedules import ScheduleRepository

T0 = datetime(2026, 10, 23, 0, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
SCHEDULE_ID = "schedule-1"

Env = tuple[sqlite3.Connection, VirtualClock, JobRunService, ScheduleScanner]


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'plugin-1', 'env-1', '1', '{}', "
        "'{}', 1, ?, ?)",
        (TEMPLATE_ID, "2026-10-23T00:00:00+00:00", "2026-10-23T00:00:00+00:00"),
    )


def insert_schedule(
    connection: sqlite3.Connection,
    *,
    next_run_at: datetime | None,
    kind: str = "interval",
    expr: str = "60",
    catch_up: str = "run_once",
    window_minutes: int = 120,
    deadline_minutes: int = 180,
    preheat_minutes: int = 0,
    coalesce: int = 1,
    enabled: int = 1,
) -> None:
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, "
        "window_minutes, coalesce, priority, deadline_minutes, preheat_minutes, "
        "keep_warm_minutes, job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每小时', ?, ?, NULL, 'UTC', ?, ?, ?, 0, ?, ?, 0, ?, ?, ?, ?, ?)",
        (
            SCHEDULE_ID,
            kind,
            expr,
            catch_up,
            window_minutes,
            coalesce,
            deadline_minutes,
            preheat_minutes,
            TEMPLATE_ID,
            enabled,
            None if next_run_at is None else next_run_at.isoformat(),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    insert_template(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    scanner = ScheduleScanner(connection, clock, runs)
    try:
        yield connection, clock, runs, scanner
    finally:
        connection.close()


def read_next_run(connection: sqlite3.Connection) -> datetime | None:
    row = connection.execute(
        "SELECT next_run_at FROM schedules WHERE id = ?",
        (SCHEDULE_ID,),
    ).fetchone()
    assert row is not None
    value = row[0]
    return None if value is None else datetime.fromisoformat(str(value))


def test_bootstrap_computes_first_next_run(
    env: Env,
) -> None:
    connection, _clock, _runs, scanner = env
    insert_schedule(connection, next_run_at=None)
    assert scanner.tick(T0) == ()
    assert read_next_run(connection) == T0 + timedelta(hours=1)


def test_tick_creates_and_enqueues_due_run(
    env: Env,
) -> None:
    connection, _clock, runs, scanner = env
    insert_schedule(connection, next_run_at=T0 + timedelta(hours=1))
    created = scanner.tick(T0 + timedelta(hours=1))
    assert len(created) == 1
    record = runs.get(created[0].id)
    assert record.state is RunState.QUEUED
    assert record.scheduled_at == T0 + timedelta(hours=1)
    assert record.deadline_at == T0 + timedelta(hours=1, minutes=180)
    assert read_next_run(connection) == T0 + timedelta(hours=2)


def test_preheat_creates_run_before_scheduled_time(
    env: Env,
) -> None:
    connection, _clock, _runs, scanner = env
    insert_schedule(
        connection,
        next_run_at=T0 + timedelta(hours=1),
        preheat_minutes=30,
    )
    assert scanner.tick(T0 + timedelta(minutes=20)) == ()
    created = scanner.tick(T0 + timedelta(minutes=40))
    assert len(created) == 1
    assert created[0].scheduled_at == T0 + timedelta(hours=1)
    assert read_next_run(connection) == T0 + timedelta(hours=2)


def test_catch_up_run_once_keeps_latest_missed_point(
    env: Env,
) -> None:
    connection, _clock, _runs, scanner = env
    insert_schedule(connection, next_run_at=T0 + timedelta(hours=1))
    created = scanner.tick(T0 + timedelta(hours=4))
    assert len(created) == 1
    assert created[0].scheduled_at == T0 + timedelta(hours=4)
    assert created[0].detail == {}
    assert read_next_run(connection) == T0 + timedelta(hours=5)


def test_catch_up_window_coalesces_missed_points(
    env: Env,
) -> None:
    connection, _clock, runs, scanner = env
    insert_schedule(
        connection,
        next_run_at=T0 + timedelta(hours=1),
        catch_up="window",
        window_minutes=60,
        coalesce=1,
    )
    created = scanner.tick(T0 + timedelta(hours=4))
    assert len(created) == 1
    events = runs.events(created[0].id)
    assert events[0].payload["merged_scheduled_ats"] == [(T0 + timedelta(hours=3)).isoformat()]
    assert read_next_run(connection) == T0 + timedelta(hours=5)


def test_catch_up_skip_creates_nothing(
    env: Env,
) -> None:
    connection, _clock, _runs, scanner = env
    insert_schedule(
        connection,
        next_run_at=T0 + timedelta(hours=1),
        catch_up="skip",
    )
    assert scanner.tick(T0 + timedelta(hours=4)) == ()
    assert read_next_run(connection) == T0 + timedelta(hours=5)


def test_repeated_tick_is_idempotent(
    env: Env,
) -> None:
    connection, _clock, _runs, scanner = env
    insert_schedule(connection, next_run_at=T0 + timedelta(hours=1))
    first = scanner.tick(T0 + timedelta(hours=1))
    second = scanner.tick(T0 + timedelta(hours=1))
    assert len(first) == 1
    assert second == ()


def test_disabled_schedule_is_ignored(
    env: Env,
) -> None:
    connection, _clock, _runs, scanner = env
    insert_schedule(connection, next_run_at=T0 + timedelta(hours=1), enabled=0)
    assert scanner.tick(T0 + timedelta(hours=4)) == ()


def test_schedule_row_exposes_updated_at(env: Env) -> None:
    connection, _clock, _runs, _scanner = env
    insert_schedule(connection, next_run_at=T0 + timedelta(hours=1))
    repository = ScheduleRepository(connection)
    before = repository.get(SCHEDULE_ID)
    assert before is not None
    assert before.updated_at == T0

    later = T0 + timedelta(minutes=30)
    repository.set_next_run(SCHEDULE_ID, T0 + timedelta(hours=2), updated_at=later)
    after = repository.get(SCHEDULE_ID)
    assert after is not None
    assert after.updated_at == later
