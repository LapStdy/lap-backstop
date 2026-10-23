"""场景：虚拟时钟下每小时调度连续跑 30 天。"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.scheduler.scan import ScheduleScanner
from storage import migrate, open_database

T0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
DAYS = 30
HOURS = DAYS * 24
SCHEDULE_ID = "hourly"


def prepare_database(path: Path) -> sqlite3.Connection:
    connection = open_database(path)
    migrate(connection)
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES ('template-1', '日常', 'plugin-1', 'env-1', "
        "'1', '{}', '{}', 1, ?, ?)",
        (T0.isoformat(), T0.isoformat()),
    )
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, "
        "window_minutes, coalesce, priority, deadline_minutes, preheat_minutes, "
        "keep_warm_minutes, job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每小时', 'interval', '60', NULL, 'UTC', 'run_once', 120, 1, 0, "
        "180, 0, 0, 'template-1', 1, ?, ?, ?)",
        (
            SCHEDULE_ID,
            (T0 + timedelta(hours=1)).isoformat(),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )
    return connection


def test_hourly_schedule_runs_30_days_without_duplicates(tmp_path: Path) -> None:
    connection = prepare_database(tmp_path / "soak.db")
    try:
        clock = VirtualClock(T0)
        runs = JobRunService(connection, clock)
        scanner = ScheduleScanner(connection, clock, runs)
        created = 0
        for hour in range(1, HOURS + 1):
            moment = T0 + timedelta(hours=hour)
            clock.jump_to(moment)
            created += len(scanner.tick(moment))
        row = connection.execute("SELECT COUNT(*) FROM job_runs").fetchone()
        assert row is not None
        assert created == HOURS
        assert row[0] == HOURS
        states = {
            str(item[0]) for item in connection.execute("SELECT DISTINCT state FROM job_runs")
        }
        assert states == {"queued"}
        distinct = connection.execute(
            "SELECT COUNT(DISTINCT idempotency_key) FROM job_runs"
        ).fetchone()
        assert distinct is not None
        assert distinct[0] == HOURS
        event_count = connection.execute("SELECT COUNT(*) FROM run_events").fetchone()
        assert event_count is not None
        assert event_count[0] == HOURS * 3
        next_run = connection.execute(
            "SELECT next_run_at FROM schedules WHERE id = ?",
            (SCHEDULE_ID,),
        ).fetchone()
        assert next_run is not None
        assert datetime.fromisoformat(str(next_run[0])) == T0 + timedelta(hours=HOURS + 1)
    finally:
        connection.close()
