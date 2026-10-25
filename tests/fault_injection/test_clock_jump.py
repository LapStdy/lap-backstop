"""故障注入：虚拟时钟前跳 1 小时，补跑按窗口与 catch_up 策略收敛（docs/12 故障注入表「时钟跳变」）。

判据是「补跑按窗口判定，不连发多次」：一次前跳跨过整批发生点，
落库的运行按策略合并或只补最近一次，而不是每个错过点各建一个。
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.scheduler.scan import ScheduleScanner
from storage import migrate, open_database

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
SCHEDULE_ID = "every-5m"


def insert_schedule(connection: sqlite3.Connection, catch_up: str) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES ('template-1', '日常', 'fake', 'env-1', "
        "'1', '{}', '{}', 1, ?, ?)",
        (T0.isoformat(), T0.isoformat()),
    )
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, "
        "window_minutes, coalesce, priority, deadline_minutes, preheat_minutes, "
        "keep_warm_minutes, job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每五分钟', 'interval', '5', NULL, 'UTC', ?, 10, 1, 0, 180, 0, 0, "
        "'template-1', 1, NULL, ?, ?)",
        (SCHEDULE_ID, catch_up, T0.isoformat(), T0.isoformat()),
    )


def next_run_at(connection: sqlite3.Connection) -> datetime:
    row = connection.execute(
        "SELECT next_run_at FROM schedules WHERE id = ?", (SCHEDULE_ID,)
    ).fetchone()
    assert row is not None and row[0] is not None
    value = datetime.fromisoformat(str(row[0]))
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def merged_ats(runs: JobRunService, run_id: str) -> list[str]:
    created = [event for event in runs.events(run_id) if event.type == "run.created"]
    assert len(created) == 1
    merged = created[0].payload.get("merged_scheduled_ats")
    assert isinstance(merged, list)
    return [str(item) for item in merged]


@pytest.mark.parametrize(
    ("catch_up", "expected_merged"),
    [("window", 2), ("run_once", 0)],
)
def test_clock_jump_ahead_one_hour_creates_single_run(
    tmp_path: Path,
    catch_up: str,
    expected_merged: int,
) -> None:
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        insert_schedule(connection, catch_up)
        clock = VirtualClock(T0)
        runs = JobRunService(connection, clock)
        scanner = ScheduleScanner(connection, clock, runs)
        scanner.tick(T0)
        anchor = next_run_at(connection)

        clock.jump_to(anchor + timedelta(hours=1))
        created = scanner.tick(clock.utc_now())

        assert len(created) == 1
        row = connection.execute("SELECT COUNT(*) FROM job_runs").fetchone()
        assert row is not None
        assert row[0] == 1
        assert created[0].scheduled_at == anchor + timedelta(hours=1)
        assert len(merged_ats(runs, created[0].id)) == expected_merged
    finally:
        connection.close()
