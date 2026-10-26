"""全局事件保留维护：清理口径与日闸门（docs/03 保留表）。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import EventType, Logger
from core.clock import VirtualClock
from core.events.maintenance import (
    GLOBAL_EVENTS_PRUNED_EVENT,
    GLOBAL_EVENTS_PRUNED_ON_KEY,
    GlobalEventsPruner,
    PruneOutcome,
)
from core.hostops.serve import HeartbeatWriter, ServeLoop, ServeOptions, run_serve
from core.jobs.run_service import JobRunService
from storage import migrate, open_database
from storage.repositories.events import GLOBAL_EVENTS_KEEP, EventLog

NOW = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = open_database(tmp_path / "backstop.sqlite")
    migrate(connection)
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def clock() -> VirtualClock:
    return VirtualClock(NOW)


def last_global_seq(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COALESCE(MAX(seq), 0) FROM events_global").fetchone()
    return int(row[0])


def global_count(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) FROM events_global").fetchone()
    return int(row[0])


def min_global_seq(connection: sqlite3.Connection) -> int | None:
    row = connection.execute("SELECT MIN(seq) FROM events_global").fetchone()
    return None if row[0] is None else int(row[0])


def seed_global_events(connection: sqlite3.Connection, count: int) -> None:
    """按显式序号批量种全局事件，从当前末尾接续，模拟已积累的历史。"""
    start = last_global_seq(connection) + 1
    connection.executemany(
        "INSERT INTO events_global (seq, type, payload, ts) VALUES (?, ?, '{}', ?)",
        [
            (start + offset, EventType.CORE_EVENT.value, NOW.isoformat())
            for offset in range(count)
        ],
    )


def core_event_payloads(connection: sqlite3.Connection, name: str) -> list[dict[str, Any]]:
    payloads = []
    rows = connection.execute(
        "SELECT payload FROM events_global WHERE type = ? ORDER BY seq",
        (EventType.CORE_EVENT.value,),
    ).fetchall()
    for row in rows:
        payload = json.loads(str(row["payload"]))
        if payload.get("event") == name:
            payloads.append(payload)
    return payloads


def pruned_on_value(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        "SELECT value FROM settings WHERE key = ?",
        (GLOBAL_EVENTS_PRUNED_ON_KEY,),
    ).fetchone()
    return None if row is None else str(row[0])


def test_prune_keeps_exactly_1000_without_deleting(database: sqlite3.Connection) -> None:
    seed_global_events(database, GLOBAL_EVENTS_KEEP)
    deleted = EventLog(database).prune_global()
    assert deleted == 0
    assert global_count(database) == GLOBAL_EVENTS_KEEP
    assert min_global_seq(database) == 1


def test_prune_1001_deletes_only_oldest(database: sqlite3.Connection) -> None:
    seed_global_events(database, GLOBAL_EVENTS_KEEP + 1)
    deleted = EventLog(database).prune_global()
    assert deleted == 1
    assert global_count(database) == GLOBAL_EVENTS_KEEP
    assert min_global_seq(database) == 2
    assert last_global_seq(database) == GLOBAL_EVENTS_KEEP + 1


def test_prune_deletes_all_rows_beyond_keep(database: sqlite3.Connection) -> None:
    seed_global_events(database, GLOBAL_EVENTS_KEEP + 300)
    deleted = EventLog(database).prune_global()
    assert deleted == 300
    assert global_count(database) == GLOBAL_EVENTS_KEEP


def test_prune_empty_table_returns_zero(database: sqlite3.Connection) -> None:
    assert EventLog(database).prune_global() == 0
    assert global_count(database) == 0


def test_prune_repeat_call_is_idempotent(database: sqlite3.Connection) -> None:
    seed_global_events(database, GLOBAL_EVENTS_KEEP + 100)
    log = EventLog(database)
    assert log.prune_global() == 100
    assert log.prune_global() == 0
    assert global_count(database) == GLOBAL_EVENTS_KEEP


def test_prune_keeps_survivor_seq_and_autoincrement(database: sqlite3.Connection) -> None:
    seed_global_events(database, GLOBAL_EVENTS_KEEP + 5)
    log = EventLog(database)
    assert log.prune_global() == 5
    appended = log.append_global(EventType.CORE_EVENT, {"event": "x"}, at=NOW)
    assert appended.seq == GLOBAL_EVENTS_KEEP + 6


def test_pruner_runs_once_per_day_then_again_next_day(
    database: sqlite3.Connection, clock: VirtualClock
) -> None:
    seed_global_events(database, GLOBAL_EVENTS_KEEP + 100)
    pruner = GlobalEventsPruner(database, clock)
    assert pruner.run_once() == PruneOutcome(deleted=100, retained=GLOBAL_EVENTS_KEEP)
    assert pruned_on_value(database) == json.dumps(NOW.date().isoformat())
    assert core_event_payloads(database, GLOBAL_EVENTS_PRUNED_EVENT) == [
        {
            "event": GLOBAL_EVENTS_PRUNED_EVENT,
            "detail": {"deleted": 100, "retained": GLOBAL_EVENTS_KEEP},
        }
    ]

    assert pruner.run_once() is None
    assert len(core_event_payloads(database, GLOBAL_EVENTS_PRUNED_EVENT)) == 1

    clock.advance(timedelta(days=1))
    assert pruner.run_once() == PruneOutcome(deleted=1, retained=GLOBAL_EVENTS_KEEP)
    assert len(core_event_payloads(database, GLOBAL_EVENTS_PRUNED_EVENT)) == 2
    assert pruned_on_value(database) == json.dumps((NOW + timedelta(days=1)).date().isoformat())


def test_pruner_restart_same_day_skips_via_settings(
    tmp_path: Path, clock: VirtualClock
) -> None:
    path = tmp_path / "backstop.sqlite"
    first = open_database(path)
    migrate(first)
    try:
        seed_global_events(first, GLOBAL_EVENTS_KEEP)
        GlobalEventsPruner(first, clock).run_once()
    finally:
        first.close()
    reopened = open_database(path)
    try:
        assert GlobalEventsPruner(reopened, clock).run_once() is None
        assert len(core_event_payloads(reopened, GLOBAL_EVENTS_PRUNED_EVENT)) == 1
    finally:
        reopened.close()


@pytest.mark.parametrize("raw_value", ['"not-a-date"', "123", "null", "{"])
def test_pruner_treats_bad_date_value_as_never_pruned(
    database: sqlite3.Connection, clock: VirtualClock, raw_value: str
) -> None:
    seed_global_events(database, GLOBAL_EVENTS_KEEP + 1)
    database.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        (GLOBAL_EVENTS_PRUNED_ON_KEY, raw_value, NOW.isoformat()),
    )
    pruner = GlobalEventsPruner(database, clock)
    assert pruner.run_once() == PruneOutcome(deleted=1, retained=GLOBAL_EVENTS_KEEP)
    assert pruned_on_value(database) == json.dumps(NOW.date().isoformat())


class _StubSupervisor:
    """失败路径专用的最小 Supervisor：只计数 tick。"""

    def __init__(self) -> None:
        self.ticks = 0

    def tick(self) -> None:
        self.ticks += 1

    @property
    def is_disk_critical(self) -> bool:
        return False


class _RecordingLogger:
    """只留 error 行的记录器；断言维护失败进了日志。"""

    def __init__(self) -> None:
        self.errors: list[dict[str, Any]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        self.errors.append(dict(fields))


def test_serve_loop_survives_prune_failure_and_keeps_ticking(
    tmp_path: Path, clock: VirtualClock
) -> None:
    connection = open_database(tmp_path / "backstop.sqlite")
    migrate(connection)
    broken = open_database(tmp_path / "broken.sqlite")
    broken.close()
    recorder = _RecordingLogger()
    supervisor = _StubSupervisor()
    try:
        loop = ServeLoop(
            supervisor,
            JobRunService(connection, clock),
            HeartbeatWriter(
                path=tmp_path / "run" / "heartbeat.json",
                started_at=NOW,
                pid=1,
                version="dev",
            ),
            ServeOptions(clock=clock, max_ticks=2, tick_interval_seconds=0.0, log=recorder),
            pruner=GlobalEventsPruner(broken, clock),
        )
        assert loop.run() == 0
        assert supervisor.ticks == 2
        assert recorder.errors
        assert recorder.errors[0]["event"] == "maintenance.global_events_prune_failed"
        # 失败不落闸门键，重启或下个 tick 还会补试
        assert pruned_on_value(connection) is None
    finally:
        connection.close()


def test_run_serve_executes_prune_once_and_survives_restart(tmp_path: Path) -> None:
    options = ServeOptions(
        clock=VirtualClock(NOW),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        registry_get=lambda _key, _name: "0",
    )
    assert run_serve(tmp_path, options) == 0
    reader = open_database(tmp_path / "backstop.sqlite")
    try:
        assert pruned_on_value(reader) == json.dumps(NOW.date().isoformat())
        assert core_event_payloads(reader, GLOBAL_EVENTS_PRUNED_EVENT) == [
            {"event": GLOBAL_EVENTS_PRUNED_EVENT, "detail": {"deleted": 0, "retained": 1}}
        ]
    finally:
        reader.close()
    assert run_serve(tmp_path, options) == 0
    reader = open_database(tmp_path / "backstop.sqlite")
    try:
        assert len(core_event_payloads(reader, GLOBAL_EVENTS_PRUNED_EVENT)) == 1
    finally:
        reader.close()
