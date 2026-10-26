"""全局事件保留维护：清理口径与日闸门（docs/03 保留表）。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from contracts import EventType
from core.clock import VirtualClock
from core.events.maintenance import (
    GLOBAL_EVENTS_PRUNED_EVENT,
    GLOBAL_EVENTS_PRUNED_ON_KEY,
    GlobalEventsPruner,
    PruneOutcome,
)
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
