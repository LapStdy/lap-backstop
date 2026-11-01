"""启动清扫：硬杀遗留的 pending 通知账目在下次启动落成 failed 并留证据。

pending 只能是进程存活期间的短暂中间态（docs/03）；清扫落终态与证据事件
绑定在同一事务，可只读事件复现「清扫了哪几条」。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, cast

from core.clock import VirtualClock
from core.logging import JsonLineLogger
from core.notify.sweep import SWEEP_ERROR, sweep_pending_notifications
from storage.repositories.notifications import NotificationRepository
from tests.notify_support import NOW, core_events


def _insert_pending(
    connection: sqlite3.Connection, notification_id: str, *, attempts: int = 0
) -> None:
    NotificationRepository(connection).insert_pending(notification_id, None, "serverchan")
    if attempts:
        connection.execute(
            "UPDATE notifications SET attempts = ? WHERE id = ?", (attempts, notification_id)
        )


def _rows_by_id(connection: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    return {
        str(row["id"]): row
        for row in connection.execute("SELECT * FROM notifications ORDER BY rowid").fetchall()
    }


def _sweep(connection: sqlite3.Connection, log: JsonLineLogger | None = None) -> int:
    return sweep_pending_notifications(connection, VirtualClock(NOW), log=log)


def test_sweep_finalizes_legacy_pending_rows_with_matching_events(
    connection: sqlite3.Connection,
) -> None:
    _insert_pending(connection, "n-left-1")
    _insert_pending(connection, "n-left-2", attempts=2)
    _insert_pending(connection, "n-left-3", attempts=5)
    swept = _sweep(connection)
    assert swept == 3, f"应清扫 3 行，实际 {swept}"
    rows = _rows_by_id(connection)
    assert {row["status"] for row in rows.values()} == {"failed"}
    assert {row["error"] for row in rows.values()} == {SWEEP_ERROR}
    assert all(row["sent_at"] is None for row in rows.values())
    assert (rows["n-left-1"]["attempts"], rows["n-left-2"]["attempts"]) == (0, 2)
    events = core_events(connection, "notify.pending_swept")
    assert [event["payload"]["notification_id"] for event in events] == [
        "n-left-1",
        "n-left-2",
        "n-left-3",
    ]
    assert {event["payload"]["channel"] for event in events} == {"serverchan"}
    assert {event["payload"]["error"] for event in events} == {SWEEP_ERROR}


def test_sweep_without_pending_writes_no_events(connection: sqlite3.Connection) -> None:
    repository = NotificationRepository(connection)
    repository.insert_pending("n-none", None, "serverchan")
    repository.mark_sent("n-none", attempts=1, at=NOW)
    swept = _sweep(connection)
    assert swept == 0
    assert core_events(connection, "notify.pending_swept") == []


def test_sweep_leaves_terminal_rows_untouched(connection: sqlite3.Connection) -> None:
    repository = NotificationRepository(connection)
    repository.insert_pending("n-sent", None, "serverchan")
    repository.mark_sent("n-sent", attempts=2, at=NOW)
    repository.insert_pending("n-failed", None, "serverchan")
    repository.mark_failed("n-failed", attempts=3, error="HTTP 500")
    _insert_pending(connection, "n-sweep-me")
    before = {
        notification_id: dict(row)
        for notification_id, row in _rows_by_id(connection).items()
        if notification_id in {"n-sent", "n-failed"}
    }
    swept = _sweep(connection)
    assert swept == 1
    rows = _rows_by_id(connection)
    for notification_id in ("n-sent", "n-failed"):
        assert dict(rows[notification_id]) == before[notification_id]
    assert rows["n-sweep-me"]["status"] == "failed"
    events = core_events(connection, "notify.pending_swept")
    assert [event["payload"]["notification_id"] for event in events] == ["n-sweep-me"]


def test_sweep_twice_only_touches_rows_once(connection: sqlite3.Connection) -> None:
    _insert_pending(connection, "n-again-1")
    _insert_pending(connection, "n-again-2", attempts=1)
    assert _sweep(connection) == 2
    events_after_first = core_events(connection, "notify.pending_swept")
    rows_after_first = {key: dict(row) for key, row in _rows_by_id(connection).items()}
    swept = _sweep(connection)
    assert swept == 0
    assert core_events(connection, "notify.pending_swept") == events_after_first
    assert {key: dict(row) for key, row in _rows_by_id(connection).items()} == rows_after_first


class _PoisonedConnection:
    """对特定通知 id 的账目 UPDATE 抛错，其余照常转发；验证清扫的单行容错。"""

    def __init__(self, inner: sqlite3.Connection, poisoned_id: str) -> None:
        self._inner = inner
        self._poisoned_id = poisoned_id

    def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
        if _is_ledger_update(sql) and self._poisoned_id in tuple(parameters):
            raise sqlite3.OperationalError(f"注入失败：{self._poisoned_id}")
        return self._inner.execute(sql, parameters)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _is_ledger_update(sql: str) -> bool:
    return " ".join(sql.split()).upper().startswith("UPDATE NOTIFICATIONS")


def test_sweep_continues_when_single_row_write_fails(
    connection: sqlite3.Connection, tmp_path: Path
) -> None:
    _insert_pending(connection, "n-ok-1")
    _insert_pending(connection, "n-broken")
    _insert_pending(connection, "n-ok-2")
    log = JsonLineLogger(tmp_path / "sweep.jsonl", clock=VirtualClock(NOW))
    poisoned = _PoisonedConnection(connection, "n-broken")
    swept = _sweep(cast(sqlite3.Connection, poisoned), log=log)
    assert swept == 2, f"坏行之外的 2 行应照常清扫，实际 {swept}"
    rows = _rows_by_id(connection)
    assert rows["n-broken"]["status"] == "pending"
    assert {rows["n-ok-1"]["status"], rows["n-ok-2"]["status"]} == {"failed"}
    events = core_events(connection, "notify.pending_swept")
    assert {event["payload"]["notification_id"] for event in events} == {"n-ok-1", "n-ok-2"}
    records = [
        json.loads(line) for line in (tmp_path / "sweep.jsonl").read_text("utf-8").splitlines()
    ]
    failures = [record for record in records if record.get("event") == "notify.sweep_row_failed"]
    assert [record.get("notification_id") for record in failures] == ["n-broken"]
