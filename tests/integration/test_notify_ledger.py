"""通知账目可靠性：终态只落一次、账目写入的线程约束与停机收尾。

notifications 表的 pending 会出现在投递历史里（docs/06），它只能是短暂中间态：
终态由 poll() 线程落库，停机关闭时未投递的条目落成 failed 并留事件。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest

from contracts import EventType, Logger
from core.clock import VirtualClock
from core.logging import JsonLineLogger
from core.notify.service import NotifyService
from storage import BUSY_TIMEOUT_MS, migrate
from storage.repositories.notifications import NotificationRepository
from tests.notify_support import (
    ENABLED,
    NOW,
    StubChannel,
    append_run_event,
    create_run,
    insert_template,
    record_outcomes_within,
)

AT = NOW


class ThreadRecordingConnection:
    """包住 SQLite 连接，记下 notifications 表每次写入发生在哪个线程。"""

    def __init__(self, inner: sqlite3.Connection) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self.ledger_writes: list[tuple[str, str]] = []

    def execute(self, sql: str, parameters: Any = ()) -> sqlite3.Cursor:
        if _is_ledger_write(sql):
            with self._lock:
                self.ledger_writes.append((threading.current_thread().name, sql))
        return self._inner.execute(sql, parameters)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _is_ledger_write(sql: str) -> bool:
    text = " ".join(sql.split()).upper()
    return text.startswith(("INSERT INTO NOTIFICATIONS", "UPDATE NOTIFICATIONS"))


@pytest.fixture
def threaded_connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """放开 SQLite 的线程隶属检查，让用例自己判定「写发生在哪个线程」。"""
    database = sqlite3.connect(
        tmp_path / "backstop.sqlite",
        timeout=BUSY_TIMEOUT_MS / 1000,
        check_same_thread=False,
        isolation_level=None,
    )
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA journal_mode = WAL")
    database.execute("PRAGMA foreign_keys = ON")
    migrate(database)
    insert_template(database)
    try:
        yield database
    finally:
        database.close()


def _service(
    connection: sqlite3.Connection, stub: StubChannel, *, log: Logger | None = None
) -> NotifyService:
    return NotifyService(
        connection,
        VirtualClock(NOW),
        data_root=Path("data"),
        log=log,
        settings_reader=lambda: dict(ENABLED),
        channel_factory=lambda config: stub,
        wait=lambda seconds: None,
    )


def _ledger_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute("SELECT * FROM notifications ORDER BY id").fetchall()


def _ledger_status(connection: sqlite3.Connection) -> str | None:
    row = connection.execute("SELECT status FROM notifications").fetchone()
    return None if row is None else str(row["status"])


def _queue_three_deliveries(connection: sqlite3.Connection) -> None:
    for index in range(3):
        run_id = create_run(connection, f"notify:close-{index}")
        append_run_event(
            connection,
            run_id,
            EventType.RUN_FINISHED,
            {"run_id": run_id, "status": "failed", "duration_ms": 1000},
        )


def test_mark_sent_on_missing_row_raises(connection: sqlite3.Connection) -> None:
    repository = NotificationRepository(connection)
    with pytest.raises(sqlite3.DatabaseError, match="通知账目不存在"):
        repository.mark_sent("missing-id", attempts=1, at=AT)


def test_mark_failed_on_missing_row_raises(connection: sqlite3.Connection) -> None:
    repository = NotificationRepository(connection)
    with pytest.raises(sqlite3.DatabaseError, match="通知账目不存在"):
        repository.mark_failed("missing-id", attempts=0, error="渠道不可用")


def test_terminal_status_is_not_rewritten(connection: sqlite3.Connection) -> None:
    repository = NotificationRepository(connection)
    repository.insert_pending("n-1", None, "serverchan")
    repository.mark_sent("n-1", attempts=2, at=AT)
    with pytest.raises(sqlite3.DatabaseError, match="已是 sent"):
        repository.mark_failed("n-1", attempts=3, error="HTTP 500")
    row = _ledger_rows(connection)[0]
    assert (row["status"], row["attempts"], row["error"]) == ("sent", 2, None)


def test_ledger_writes_stay_on_poll_thread(
    threaded_connection: sqlite3.Connection, stub: StubChannel
) -> None:
    connection = threaded_connection
    proxy = ThreadRecordingConnection(connection)
    service = _service(cast(sqlite3.Connection, proxy), stub)
    try:
        run_id = create_run(connection, "notify:thread")
        append_run_event(
            connection,
            run_id,
            EventType.RUN_FINISHED,
            {"run_id": run_id, "status": "succeeded", "duration_ms": 1000},
        )
        service.poll()
        assert stub.delivered.wait(2.0)
        _assert_writes_on_poll_thread(proxy)
        assert record_outcomes_within(service, lambda: _ledger_status(connection) == "sent")
    finally:
        service.close()
    assert set(stub.threads) == {"notify-delivery"}
    _assert_writes_on_poll_thread(proxy)
    assert len(proxy.ledger_writes) == 2


def test_close_finalizes_undelivered_rows_on_poll_thread(
    threaded_connection: sqlite3.Connection, stub: StubChannel
) -> None:
    connection = threaded_connection
    proxy = ThreadRecordingConnection(connection)
    service = _service(cast(sqlite3.Connection, proxy), stub)
    stub.gate = threading.Event()
    _queue_three_deliveries(connection)
    try:
        service.poll()
        assert stub.entered.wait(2.0)
        _assert_writes_on_poll_thread(proxy)
        service.close(join_timeout=0.05)
        rows = _ledger_rows(connection)
        assert len(rows) == 3
        assert {row["status"] for row in rows} == {"failed"}
        assert {row["attempts"] for row in rows} == {0}
        assert {row["error"] for row in rows} == {"停机放弃投递"}
    finally:
        stub.gate.set()
    _assert_writes_on_poll_thread(proxy)
    assert len(proxy.ledger_writes) == 6
    failed = connection.execute(
        "SELECT payload FROM run_events WHERE type = ?", (EventType.NOTIFY_FAILED.value,)
    ).fetchall()
    assert len(failed) == 3


def _assert_writes_on_poll_thread(proxy: ThreadRecordingConnection) -> None:
    names = {name for name, _sql in proxy.ledger_writes}
    assert names == {threading.current_thread().name}, f"账目写入发生在 {names}"


def test_close_logs_each_undelivered_id_when_ledger_is_gone(
    connection: sqlite3.Connection, stub: StubChannel, tmp_path: Path
) -> None:
    log = JsonLineLogger(tmp_path / "notify.jsonl", clock=VirtualClock(NOW))
    service = _service(connection, stub, log=log)
    stub.gate = threading.Event()
    _queue_three_deliveries(connection)
    try:
        service.poll()
        assert stub.entered.wait(2.0)
        connection.close()
        service.close(join_timeout=0.05)
    finally:
        stub.gate.set()
    lines = (tmp_path / "notify.jsonl").read_text("utf-8").splitlines()
    records = [json.loads(line) for line in lines]
    shutdown = [
        record for record in records if record.get("event") == "notify.shutdown_ledger_unavailable"
    ]
    assert len(shutdown) == 3
    assert all(item.get("notification_id") for item in shutdown)


def test_refused_enqueue_records_failed_row(
    connection: sqlite3.Connection,
    stub: StubChannel,
    monkeypatch: Any,
) -> None:
    service: Any = _service(connection, stub)
    service.poll()
    monkeypatch.setattr(service, "_worker", _RefusingWorker(service._worker))
    run_id = create_run(connection, "notify:refused")
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "succeeded", "duration_ms": 1000},
    )
    service.poll()
    rows = _ledger_rows(connection)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["error"] == "停机放弃投递"
    assert stub.sent == []


class _RefusingWorker:
    """模拟入队瞬间被停机抢先：投递线程拒绝接收，账目必须当场落终态。"""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def submit(self, item: Any) -> bool:
        return False
