"""通知记录端点：字段、排序、过滤、截断、鉴权与只读（docs/03 GET /notifications）。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from storage import migrate, open_database
from tests.notify_support import NOW, create_run, insert_template

TOKEN = "secret-token"
PATH = "/api/v1/notifications"

NOTIFICATION_FIELDS = {"id", "run_id", "channel", "status", "attempts", "sent_at", "error"}


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Path]:
    path = tmp_path / "state.db"
    connection = open_database(path)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
    finally:
        connection.close()
    yield path


@pytest.fixture
def app(database: Path) -> ApiApp:
    """只有 token、没有任何通知记录的应用。"""
    return ApiApp(database, clock=VirtualClock(NOW))


@pytest.fixture
def seeded_app(database: Path) -> ApiApp:
    """带四条通知记录（两条 sent、一条 pending、一条 failed）的应用。"""
    connection = open_database(database)
    try:
        _seed(connection)
    finally:
        connection.close()
    return ApiApp(database, clock=VirtualClock(NOW))


@pytest.fixture
def bulk_app(database: Path) -> ApiApp:
    """带 501 条同刻通知记录的应用，供 limit 上限截断用。"""
    connection = open_database(database)
    try:
        connection.executemany(
            "INSERT INTO notifications (id, run_id, channel, status, attempts, sent_at, error) "
            "VALUES (?, NULL, 'serverchan', 'sent', 1, '2026-10-26T10:00:00+00:00', NULL)",
            [(f"bulk-{index:04d}",) for index in range(501)],
        )
    finally:
        connection.close()
    return ApiApp(database, clock=VirtualClock(NOW))


def _seed(connection: sqlite3.Connection) -> None:
    """时间线：n-2 最新的 sent，n-1 次之；pending 与 failed 无 sent_at 排在已发送之后。"""
    insert_template(connection)
    run_a = create_run(connection, "records:a")
    run_b = create_run(connection, "records:b")
    connection.executemany(
        "INSERT INTO notifications (id, run_id, channel, status, attempts, sent_at, error) "
        "VALUES (?, ?, 'serverchan', ?, ?, ?, ?)",
        [
            ("n-1", run_a, "sent", 1, "2026-10-26T11:00:00+00:00", None),
            ("n-2", run_b, "sent", 1, "2026-10-26T12:00:00+00:00", None),
            ("n-3", None, "pending", 0, None, None),
            ("n-4", run_a, "failed", 3, None, "E_NOTIFY：HTTP 500"),
        ],
    )


def call(
    app: ApiApp,
    query: Mapping[str, str] | None = None,
    *,
    token: str | None = TOKEN,
) -> ApiResponse:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return app.handle(ApiRequest("GET", PATH, dict(query or {}), headers, b""))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def test_empty_table_returns_empty_list(app: ApiApp) -> None:
    assert body_of(call(app)) == {"notifications": []}


def test_list_fields_and_order(seeded_app: ApiApp) -> None:
    notifications = body_of(call(seeded_app))["notifications"]
    # sent_at 倒序在前，无 sent_at 的按 id 倒序垫底。
    assert [item["id"] for item in notifications] == ["n-2", "n-1", "n-4", "n-3"]
    by_id = {item["id"]: item for item in notifications}
    for row in by_id.values():
        assert set(row) == NOTIFICATION_FIELDS
        assert row["channel"] == "serverchan"
    newest = by_id["n-2"]
    assert newest["status"] == "sent"
    assert newest["attempts"] == 1
    assert newest["sent_at"] == "2026-10-26T12:00:00+00:00"
    assert newest["error"] is None
    assert isinstance(newest["run_id"], str)
    assert by_id["n-1"]["sent_at"] == "2026-10-26T11:00:00+00:00"
    assert by_id["n-3"] == {
        "id": "n-3",
        "run_id": None,
        "channel": "serverchan",
        "status": "pending",
        "attempts": 0,
        "sent_at": None,
        "error": None,
    }
    failed = by_id["n-4"]
    assert failed["status"] == "failed"
    assert failed["attempts"] == 3
    assert failed["sent_at"] is None
    assert failed["error"] == "E_NOTIFY：HTTP 500"


def test_filter_by_run_id(seeded_app: ApiApp) -> None:
    notifications = body_of(call(seeded_app))["notifications"]
    by_id = {item["id"]: item for item in notifications}
    run_id = by_id["n-1"]["run_id"]
    assert isinstance(run_id, str)

    filtered = body_of(call(seeded_app, {"run_id": run_id}))["notifications"]

    assert [item["id"] for item in filtered] == ["n-1", "n-4"]


def test_filter_by_status(seeded_app: ApiApp) -> None:
    sent = body_of(call(seeded_app, {"status": "sent"}))["notifications"]
    pending = body_of(call(seeded_app, {"status": "pending"}))["notifications"]
    failed = body_of(call(seeded_app, {"status": "failed"}))["notifications"]
    unmatched = body_of(call(seeded_app, {"status": "banana"}))["notifications"]

    assert [item["id"] for item in sent] == ["n-2", "n-1"]
    assert [item["id"] for item in pending] == ["n-3"]
    assert [item["id"] for item in failed] == ["n-4"]
    assert unmatched == []


def test_limit_returns_requested_count(seeded_app: ApiApp) -> None:
    limited = body_of(call(seeded_app, {"limit": "2"}))["notifications"]

    assert [item["id"] for item in limited] == ["n-2", "n-1"]


def test_limit_is_capped_and_invalid_falls_back_to_default(bulk_app: ApiApp) -> None:
    capped = body_of(call(bulk_app, {"limit": "1000"}))["notifications"]
    default = body_of(call(bulk_app))["notifications"]
    invalid = body_of(call(bulk_app, {"limit": "abc"}))["notifications"]
    floor = body_of(call(bulk_app, {"limit": "0"}))["notifications"]

    assert len(capped) == 500
    assert capped[0]["id"] == "bulk-0500" and capped[-1]["id"] == "bulk-0001"
    assert len(default) == 100
    assert len(invalid) == 100
    assert len(floor) == 1


def test_unknown_query_params_are_ignored(seeded_app: ApiApp) -> None:
    plain = body_of(call(seeded_app))

    noisy = body_of(call(seeded_app, {"channel": "dingtalk", "foo": "bar"}))

    assert noisy == plain


def test_missing_or_wrong_token_is_rejected(app: ApiApp) -> None:
    missing = call(app, token=None)
    wrong = call(app, {"status": "sent"}, token="wrong")

    assert missing.status == 401
    assert wrong.status == 401


def test_calls_leave_table_row_counts_unchanged(database: Path) -> None:
    connection = open_database(database)
    try:
        _seed(connection)
    finally:
        connection.close()
    app = ApiApp(database, clock=VirtualClock(NOW))

    with closing(open_database(database)) as watcher:
        before = _row_count(watcher)
    call(app)
    call(app, {"run_id": "run-1", "status": "sent", "limit": "50", "bogus": "x"})
    with closing(open_database(database)) as watcher:
        after = _row_count(watcher)

    assert before == after == 4


def _row_count(connection: sqlite3.Connection) -> int:
    return int(connection.execute("SELECT COUNT(*) FROM notifications").fetchone()[0])
