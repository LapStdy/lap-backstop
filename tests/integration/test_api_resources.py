"""资源与租约只读端点（docs/03 /resources、/resources/leases）。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from core.resources.service import LeaseService
from storage import migrate, open_database

NOW = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
TEMPLATE_ID = "template-1"
RUN_1 = "run-1"
RUN_2 = "run-2"

LEASE_FIELDS = {
    "id",
    "resource_id",
    "run_id",
    "owner",
    "state",
    "acquired_at",
    "expires_at",
    "renewed_at",
}


@pytest.fixture
def app(tmp_path: Path) -> Iterator[ApiApp]:
    """带三个资源与三条租约（一条已释放）的应用。"""
    yield _seeded_app(tmp_path)[0]


@pytest.fixture
def bare_app(tmp_path: Path) -> Iterator[ApiApp]:
    """只有 token、没有任何资源与租约的应用。"""
    database = tmp_path / "state.db"
    connection = open_database(database)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
    finally:
        connection.close()
    yield ApiApp(database, clock=VirtualClock(NOW))


def _seeded_app(tmp_path: Path) -> tuple[ApiApp, Path]:
    database = tmp_path / "state.db"
    connection = open_database(database)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        _seed(connection)
    finally:
        connection.close()
    return ApiApp(database, clock=VirtualClock(NOW)), database


def _seed(connection: sqlite3.Connection) -> None:
    """资源、运行与租约的时间线：gpu-1 已释放，acct-1 续租过，env-1 新租。"""
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', 'env-1', '1', '{}', "
        "'{}', 1, ?, ?)",
        (TEMPLATE_ID, NOW.isoformat(), NOW.isoformat()),
    )
    connection.execute(
        "INSERT INTO environments (id, provider_id, name, state, detail) "
        "VALUES ('env-1', 'fake', 'MuMu', 'ready', '{}')",
    )
    connection.executemany(
        "INSERT INTO resources (id, type, name, capacity, detail) VALUES (?, ?, ?, ?, ?)",
        [
            ("gpu-1", "gpu", "训练卡", 1, '{"slots": 4}'),
            ("env-1", "environment", "MuMu", 1, "{}"),
            ("acct-1", "account", "主账号", 2, "{}"),
            ("stor-1", "custom", "仓库", 1, "{}"),
        ],
    )
    for run_id in (RUN_1, RUN_2):
        connection.execute(
            "INSERT INTO job_runs (id, template_id, created_at, idempotency_key, state) "
            "VALUES (?, ?, ?, ?, 'queued')",
            (run_id, TEMPLATE_ID, NOW.isoformat(), f"resources-{run_id}"),
        )
    leases = LeaseService(connection, VirtualClock(NOW))
    leases.acquire(RUN_1, ("gpu-1",), ttl=timedelta(minutes=10))
    leases.release(RUN_1)
    leases.acquire(RUN_2, ("acct-1",), ttl=timedelta(minutes=20))
    leases.renew(RUN_2, ttl=timedelta(minutes=25))
    leases.acquire(RUN_2, ("env-1",), ttl=timedelta(minutes=30))


def call(app: ApiApp, path: str, query: Mapping[str, str] | None = None) -> ApiResponse:
    return app.handle(
        ApiRequest("GET", path, dict(query or {}), {"authorization": f"Bearer {TOKEN}"}, b"")
    )


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def test_empty_tables_return_empty_lists(bare_app: ApiApp) -> None:
    assert body_of(call(bare_app, "/api/v1/resources")) == {"resources": []}
    assert body_of(call(bare_app, "/api/v1/resources/leases")) == {"leases": []}


def test_resources_list_fields_and_order(app: ApiApp) -> None:
    resources = body_of(call(app, "/api/v1/resources"))["resources"]
    assert [item["id"] for item in resources] == ["acct-1", "env-1", "gpu-1", "stor-1"]
    acct, env, gpu, stor = resources
    assert acct["type"] == "account"
    assert acct["name"] == "主账号"
    assert acct["capacity"] == 2
    assert acct["detail"] == {}
    assert env["type"] == "environment"
    assert env["capacity"] == 1
    assert gpu["detail"] == {"slots": 4}
    assert stor["type"] == "custom"
    assert stor["capacity"] == 1


def test_leases_list_fields_and_order(app: ApiApp) -> None:
    leases = body_of(call(app, "/api/v1/resources/leases"))["leases"]
    # 已释放的行也在表里，按 expires_at 升序列出。
    assert [item["resource_id"] for item in leases] == ["gpu-1", "acct-1", "env-1"]
    gpu, acct, env = leases
    for row in leases:
        assert set(row) == LEASE_FIELDS
    assert gpu["run_id"] == RUN_1
    assert gpu["owner"] == "supervisor"
    assert gpu["state"] == "released"
    assert gpu["acquired_at"] == NOW.isoformat()
    assert gpu["expires_at"] == (NOW + timedelta(minutes=10)).isoformat()
    assert gpu["renewed_at"] is None
    assert acct["run_id"] == RUN_2
    assert acct["state"] == "leased"
    assert acct["expires_at"] == (NOW + timedelta(minutes=25)).isoformat()
    assert acct["renewed_at"] == NOW.isoformat()
    assert env["state"] == "leased"
    assert env["expires_at"] == (NOW + timedelta(minutes=30)).isoformat()
    assert env["renewed_at"] is None


def test_query_params_do_not_filter_resources(app: ApiApp) -> None:
    filtered = body_of(call(app, "/api/v1/resources", {"state": "free"}))
    assert filtered == body_of(call(app, "/api/v1/resources"))


def test_calls_leave_table_row_counts_unchanged(tmp_path: Path) -> None:
    app, database = _seeded_app(tmp_path)
    with closing(open_database(database)) as watcher:
        before = _row_counts(watcher)
    call(app, "/api/v1/resources")
    call(app, "/api/v1/resources/leases")
    with closing(open_database(database)) as watcher:
        after = _row_counts(watcher)
    assert before == after == {"resources": 4, "resource_leases": 3}


def test_resources_without_token_is_rejected(bare_app: ApiApp) -> None:
    response = bare_app.handle(ApiRequest("GET", "/api/v1/resources", {}, {}, b""))
    assert response.status == 401


def test_leases_without_token_is_rejected(bare_app: ApiApp) -> None:
    response = bare_app.handle(ApiRequest("GET", "/api/v1/resources/leases", {}, {}, b""))
    assert response.status == 401


def _row_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in ("resources", "resource_leases")
    }
