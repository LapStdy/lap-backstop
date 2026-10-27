"""插件配置端点：schema、当前配置读改、修订号与变更账（docs/03 HTTP API）。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from storage import migrate, open_database

NOW = datetime(2026, 10, 27, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
PLUGIN_ID = "maa"
SCHEMA = {
    "type": "object",
    "properties": {"path": {"type": "string"}},
    "required": ["path"],
}


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Path]:
    path = tmp_path / "state.db"
    connection = open_database(path)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        connection.execute(
            "INSERT INTO plugins (id, name, version, api_version, capabilities, "
            "config_schema, error_map, requirements, enabled, installed_at) VALUES "
            "(?, 'MAA', '6.20.1', 1, '[]', ?, '{}', '{}', 1, ?)",
            (PLUGIN_ID, json.dumps(SCHEMA), NOW.isoformat()),
        )
    finally:
        connection.close()
    yield path


@pytest.fixture
def app(database: Path) -> ApiApp:
    return ApiApp(database, clock=VirtualClock(NOW))


def call(
    app: ApiApp,
    method: str,
    path: str,
    *,
    body: object | None = None,
    headers: dict[str, str] | None = None,
    token: str | None = TOKEN,
) -> ApiResponse:
    merged = {"authorization": f"Bearer {token}"} if token is not None else {}
    merged.update(headers or {})
    raw = b"" if body is None else json.dumps(body).encode("utf-8")
    return app.handle(ApiRequest(method, path, {}, merged, raw))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def config_path(plugin_id: str = PLUGIN_ID) -> str:
    return f"/api/v1/plugins/{plugin_id}/config"


def schema_path(plugin_id: str = PLUGIN_ID) -> str:
    return f"/api/v1/plugins/{plugin_id}/schema"


def revisions(database: Path) -> list[tuple[str, int, str, dict[str, Any]]]:
    connection = open_database(database)
    try:
        rows = connection.execute(
            "SELECT plugin_id, revision, result, diff FROM config_revisions ORDER BY revision"
        ).fetchall()
    finally:
        connection.close()
    return [(str(r[0]), int(r[1]), str(r[2]), json.loads(str(r[3]))) for r in rows]


def test_schema_returns_declared_config_schema(app: ApiApp) -> None:
    response = call(app, "GET", schema_path())
    assert response.status == 200
    assert body_of(response) == {"schema": SCHEMA}


def test_schema_of_unknown_plugin_is_not_found(app: ApiApp) -> None:
    response = call(app, "GET", schema_path("missing"))
    assert response.status == 404
    assert body_of(response)["error"]["code"] == "E_NOT_FOUND"


def test_config_starts_empty_at_revision_zero(app: ApiApp) -> None:
    response = call(app, "GET", config_path())
    assert response.status == 200
    assert body_of(response) == {"config": {}, "revision": 0}
    assert response.etag is not None


def test_put_saves_payload_and_bumps_revision(app: ApiApp, database: Path) -> None:
    payload = {"path": "D:/MAA", "unknown": {"keep": True}}
    response = call(app, "PUT", config_path(), body=payload)
    assert response.status == 200
    assert body_of(response) == {"config": payload, "revision": 1}
    assert response.etag is not None
    assert call(app, "GET", config_path()).payload == {"config": payload, "revision": 1}
    assert revisions(database) == [
        (
            PLUGIN_ID,
            1,
            "applied",
            {"added": {"path": "D:/MAA", "unknown": {"keep": True}}, "removed": {}, "changed": {}},
        )
    ]


def test_second_put_records_change_diff(app: ApiApp, database: Path) -> None:
    call(app, "PUT", config_path(), body={"path": "D:/MAA"})
    response = call(app, "PUT", config_path(), body={"path": "D:/MAA2"})
    assert body_of(response) == {"config": {"path": "D:/MAA2"}, "revision": 2}
    assert revisions(database)[1] == (
        PLUGIN_ID,
        2,
        "applied",
        {"added": {}, "removed": {}, "changed": {"path": {"from": "D:/MAA", "to": "D:/MAA2"}}},
    )


def test_put_with_stale_if_match_is_conflict(app: ApiApp) -> None:
    call(app, "PUT", config_path(), body={"path": "D:/MAA"})
    response = call(
        app, "PUT", config_path(), body={"path": "D:/other"}, headers={"if-match": '"stale"'}
    )
    assert response.status == 409
    assert body_of(response)["error"]["code"] == "E_CONFLICT"


def test_put_with_matching_if_match_succeeds(app: ApiApp) -> None:
    current = call(app, "GET", config_path())
    assert current.etag is not None
    response = call(
        app,
        "PUT",
        config_path(),
        body={"path": "D:/MAA3"},
        headers={"if-match": current.etag},
    )
    assert response.status == 200
    assert body_of(response) == {"config": {"path": "D:/MAA3"}, "revision": 1}


def test_put_rejects_non_object_body(app: ApiApp) -> None:
    response = call(app, "PUT", config_path(), body=["not", "object"])
    assert response.status == 400
    assert body_of(response)["error"]["code"] == "E_BAD_REQUEST"


def test_put_of_unknown_plugin_is_not_found(app: ApiApp) -> None:
    response = call(app, "PUT", config_path("missing"), body={})
    assert response.status == 404
    assert body_of(response)["error"]["code"] == "E_NOT_FOUND"


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", schema_path(), None),
        ("GET", config_path(), None),
        ("PUT", config_path(), {}),
    ],
)
def test_endpoints_require_token(app: ApiApp, method: str, path: str, body: object) -> None:
    response = call(app, method, path, body=body, token=None)
    assert response.status == 401


def test_migration_adds_config_columns_with_defaults(database: Path) -> None:
    connection: sqlite3.Connection = open_database(database)
    try:
        columns = {
            str(row["name"]): row for row in connection.execute("PRAGMA table_info(plugins)")
        }
        assert columns["config"]["dflt_value"] == "'{}'"
        assert columns["config_revision"]["dflt_value"] == "0"
        row = connection.execute(
            "SELECT config, config_revision FROM plugins WHERE id = ?", (PLUGIN_ID,)
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    assert (row["config"], row["config_revision"]) == ("{}", 0)
