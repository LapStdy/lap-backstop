"""调度端点：新建、默认值、幂等键创建与列表/详情（docs/03 schedules 字段表）。"""

from __future__ import annotations

import json
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

NOW = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
TEMPLATE_ID = "template-1"


@pytest.fixture
def app(tmp_path: Path) -> Iterator[ApiApp]:
    database = tmp_path / "state.db"
    connection = open_database(database)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        connection.execute(
            "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
            "config_schema_version, verification_policy, recovery_policy, enabled, "
            "created_at, updated_at) VALUES (?, '日常', 'fake', 'env-1', '1', '{}', "
            "'{}', 1, ?, ?)",
            (TEMPLATE_ID, NOW.isoformat(), NOW.isoformat()),
        )
    finally:
        connection.close()
    yield ApiApp(database, clock=VirtualClock(NOW))


def call(
    app: ApiApp,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> ApiResponse:
    headers = {"authorization": f"Bearer {TOKEN}"}
    if idempotency_key is not None:
        headers["idempotency-key"] = idempotency_key
    payload = b"" if body is None else json.dumps(body).encode("utf-8")
    return app.handle(ApiRequest(method, path, {}, headers, payload))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def error_code_of(response: ApiResponse) -> str:
    error = body_of(response)["error"]
    assert isinstance(error, dict)
    return str(error["code"])


MINIMAL = {"name": "每日日常", "kind": "cron", "expr": "0 4 * * *", "job_template_id": TEMPLATE_ID}


def test_create_applies_scheduler_defaults(app: ApiApp) -> None:
    response = call(app, "POST", "/api/v1/schedules", body=MINIMAL)
    assert response.status == 201
    assert response.etag is not None
    schedule = body_of(response)["schedule"]
    assert schedule["name"] == "每日日常"
    assert schedule["timezone"] == "Asia/Shanghai"
    assert schedule["catch_up"] == "run_once"
    assert schedule["window_minutes"] == 120
    assert schedule["deadline_minutes"] == 180
    assert schedule["coalesce"] is True
    assert schedule["enabled"] is True
    assert schedule["next_run_at"] is None


def test_create_with_idempotency_key_returns_same_row(app: ApiApp) -> None:
    first = call(app, "POST", "/api/v1/schedules", body=MINIMAL, idempotency_key="key-1")
    second = call(app, "POST", "/api/v1/schedules", body=MINIMAL, idempotency_key="key-1")
    assert first.status == 201
    assert second.status == 200
    assert body_of(second)["schedule"]["id"] == body_of(first)["schedule"]["id"]
    listing = call(app, "GET", "/api/v1/schedules")
    assert len(body_of(listing)["schedules"]) == 1


def test_create_rejects_unknown_or_missing_fields(app: ApiApp) -> None:
    unknown = call(app, "POST", "/api/v1/schedules", body={**MINIMAL, "colour": "red"})
    assert unknown.status == 400
    assert error_code_of(unknown) == "E_BAD_REQUEST"
    missing = call(app, "POST", "/api/v1/schedules", body={"name": "x", "kind": "cron"})
    assert missing.status == 400


def test_create_rejects_window_larger_than_deadline(app: ApiApp) -> None:
    response = call(
        app,
        "POST",
        "/api/v1/schedules",
        body={**MINIMAL, "window_minutes": 300, "deadline_minutes": 120},
    )
    assert response.status == 400
    assert error_code_of(response) == "E_CONFIG"


def test_create_rejects_bad_timezone_kind_and_template(app: ApiApp) -> None:
    bad_timezone = call(
        app,
        "POST",
        "/api/v1/schedules",
        body={**MINIMAL, "timezone": "Mars/Olympus"},
    )
    assert bad_timezone.status == 400
    bad_kind = call(app, "POST", "/api/v1/schedules", body={**MINIMAL, "kind": "daily"})
    assert bad_kind.status == 400
    bad_template = call(
        app,
        "POST",
        "/api/v1/schedules",
        body={**MINIMAL, "job_template_id": "nope"},
    )
    assert bad_template.status == 400
    assert "job_template_id" in str(body_of(bad_template)["error"]["message"])


def test_get_and_list_roundtrip(app: ApiApp) -> None:
    created = call(app, "POST", "/api/v1/schedules", body=MINIMAL)
    schedule_id = body_of(created)["schedule"]["id"]
    detail = call(app, "GET", f"/api/v1/schedules/{schedule_id}")
    assert detail.status == 200
    assert detail.etag == created.etag
    assert body_of(detail)["schedule"]["id"] == schedule_id
    assert call(app, "GET", "/api/v1/schedules/nope").status == 404
