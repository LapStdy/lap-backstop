"""作业模板端点：CRUD、payload 保留与引用冲突（docs/03 job_templates 字段表）。"""

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
MINIMAL = {"name": "日常", "plugin_id": "fake", "environment_id": "env-1"}


@pytest.fixture
def app(tmp_path: Path) -> Iterator[ApiApp]:
    database = tmp_path / "state.db"
    connection = open_database(database)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
    finally:
        connection.close()
    yield ApiApp(database, clock=VirtualClock(NOW))


def call(
    app: ApiApp,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> ApiResponse:
    headers = {"authorization": f"Bearer {TOKEN}"}
    if extra_headers is not None:
        headers.update(extra_headers)
    payload = b"" if body is None else json.dumps(body).encode("utf-8")
    return app.handle(ApiRequest(method, path, {}, headers, payload))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def error_code_of(response: ApiResponse) -> str:
    error = body_of(response)["error"]
    assert isinstance(error, dict)
    return str(error["code"])


def test_create_defaults_and_payload_roundtrip(app: ApiApp) -> None:
    full = {
        **MINIMAL,
        "config": {"stage": "1-7", "times": 2},
        "verification_policy": {"policy_id": "p1"},
        "recovery_policy": {"max_attempts": 3},
    }
    response = call(app, "POST", "/api/v1/job-templates", body=full)
    assert response.status == 201
    template = body_of(response)["job_template"]
    assert template["config"] == {"stage": "1-7", "times": 2}
    assert template["verification_policy"] == {"policy_id": "p1"}
    assert template["config_schema_version"] == "1"
    assert template["enabled"] is True
    minimal = call(app, "POST", "/api/v1/job-templates", body=MINIMAL)
    assert body_of(minimal)["job_template"]["config"] == {}


def test_create_rejects_bad_payload_shape(app: ApiApp) -> None:
    response = call(app, "POST", "/api/v1/job-templates", body={**MINIMAL, "config": [1]})
    assert response.status == 400
    assert error_code_of(response) == "E_BAD_REQUEST"
    assert call(app, "POST", "/api/v1/job-templates", body={"name": "x"}).status == 400


def test_put_merges_and_enforces_if_match(app: ApiApp) -> None:
    created = call(app, "POST", "/api/v1/job-templates", body=MINIMAL)
    template_id = str(body_of(created)["job_template"]["id"])
    detail = call(app, "GET", f"/api/v1/job-templates/{template_id}")
    assert detail.etag is not None
    updated = call(
        app,
        "PUT",
        f"/api/v1/job-templates/{template_id}",
        body={"config": {"stage": "1-7"}},
        extra_headers={"if-match": detail.etag},
    )
    assert updated.status == 200
    assert body_of(updated)["job_template"]["config"] == {"stage": "1-7"}
    assert updated.etag != detail.etag
    stale = call(
        app,
        "PUT",
        f"/api/v1/job-templates/{template_id}",
        body={"config": {}},
        extra_headers={"if-match": detail.etag},
    )
    assert stale.status == 409
    assert error_code_of(stale) == "E_CONFLICT"


def test_delete_template(app: ApiApp) -> None:
    created = call(app, "POST", "/api/v1/job-templates", body=MINIMAL)
    template_id = str(body_of(created)["job_template"]["id"])
    assert call(app, "DELETE", f"/api/v1/job-templates/{template_id}").status == 204
    assert call(app, "GET", f"/api/v1/job-templates/{template_id}").status == 404


def test_delete_rejects_template_referenced_by_schedule(app: ApiApp) -> None:
    created = call(app, "POST", "/api/v1/job-templates", body=MINIMAL)
    template_id = str(body_of(created)["job_template"]["id"])
    schedule = call(
        app,
        "POST",
        "/api/v1/schedules",
        body={"name": "每日", "kind": "cron", "expr": "0 4 * * *", "job_template_id": template_id},
    )
    assert schedule.status == 201
    response = call(app, "DELETE", f"/api/v1/job-templates/{template_id}")
    assert response.status == 409
    assert error_code_of(response) == "E_CONFLICT"
