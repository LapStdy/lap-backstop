"""API 错误路径矩阵：缺参数、参数类型错、越界值与未知资源（docs/03 错误映射）。

每条用例断言 HTTP 状态码与错误体结构：顶层只有 error，error 只有非空的
code 与 message；错误码取 docs/03 的 E_ 集合，message 指出字段或资源。
"""

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
MINIMAL = {"name": "每日日常", "kind": "cron", "expr": "0 4 * * *", "job_template_id": TEMPLATE_ID}
TEMPLATE_BODY = {"name": "模板", "plugin_id": "fake", "environment_id": "env-1"}


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
    query: dict[str, str] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> ApiResponse:
    headers = {"authorization": f"Bearer {TOKEN}"}
    if extra_headers is not None:
        headers.update(extra_headers)
    payload = b"" if body is None else json.dumps(body).encode("utf-8")
    return app.handle(ApiRequest(method, path, query or {}, headers, payload))


def error_of(response: ApiResponse) -> dict[str, str]:
    """错误体结构：顶层只有 error，error 只有非空的 code 与 message。"""
    assert isinstance(response.payload, dict), response.payload
    assert set(response.payload) == {"error"}
    error = response.payload["error"]
    assert isinstance(error, dict)
    assert set(error) == {"code", "message"}
    assert isinstance(error["code"], str) and error["code"]
    assert isinstance(error["message"], str) and error["message"]
    return {"code": str(error["code"]), "message": str(error["message"])}


def assert_error(
    response: ApiResponse,
    status: int,
    code: str,
    *,
    fragment: str | None = None,
) -> None:
    assert response.status == status
    error = error_of(response)
    assert error["code"] == code, error["message"]
    if fragment is not None:
        assert fragment in error["message"], error["message"]


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def created_schedule_id(app: ApiApp) -> str:
    created = call(app, "POST", "/api/v1/schedules", body=MINIMAL)
    assert created.status == 201
    return str(body_of(created)["schedule"]["id"])


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/api/v1/schedules", {}),
        ("/api/v1/schedules", {"kind": "cron", "job_template_id": TEMPLATE_ID}),
        ("/api/v1/job-templates", {}),
        ("/api/v1/job-templates", {"name": "模板"}),
    ],
)
def test_missing_required_field_returns_400(app: ApiApp, path: str, body: dict[str, Any]) -> None:
    assert_error(call(app, "POST", path, body=body), 400, "E_BAD_REQUEST", fragment="缺少必填字段")


def test_run_now_without_idempotency_key_returns_400(app: ApiApp) -> None:
    path = f"/api/v1/schedules/{created_schedule_id(app)}/run"
    response = call(app, "POST", path)
    assert_error(response, 400, "E_BAD_REQUEST", fragment="Idempotency-Key")


@pytest.mark.parametrize(
    ("path", "body", "fragment"),
    [
        ("/api/v1/schedules", {**MINIMAL, "window_minutes": "120"}, "window_minutes 必须是整数"),
        ("/api/v1/schedules", {**MINIMAL, "priority": [3]}, "priority 必须是整数"),
        ("/api/v1/schedules", {**MINIMAL, "enabled": "yes"}, "enabled 必须是布尔值"),
        ("/api/v1/schedules", {**MINIMAL, "kind": 7}, "kind 取值必须是"),
        (
            "/api/v1/job-templates",
            {**TEMPLATE_BODY, "config": "not-object"},
            "config 必须是 JSON 对象",
        ),
        (
            "/api/v1/job-templates",
            {**TEMPLATE_BODY, "enabled": 1},
            "enabled 必须是布尔值",
        ),
    ],
)
def test_wrong_field_type_returns_400(
    app: ApiApp, path: str, body: dict[str, Any], fragment: str
) -> None:
    assert_error(call(app, "POST", path, body=body), 400, "E_BAD_REQUEST", fragment=fragment)


def test_settings_body_must_be_json_object(app: ApiApp) -> None:
    empty = app.handle(
        ApiRequest("PUT", "/api/v1/settings", {}, {"authorization": f"Bearer {TOKEN}"}, b"")
    )
    assert_error(empty, 400, "E_BAD_REQUEST", fragment="JSON 对象")
    array = app.handle(
        ApiRequest("PUT", "/api/v1/settings", {}, {"authorization": f"Bearer {TOKEN}"}, b"[1]")
    )
    assert_error(array, 400, "E_BAD_REQUEST", fragment="JSON 对象")


@pytest.mark.parametrize(
    ("body", "code", "fragment"),
    [
        ({**MINIMAL, "window_minutes": -1}, "E_BAD_REQUEST", "window_minutes 不能小于 0"),
        (
            {**MINIMAL, "kind": "interval", "expr": 0},
            "E_BAD_REQUEST",
            "expr 必须是正整数",
        ),
        (
            {**MINIMAL, "window_minutes": 300, "deadline_minutes": 120},
            "E_CONFIG",
            "window_minutes 不能超过 deadline_minutes",
        ),
        ({**MINIMAL, "at": "2026-01-01T00:00:00"}, "E_BAD_REQUEST", "必须带时区"),
    ],
)
def test_out_of_range_value_returns_400(
    app: ApiApp, body: dict[str, Any], code: str, fragment: str
) -> None:
    assert_error(call(app, "POST", "/api/v1/schedules", body=body), 400, code, fragment=fragment)


@pytest.mark.parametrize(
    ("query", "fragment"),
    [
        ({"limit": "0"}, "limit 必须在 1 到 500 之间"),
        ({"limit": "501"}, "limit 必须在 1 到 500 之间"),
        ({"limit": "abc"}, "limit 必须是整数"),
        ({"state": "done"}, "state 取值非法"),
    ],
)
def test_out_of_range_query_returns_400(app: ApiApp, query: dict[str, str], fragment: str) -> None:
    assert_error(
        call(app, "GET", "/api/v1/runs", query=query), 400, "E_BAD_REQUEST", fragment=fragment
    )


@pytest.mark.parametrize(
    ("method", "path", "body", "extra_headers"),
    [
        ("GET", "/api/v1/schedules/nope", None, None),
        ("PUT", "/api/v1/schedules/nope", MINIMAL, None),
        ("DELETE", "/api/v1/schedules/nope", None, None),
        ("POST", "/api/v1/schedules/nope/run", None, {"idempotency-key": "k"}),
        ("GET", "/api/v1/job-templates/nope", None, None),
        ("DELETE", "/api/v1/job-templates/nope", None, None),
        ("GET", "/api/v1/runs/nope", None, None),
        ("GET", "/api/v1/runs/nope/evidence/shot.png", None, None),
        ("GET", "/api/v1/environments/nope", None, None),
        ("GET", "/api/v1/plugins/nope", None, None),
        ("GET", "/api/v1/plugins/nope/schema", None, None),
        ("GET", "/api/v1/plugins/nope/config", None, None),
        ("PUT", "/api/v1/plugins/nope/config", None, None),
    ],
)
def test_unknown_resource_returns_404(
    app: ApiApp,
    method: str,
    path: str,
    body: dict[str, Any] | None,
    extra_headers: dict[str, str] | None,
) -> None:
    response = call(app, method, path, body=body, extra_headers=extra_headers)
    assert_error(response, 404, "E_NOT_FOUND")


def test_unknown_resource_message_names_the_id(app: ApiApp) -> None:
    response = call(app, "GET", "/api/v1/schedules/nope")
    assert_error(response, 404, "E_NOT_FOUND", fragment="nope")
