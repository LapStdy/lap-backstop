"""API 应用骨干：鉴权、路由 404、坏 JSON 400 与 health（docs/03 鉴权与错误码）。"""

from __future__ import annotations

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


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Path]:
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
    finally:
        connection.close()
    yield tmp_path / "state.db"


@pytest.fixture
def app(database: Path) -> ApiApp:
    return ApiApp(database, clock=VirtualClock(NOW))


def call(
    app: ApiApp,
    method: str,
    path: str,
    *,
    token: str | None = TOKEN,
    body: bytes = b"",
) -> ApiResponse:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return app.handle(ApiRequest(method=method, path=path, query={}, headers=headers, body=body))


def payload_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def error_code_of(response: ApiResponse) -> str:
    error = payload_of(response)["error"]
    assert isinstance(error, dict)
    return str(error["code"])


def test_health_returns_version_server_id_and_capabilities(app: ApiApp) -> None:
    response = call(app, "GET", "/api/v1/health")
    assert response.status == 200
    payload = payload_of(response)
    assert payload["status"] == "ok"
    assert isinstance(payload["server_id"], str) and payload["server_id"]
    assert "health" in payload["capabilities"]
    # server_id 落库后保持不变
    again = call(app, "GET", "/api/v1/health")
    assert payload_of(again)["server_id"] == payload["server_id"]


def test_request_without_authorization_is_rejected(app: ApiApp) -> None:
    response = call(app, "GET", "/api/v1/health", token=None)
    assert response.status == 401
    assert error_code_of(response) == "E_AUTH"


def test_request_with_wrong_token_is_rejected(app: ApiApp) -> None:
    assert call(app, "GET", "/api/v1/health", token="wrong").status == 401


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/v1/nope"),
        ("POST", "/api/v1/health"),
        ("GET", "/health"),
        ("GET", "/api/v2/health"),
    ],
)
def test_unknown_route_returns_404(app: ApiApp, method: str, path: str) -> None:
    response = call(app, method, path)
    assert response.status == 404
    assert error_code_of(response) == "E_NOT_FOUND"


def test_malformed_json_body_returns_400(app: ApiApp) -> None:
    response = call(app, "GET", "/api/v1/health", body=b"{not-json")
    assert response.status == 400
    assert error_code_of(response) == "E_BAD_REQUEST"
