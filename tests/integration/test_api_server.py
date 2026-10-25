"""独立 HTTP 服务：传输层行为与 settings 读写（docs/03 端点表）。"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.api.server import MAX_BODY_BYTES, ApiOptions, ApiServer, start_api_server
from core.api.store import TOKEN_KEY, put_settings
from core.clock import VirtualClock
from storage import migrate, open_database

NOW = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"


@pytest.fixture
def server(tmp_path: Path) -> Iterator[ApiServer]:
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        handle = start_api_server(
            connection,
            ApiOptions(host="127.0.0.1", port=0, clock=VirtualClock(NOW)),
        )
    finally:
        connection.close()
    yield handle
    handle.shutdown()


def request(
    handle: ApiServer,
    method: str,
    path: str,
    *,
    body: bytes | None = None,
) -> tuple[int, Any, dict[str, str]]:
    host, port = handle.address
    req = urllib.request.Request(
        f"http://{host}:{port}{path}",
        data=body,
        method=method,
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return (
                response.status,
                json.loads(response.read().decode("utf-8")),
                {k.lower(): v for k, v in response.headers.items()},
            )
    except urllib.error.HTTPError as error:
        payload = json.loads(error.read().decode("utf-8"))
        return error.code, payload, {k.lower(): v for k, v in error.headers.items()}


def test_health_over_http(server: ApiServer) -> None:
    status, payload, _ = request(server, "GET", "/api/v1/health")
    assert status == 200
    assert payload["status"] == "ok"
    assert "health" in payload["capabilities"]


def test_unconfigured_token_rejects_http_requests(tmp_path: Path) -> None:
    connection = open_database(tmp_path / "no-token.db")
    try:
        migrate(connection)
        handle = start_api_server(
            connection,
            ApiOptions(host="127.0.0.1", port=0, clock=VirtualClock(NOW)),
        )
    finally:
        connection.close()
    try:
        status, payload, _ = request(handle, "GET", "/api/v1/health")
        assert status == 401
        assert payload["error"]["code"] == "E_AUTH"
    finally:
        handle.shutdown()


def test_settings_roundtrip_after_write(server: ApiServer) -> None:
    body = json.dumps({"notify.channel": "serverchan", "ui.timezone": "Asia/Shanghai"}).encode()
    status, payload, _ = request(server, "PUT", "/api/v1/settings", body=body)
    assert status == 200
    assert payload["settings"]["notify.channel"] == "serverchan"
    # 写后读一致：GET 读到同一批值
    _, again, _ = request(server, "GET", "/api/v1/settings")
    assert again["settings"]["ui.timezone"] == "Asia/Shanghai"


def test_settings_overwrite_existing_key(server: ApiServer) -> None:
    request(server, "PUT", "/api/v1/settings", body=b'{"notify.channel": "dingtalk"}')
    request(server, "PUT", "/api/v1/settings", body=b'{"notify.channel": "ntfy"}')
    _, payload, _ = request(server, "GET", "/api/v1/settings")
    assert payload["settings"]["notify.channel"] == "ntfy"


def test_settings_put_rejects_non_object_body(server: ApiServer) -> None:
    status, payload, _ = request(server, "PUT", "/api/v1/settings", body=b"[1, 2]")
    assert status == 400
    assert payload["error"]["code"] == "E_BAD_REQUEST"


def test_oversized_body_rejected(server: ApiServer) -> None:
    body = b"x" * (MAX_BODY_BYTES + 1)
    status, payload, _ = request(server, "PUT", "/api/v1/settings", body=body)
    assert status == 400
    assert payload["error"]["code"] == "E_BAD_REQUEST"


def test_malformed_content_length_rejected(server: ApiServer) -> None:
    """Content-Length 非整数要得到 400 JSON，而不是连接被掐断。"""
    host, port = server.address
    # urllib 会重写 Content-Length，这里直接用裸套接字发原始头
    with socket.create_connection((host, port), timeout=10) as sock:
        raw = (
            f"PUT /api/v1/settings HTTP/1.0\r\n"
            f"Host: {host}\r\n"
            f"Authorization: Bearer {TOKEN}\r\n"
            f"Content-Length: abc\r\n\r\n"
        ).encode()
        sock.sendall(raw)
        status_line = sock.recv(1024).decode("utf-8", errors="replace")
    assert status_line.startswith("HTTP/1.0 400"), status_line


def test_server_stops_after_request_limit(tmp_path: Path) -> None:
    connection = open_database(tmp_path / "limited.db")
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        handle = start_api_server(
            connection,
            ApiOptions(host="127.0.0.1", port=0, clock=VirtualClock(NOW), max_requests=1),
        )
    finally:
        connection.close()
    status, _, _ = request(handle, "GET", "/api/v1/health")
    assert status == 200
    # 服务线程在处理完第 1 个请求后自行退出；等它退出后再断言拒绝连接
    handle.wait_stopped(timeout=5.0)
    with pytest.raises(urllib.error.URLError):
        request(handle, "GET", "/api/v1/health")
    handle.shutdown()
