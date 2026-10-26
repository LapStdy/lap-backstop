"""通知测试端点：真实投递一次、结构化结果、脱敏与配置即时生效（docs/03）。"""

from __future__ import annotations

import json
import urllib.parse
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from core.logging import JsonLineLogger
from storage import migrate, open_database
from tests.notify_support import ENABLED, NOW, FakeServer

TOKEN = "secret-token"
TEST_PATH = "/api/v1/notifications/test"


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
def log_path(tmp_path: Path) -> Path:
    return tmp_path / "logs" / "core.jsonl"


@pytest.fixture
def app(database: Path, log_path: Path) -> ApiApp:
    return ApiApp(
        database,
        clock=VirtualClock(NOW),
        log=JsonLineLogger(log_path, clock=VirtualClock(NOW)),
    )


def call(
    app: ApiApp,
    method: str,
    path: str,
    *,
    token: str | None = TOKEN,
) -> ApiResponse:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return app.handle(ApiRequest(method=method, path=path, query={}, headers=headers, body=b""))


def payload_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def error_of(response: ApiResponse) -> dict[str, Any]:
    error = payload_of(response)["error"]
    assert isinstance(error, dict)
    return error


def write_settings(database: Path, values: dict[str, Any]) -> None:
    """从独立连接写入 settings；API 每请求现开连接，写后即可见。"""
    connection = open_database(database)
    try:
        put_settings(connection, values, at=NOW)
    finally:
        connection.close()


def notify_event_rows(database: Path) -> list[tuple[str, str]]:
    """两张事件表里 notify 投递簿记的落点清单（表名, 事件标识）。"""
    rows: list[tuple[str, str]] = []
    connection = open_database(database)
    try:
        for row in connection.execute(
            "SELECT type FROM run_events WHERE type IN ('notify.sent', 'notify.failed')"
        ):
            rows.append(("run_events", str(row["type"])))
        for row in connection.execute(
            "SELECT payload FROM events_global WHERE type = 'core.event'"
        ):
            payload = json.loads(str(row["payload"]))
            if payload.get("event") in ("notify.sent", "notify.failed"):
                rows.append(("events_global", str(payload["event"])))
    finally:
        connection.close()
    return rows


def test_success_delivers_once_and_returns_structured_result(
    app: ApiApp, database: Path, fake_server: FakeServer
) -> None:
    write_settings(database, {**ENABLED, "notify.url": fake_server.base_url})
    fake_server.body = json.dumps({"code": 0}).encode("utf-8")

    response = call(app, "POST", TEST_PATH)

    assert response.status == 200
    payload = payload_of(response)
    assert payload["ok"] is True
    assert payload["channel"] == "serverchan"
    assert payload["error_code"] is None
    assert isinstance(payload["elapsed_ms"], int) and payload["elapsed_ms"] >= 0
    assert payload["detail"]
    assert len(fake_server.requests) == 1
    form = urllib.parse.parse_qs(fake_server.requests[0]["body"].decode("utf-8"))
    assert form["title"] == ["测试通知"]


@pytest.mark.parametrize("status", [403, 500])
def test_http_failure_maps_to_enotify_with_actionable_detail(
    app: ApiApp, database: Path, fake_server: FakeServer, status: int
) -> None:
    write_settings(database, {**ENABLED, "notify.url": fake_server.base_url})
    fake_server.status = status

    response = call(app, "POST", TEST_PATH)

    assert response.status == 200
    payload = payload_of(response)
    assert payload["ok"] is False
    assert payload["channel"] == "serverchan"
    assert payload["error_code"] == "E_NOTIFY"
    assert f"HTTP {status}" in str(payload["detail"])


def test_timeout_maps_to_enotify(app: ApiApp, database: Path, fake_server: FakeServer) -> None:
    write_settings(
        database,
        {**ENABLED, "notify.url": fake_server.base_url, "notify.timeout_seconds": 0.3},
    )
    fake_server.delay_seconds = 2.0

    response = call(app, "POST", TEST_PATH)

    assert response.status == 200
    payload = payload_of(response)
    assert payload["ok"] is False
    assert payload["error_code"] == "E_NOTIFY"
    assert "超时" in str(payload["detail"])


def test_disabled_returns_400_with_actionable_error(app: ApiApp, database: Path) -> None:
    write_settings(database, {"notify.enabled": False})

    response = call(app, "POST", TEST_PATH)

    assert response.status == 400
    assert error_of(response)["code"] == "E_CONFIG"
    assert "notify.enabled" in error_of(response)["message"]


def test_missing_credential_returns_400_with_key_name(app: ApiApp, database: Path) -> None:
    write_settings(database, {"notify.enabled": True, "notify.channel": "serverchan"})

    response = call(app, "POST", TEST_PATH)

    assert response.status == 400
    assert error_of(response)["code"] == "E_CONFIG"
    assert "notify.token" in error_of(response)["message"]


def test_unsupported_channel_returns_400(app: ApiApp, database: Path) -> None:
    write_settings(
        database, {"notify.enabled": True, "notify.channel": "feishu", "notify.token": "x"}
    )

    response = call(app, "POST", TEST_PATH)

    assert response.status == 400
    assert error_of(response)["code"] == "E_CONFIG"
    assert "notify.channel" in error_of(response)["message"]


def test_response_and_logs_hide_credentials(
    app: ApiApp, database: Path, fake_server: FakeServer, log_path: Path
) -> None:
    token = "SCT-secret-9x"
    webhook = f"{fake_server.base_url}/robot/send?access_token=ding-secret-88"
    write_settings(database, {**ENABLED, "notify.token": token, "notify.url": fake_server.base_url})
    # 渠道应答原样回显请求里的凭据（误配回显端点/远端排障文案的形态）也要遮掉。
    fake_server.status = 403
    fake_server.body = json.dumps({"echo": token}).encode("utf-8")
    serverchan = call(app, "POST", TEST_PATH)
    write_settings(
        database,
        {
            "notify.enabled": True,
            "notify.channel": "dingtalk",
            "notify.token": "",
            "notify.webhook": webhook,
        },
    )
    fake_server.body = json.dumps({"echo": webhook}).encode("utf-8")
    dingtalk = call(app, "POST", TEST_PATH)

    for response in (serverchan, dingtalk):
        rendered = json.dumps(response.payload, ensure_ascii=False)
        assert token not in rendered
        assert "ding-secret-88" not in rendered
        assert webhook not in rendered
    logs = log_path.read_text(encoding="utf-8")
    assert token not in logs
    assert "ding-secret-88" not in logs
    assert TOKEN not in logs
    # URL 按 docs/08 口径脱敏为域名加前 6 位；密钥值整段遮蔽。
    assert f"{fake_server.base_url}/robot" in str(dingtalk.payload)
    assert "[已脱敏]" in str(serverchan.payload)


def test_endpoint_delivery_writes_no_notify_events(
    app: ApiApp, database: Path, fake_server: FakeServer
) -> None:
    write_settings(database, {**ENABLED, "notify.url": fake_server.base_url})
    fake_server.body = json.dumps({"code": 0}).encode("utf-8")
    assert notify_event_rows(database) == []

    response = call(app, "POST", TEST_PATH)

    assert response.status == 200
    assert notify_event_rows(database) == []


def test_concurrent_calls_each_deliver_once(
    app: ApiApp, database: Path, fake_server: FakeServer
) -> None:
    write_settings(database, {**ENABLED, "notify.url": fake_server.base_url})
    fake_server.body = json.dumps({"code": 0}).encode("utf-8")
    fake_server.delay_seconds = 0.05

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(lambda _: call(app, "POST", TEST_PATH), range(4)))

    assert all(response.status == 200 for response in responses)
    assert all(payload_of(response)["ok"] is True for response in responses)
    assert len(fake_server.requests) == 4


def test_missing_or_wrong_token_is_rejected(
    app: ApiApp, database: Path, fake_server: FakeServer
) -> None:
    write_settings(database, {**ENABLED, "notify.url": fake_server.base_url})

    missing = call(app, "POST", TEST_PATH, token=None)
    wrong = call(app, "POST", TEST_PATH, token="wrong")

    assert missing.status == 401
    assert wrong.status == 401
    assert error_of(wrong)["code"] == "E_AUTH"
    assert fake_server.requests == []


def test_uses_current_settings_without_restart(
    app: ApiApp, database: Path, fake_server: FakeServer
) -> None:
    write_settings(database, {**ENABLED, "notify.url": fake_server.base_url})
    fake_server.body = json.dumps({"code": 0}).encode("utf-8")

    first = call(app, "POST", TEST_PATH)
    write_settings(database, {"notify.enabled": False})
    second = call(app, "POST", TEST_PATH)

    assert first.status == 200 and payload_of(first)["ok"] is True
    assert second.status == 400
    assert error_of(second)["code"] == "E_CONFIG"
    assert len(fake_server.requests) == 1
