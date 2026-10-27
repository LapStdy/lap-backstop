"""API 端到端冒烟：列表端点、证据缺失与环境动作在真实 HTTP 传输下的装配。

SSE 帧格式与证据文件下载的真 HTTP 用例分别在 test_api_events.py 与
test_api_evidence.py，本文件只补其余端点的传输层冒烟。

运行与证据数据用 core 层服务直接造，保证与执行器写出的形态一致。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.api.server import ApiOptions, ApiServer, start_api_server
from core.api.store import TOKEN_KEY, put_settings
from core.clock import VirtualClock
from core.environments.actions import EnvironmentActions
from storage import migrate, open_database
from storage.repositories.environments import EnvironmentRepository
from tests.integration.test_api_environment_actions import RecordingActions
from tests.integration.test_api_evidence import record_evidence, seed_run
from tests.notify_support import insert_template

NOW = datetime(2026, 10, 27, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
ENV_ID = "env-1"
DATABASE_NAME = "state.db"
# 宿主的 http_proxy 只让行 127.0.0.1；测试只打本机监听，一律绕开代理，
# 避免环境变量决定测试成败。
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _start_server(
    database: Path,
    *,
    environment_actions: EnvironmentActions | None = None,
) -> ApiServer:
    """起一份真 HTTP 服务：port=0 随机端口，库含 token、模板与环境行。"""
    connection = open_database(database)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        insert_template(connection)
        EnvironmentRepository(connection).ensure(ENV_ID, provider_id="fake", name="MuMu")
        return start_api_server(
            connection,
            ApiOptions(
                host="127.0.0.1",
                port=0,
                clock=VirtualClock(NOW),
                environment_actions=environment_actions,
            ),
        )
    finally:
        connection.close()


@pytest.fixture
def server(tmp_path: Path) -> Iterator[ApiServer]:
    handle = _start_server(tmp_path / DATABASE_NAME)
    try:
        yield handle
    finally:
        handle.shutdown()


def request(
    handle: ApiServer,
    method: str,
    path: str,
) -> tuple[int, Any, dict[str, str]]:
    """发一个带 token 的请求，返回（状态码、JSON 载荷、小写响应头）。"""
    host, port = handle.address
    req = urllib.request.Request(
        f"http://{host}:{port}{path}",
        method=method,
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    try:
        with _OPENER.open(req, timeout=10) as response:
            return (
                response.status,
                json.loads(response.read().decode("utf-8")),
                {k.lower(): v for k, v in response.headers.items()},
            )
    except urllib.error.HTTPError as error:
        payload = json.loads(error.read().decode("utf-8"))
        return error.code, payload, {k.lower(): v for k, v in error.headers.items()}


def test_list_endpoints_on_fresh_database_return_empty_arrays(server: ApiServer) -> None:
    """空库下列表端点走真 HTTP 应为 200 与空数组，而不是 404 或 500。"""
    for path, key in (
        ("/api/v1/notifications", "notifications"),
        ("/api/v1/resources", "resources"),
        ("/api/v1/resources/leases", "leases"),
    ):
        status, payload, _headers = request(server, "GET", path)
        assert status == 200, path
        assert payload == {key: []}, path


def test_missing_evidence_record_returns_404_over_http(server: ApiServer, tmp_path: Path) -> None:
    """运行在而证据 id 不存在：路由与联合查找装配后的应答是 404 与 E_NOT_FOUND。"""
    database = tmp_path / DATABASE_NAME
    run_id = seed_run(database, "smoke:evidence")
    record_evidence(database, run_id, "shot.log", b"present")
    status, payload, _headers = request(server, "GET", f"/api/v1/runs/{run_id}/evidence/{'0' * 32}")
    assert status == 404
    assert payload["error"]["code"] == "E_NOT_FOUND"


def test_environment_prepare_with_injected_actions_returns_ok_over_http(
    tmp_path: Path,
) -> None:
    """注入的假动作要穿过真 HTTP 到达端点，应答带 ok 与动作后的环境行。"""
    actions = RecordingActions()
    handle = _start_server(tmp_path / "actions.db", environment_actions=actions)
    try:
        status, payload, _headers = request(
            handle, "POST", f"/api/v1/environments/{ENV_ID}/prepare"
        )
    finally:
        handle.shutdown()
    assert status == 200
    assert payload["ok"] is True
    assert payload["error_code"] is None
    assert payload["environment"]["id"] == ENV_ID
    assert actions.calls == [ENV_ID]


def test_environment_prepare_without_actions_returns_conflict_over_http(
    server: ApiServer,
) -> None:
    """动作未接入内核时，端点在真 HTTP 下按 409 与 E_CONFLICT 应答。"""
    status, payload, _headers = request(server, "POST", f"/api/v1/environments/{ENV_ID}/prepare")
    assert status == 409
    assert payload["error"]["code"] == "E_CONFLICT"
