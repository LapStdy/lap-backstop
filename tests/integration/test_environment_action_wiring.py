"""环境动作的线程纪律：动作在调用线程自开连接，不碰 tick 线程的连接。

serve 的 supervisor 连接归 tick 线程，HTTP 请求在各自线程里跑；跨线程复用
supervisor 连接会让 sqlite3 抛 ProgrammingError 并落到 500。本文件把这条
不变量与真 HTTP 下的成功路径钉住。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from contracts import EnvironmentState
from core.api.server import ApiOptions, ApiServer, start_api_server
from core.api.store import TOKEN_KEY, put_settings
from core.clock import VirtualClock
from core.environments.actions import EnvironmentActionResult
from core.hostops.environment_actions import EnvironmentActionService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from plugins.fake.plugin import FakeEnvironmentProvider
from storage import migrate, open_database
from storage.repositories.environments import EnvironmentRepository

NOW = datetime(2026, 10, 27, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
ENV_ID = "env-1"
DATABASE_NAME = "state.db"
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _seed(database: Path) -> None:
    connection = open_database(database)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        EnvironmentRepository(connection).ensure(ENV_ID, provider_id="fake", name="MuMu")
    finally:
        connection.close()


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / DATABASE_NAME
    _seed(path)
    return path


def _actions(database: Path) -> EnvironmentActionService:
    registry = PluginRegistry()
    registry.register(PluginCapabilities(plugin_id="fake", environment=FakeEnvironmentProvider()))
    return EnvironmentActionService(database, VirtualClock(NOW), registry)


def test_prepare_from_request_thread_succeeds_while_tick_connection_is_open(
    database: Path,
) -> None:
    """tick 线程的连接保持打开时，动作在另一个线程执行并落库，不触发线程错误。"""
    serve_connection = open_database(database)
    try:
        outcome: dict[str, EnvironmentActionResult] = {}

        def run() -> None:
            outcome["result"] = _actions(database).prepare(ENV_ID)

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert outcome["result"].ok is True
        stored = EnvironmentRepository(serve_connection).get(ENV_ID)
        assert stored is not None
        assert stored.state is EnvironmentState.READY
    finally:
        serve_connection.close()


def test_prepare_over_http_returns_ready_row_and_state_event(database: Path) -> None:
    """真 HTTP 请求线程里动作成功：200、环境 ready、事件落库，而不是 500 E_STORAGE。"""
    bootstrap = open_database(database)
    try:
        handle: ApiServer = start_api_server(
            bootstrap,
            ApiOptions(
                host="127.0.0.1",
                port=0,
                clock=VirtualClock(NOW),
                environment_actions=_actions(database),
            ),
        )
    finally:
        bootstrap.close()
    try:
        status, payload = _post(handle, f"/api/v1/environments/{ENV_ID}/prepare")
    finally:
        handle.shutdown()
    assert status == 200
    assert payload["ok"] is True
    assert payload["environment"]["state"] == "ready"
    assert _state_changes(database) == [("unknown", "booting"), ("booting", "ready")]


def _post(handle: ApiServer, path: str) -> tuple[int, Any]:
    host, port = handle.address
    request = urllib.request.Request(
        f"http://{host}:{port}{path}",
        method="POST",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    try:
        with _OPENER.open(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def _state_changes(database: Path) -> list[tuple[str, str]]:
    reader = sqlite3.connect(database)
    try:
        rows = reader.execute(
            "SELECT payload FROM events_global WHERE type = ? ORDER BY seq",
            ("env.state.changed",),
        ).fetchall()
    finally:
        reader.close()
    return [
        (str(payload["from_state"]), str(payload["to_state"]))
        for payload in (json.loads(str(row[0])) for row in rows)
    ]
