"""环境动作端点：手动预热与复位（docs/03 POST /environments/{id}/prepare、/reset）。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from contracts import EnvironmentState
from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from core.environments.actions import EnvironmentActionResult, EnvironmentActions
from core.environments.errors import EnvironmentNotFoundError
from core.hostops.environment_actions import EnvironmentActionService
from core.plugins.errors import PluginError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from plugins.fake.plugin import FakeEnvironmentProvider
from storage import migrate, open_database
from storage.repositories.environments import EnvironmentRepository

NOW = datetime(2026, 10, 27, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
ENV_ID = "env-1"


class RecordingActions:
    """记录收到的环境 id 并按预设返回动作结果；只做转发与形状验证。"""

    def __init__(self, *, result: EnvironmentActionResult | None = None) -> None:
        self.calls: list[str] = []
        self._result = (
            result
            if result is not None
            else EnvironmentActionResult(ok=True, detail="fake 动作完成")
        )

    def prepare(self, environment_id: str) -> EnvironmentActionResult:
        self.calls.append(environment_id)
        return self._result

    def reset(self, environment_id: str) -> EnvironmentActionResult:
        self.calls.append(environment_id)
        return self._result


def _database(tmp_path: Path) -> Path:
    return tmp_path / "state.db"


def _app(database: Path, actions: EnvironmentActions | None) -> ApiApp:
    return ApiApp(database, clock=VirtualClock(NOW), environment_actions=actions)


@pytest.fixture
def seeded(tmp_path: Path) -> Iterator[tuple[Path, sqlite3.Connection]]:
    """带 token 与一个 ready 环境的库；连接保持打开供断言环境行。"""
    database = _database(tmp_path)
    connection = open_database(database)
    try:
        migrate(connection)
        put_settings(connection, {TOKEN_KEY: TOKEN}, at=NOW)
        EnvironmentRepository(connection).ensure(ENV_ID, provider_id="fake", name="MuMu")
        yield database, connection
    finally:
        connection.close()


def call(app: ApiApp, method: str, path: str, *, token: str | None = TOKEN) -> ApiResponse:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return app.handle(ApiRequest(method, path, {}, headers, b""))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def test_prepare_success_returns_latest_environment_row(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    database, _connection = seeded
    actions = RecordingActions()
    app = _app(database, actions)
    response = call(app, "POST", f"/api/v1/environments/{ENV_ID}/prepare")
    assert response.status == 200
    assert actions.calls == [ENV_ID]
    payload = body_of(response)
    assert payload["ok"] is True
    assert payload["error_code"] is None
    assert payload["detail"] == "fake 动作完成"
    # environment 字段与 GET /environments/{id} 必须是同一映射。
    assert (
        payload["environment"]
        == body_of(call(app, "GET", f"/api/v1/environments/{ENV_ID}"))["environment"]
    )


def test_reset_success_returns_latest_environment_row(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    database, _connection = seeded
    actions = RecordingActions()
    response = call(_app(database, actions), "POST", f"/api/v1/environments/{ENV_ID}/reset")
    assert response.status == 200
    assert actions.calls == [ENV_ID]
    assert body_of(response)["ok"] is True


def test_plugin_failure_still_returns_200_with_error_passthrough(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    database, _connection = seeded
    actions = RecordingActions(
        result=EnvironmentActionResult(
            ok=False, error_code="E_ENV_WAKE_TIMEOUT", detail="唤醒超上限"
        )
    )
    response = call(_app(database, actions), "POST", f"/api/v1/environments/{ENV_ID}/prepare")
    assert response.status == 200
    payload = body_of(response)
    assert payload["ok"] is False
    assert payload["error_code"] == "E_ENV_WAKE_TIMEOUT"
    assert payload["detail"] == "唤醒超上限"
    assert payload["environment"]["id"] == ENV_ID


def test_route_parameter_passes_through_hyphens_and_underscores(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    database, _connection = seeded
    actions = RecordingActions()
    app = _app(database, actions)
    call(app, "POST", "/api/v1/environments/mumu-01/reset")
    call(app, "POST", "/api/v1/environments/env_1/prepare")
    assert actions.calls == ["mumu-01", "env_1"]


def test_actions_missing_returns_conflict(seeded: tuple[Path, sqlite3.Connection]) -> None:
    database, _connection = seeded
    response = call(_app(database, None), "POST", f"/api/v1/environments/{ENV_ID}/prepare")
    assert response.status == 409
    assert body_of(response)["error"]["code"] == "E_CONFLICT"


def test_unknown_environment_returns_not_found(seeded: tuple[Path, sqlite3.Connection]) -> None:
    database, _connection = seeded

    class MissingActions(RecordingActions):
        def reset(self, environment_id: str) -> EnvironmentActionResult:
            raise EnvironmentNotFoundError(f"环境不存在：{environment_id}")

    response = call(_app(database, MissingActions()), "POST", "/api/v1/environments/nope/reset")
    assert response.status == 404
    assert body_of(response)["error"]["code"] == "E_NOT_FOUND"


def test_busy_prepare_via_real_service_returns_conflict(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    """真实 EnvironmentService 链路：busy 环境的 prepare 拿 409，provider 不被调到。"""
    database, connection = seeded
    EnvironmentRepository(connection).set_state(
        ENV_ID, EnvironmentState.BUSY, at=NOW, detail={"action": "test"}
    )
    response = call(
        _app(database, _actions(database)),
        "POST",
        f"/api/v1/environments/{ENV_ID}/prepare",
    )
    assert response.status == 409
    assert body_of(response)["error"]["message"] == "环境执行中，不能准备"


def test_unknown_environment_via_real_service_returns_not_found(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    """真实链路：环境不存在是 404 E_NOT_FOUND，不是 500。"""
    database, _connection = seeded
    response = call(_app(database, _actions(database)), "POST", "/api/v1/environments/nope/prepare")
    assert response.status == 404
    assert body_of(response)["error"]["code"] == "E_NOT_FOUND"


def test_missing_provider_via_real_service_returns_conflict(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    """真实链路：provider 未注册是 409 E_CONFLICT，message 指出插件 id。"""
    database, _connection = seeded
    response = call(
        _app(database, _actions(database, with_provider=False)),
        "POST",
        f"/api/v1/environments/{ENV_ID}/prepare",
    )
    assert response.status == 409
    payload = body_of(response)
    assert payload["error"]["code"] == "E_CONFLICT"
    assert "插件未注册：fake" in payload["error"]["message"]


def test_plugin_error_returns_conflict(seeded: tuple[Path, sqlite3.Connection]) -> None:
    """插件领域的其它失败同样按 provider 不允许映射 409，不泄漏成 500。"""

    class BrokenActions(RecordingActions):
        def prepare(self, environment_id: str) -> EnvironmentActionResult:
            raise PluginError("插件加载失败")

        def reset(self, environment_id: str) -> EnvironmentActionResult:
            raise PluginError("插件加载失败")

    database, _connection = seeded
    response = call(
        _app(database, BrokenActions()), "POST", f"/api/v1/environments/{ENV_ID}/prepare"
    )
    assert response.status == 409
    assert body_of(response)["error"]["message"] == "插件加载失败"


def test_reset_via_real_service_updates_environment_row(
    seeded: tuple[Path, sqlite3.Connection],
) -> None:
    """真实链路：reset 成功后环境行是动作后的最新状态，仍是 booting 等探针。"""
    database, _connection = seeded
    response = call(
        _app(database, _actions(database)),
        "POST",
        f"/api/v1/environments/{ENV_ID}/reset",
    )
    assert response.status == 200
    payload = body_of(response)
    assert payload["ok"] is True
    assert payload["detail"] == "fake 复位完成"
    assert payload["environment"]["state"] == "booting"


def test_prepare_without_token_is_rejected(seeded: tuple[Path, sqlite3.Connection]) -> None:
    database, _connection = seeded
    response = call(
        _app(database, RecordingActions()),
        "POST",
        f"/api/v1/environments/{ENV_ID}/prepare",
        token=None,
    )
    assert response.status == 401


def test_reset_without_token_is_rejected(seeded: tuple[Path, sqlite3.Connection]) -> None:
    database, _connection = seeded
    response = call(
        _app(database, RecordingActions()),
        "POST",
        f"/api/v1/environments/{ENV_ID}/reset",
        token=None,
    )
    assert response.status == 401


def test_wrong_token_is_rejected(seeded: tuple[Path, sqlite3.Connection]) -> None:
    database, _connection = seeded
    app = _app(database, RecordingActions())
    assert call(app, "POST", f"/api/v1/environments/{ENV_ID}/prepare", token="wrong").status == 401
    assert call(app, "POST", f"/api/v1/environments/{ENV_ID}/reset", token="wrong").status == 401


def _actions(database: Path, *, with_provider: bool = True) -> EnvironmentActionService:
    registry = PluginRegistry()
    if with_provider:
        registry.register(
            PluginCapabilities(plugin_id="fake", environment=FakeEnvironmentProvider())
        )
    return EnvironmentActionService(database, VirtualClock(NOW), registry)
