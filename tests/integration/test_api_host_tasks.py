"""重新注册计划任务端点：逐任务结果、事件落库与鉴权（docs/03 POST /host/tasks/register）。"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from core.hostops import tasks
from storage import migrate, open_database
from tests.fixtures.subprocess_fakes import FakeRunner, fake_runner

NOW = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
REGISTER_PATH = "/api/v1/host/tasks/register"
# 汇总口径与 serve 启动路径同一构造：清单来自 tasks.REGISTERED_TASKS
SUCCESS_DETAIL = "已注册并写入定义：" + "、".join(tasks.REGISTERED_TASKS)
FAILURE_DETAIL = "注册失败：" + "、".join(tasks.REGISTERED_TASKS)


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
def schtasks() -> tuple[FakeRunner, list[list[str]]]:
    """假 schtasks 执行器与其调用记录；注册动作不真跑。"""
    return fake_runner()


@pytest.fixture
def app(database: Path, schtasks: tuple[FakeRunner, list[list[str]]]) -> ApiApp:
    runner, _calls = schtasks
    return ApiApp(database, clock=VirtualClock(NOW), task_runner=runner)


def call(app: ApiApp, method: str, path: str, *, token: str | None = TOKEN) -> ApiResponse:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return app.handle(ApiRequest(method=method, path=path, query={}, headers=headers, body=b""))


def payload_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def error_of(response: ApiResponse) -> dict[str, Any]:
    error = payload_of(response)["error"]
    assert isinstance(error, dict)
    return error


def host_events(database: Path) -> list[dict[str, Any]]:
    """events_global 里的 host.event 行，按写入顺序。"""
    connection = open_database(database)
    try:
        rows = connection.execute(
            "SELECT type, payload FROM events_global WHERE type = 'host.event' ORDER BY seq"
        ).fetchall()
    finally:
        connection.close()
    return [{"type": str(row["type"]), "payload": json.loads(str(row["payload"]))} for row in rows]


def test_register_returns_per_task_result_and_writes_definitions(
    app: ApiApp, database: Path, schtasks: tuple[FakeRunner, list[list[str]]]
) -> None:
    _runner, calls = schtasks

    response = call(app, "POST", REGISTER_PATH)

    assert response.status == 200
    payload = payload_of(response)
    assert payload["ok"] is True
    assert payload["detail"] == SUCCESS_DETAIL
    assert [task["name"] for task in payload["tasks"]] == list(tasks.REGISTERED_TASKS)
    assert all(task["ok"] and task["detail"] == "已注册" for task in payload["tasks"])
    assert len(calls) == len(tasks.REGISTERED_TASKS)
    # 定义文件落在数据目录，供比对与重建（docs/08 计划任务一节）
    tasks_dir = database.parent / tasks.TASKS_DIRNAME
    assert (tasks_dir / f"{tasks.TASK_CORE}.xml").is_file()
    assert (tasks_dir / f"{tasks.TASK_WATCHDOG}.xml").is_file()
    assert (tasks_dir / f"{tasks.TASK_BACKUP}.xml").is_file()


def test_register_event_matches_startup_shape(app: ApiApp, database: Path) -> None:
    call(app, "POST", REGISTER_PATH)

    events = host_events(database)
    assert len(events) == 1
    assert events[0]["payload"]["event"] == "host.tasks_registered"
    # detail 与 serve 启动路径同一构造
    assert events[0]["payload"]["detail"] == SUCCESS_DETAIL


def test_register_failure_keeps_structured_result_and_event(app: ApiApp, database: Path) -> None:
    runner, _calls = fake_runner(returncode=1)
    failing = ApiApp(database, clock=VirtualClock(NOW), task_runner=runner)

    response = call(failing, "POST", REGISTER_PATH)

    assert response.status == 200
    payload = payload_of(response)
    assert payload["ok"] is False
    assert payload["detail"] == FAILURE_DETAIL
    assert all(task["ok"] is False for task in payload["tasks"])
    assert all(task["detail"] == "schtasks 退出码 1" for task in payload["tasks"])
    events = host_events(database)
    assert len(events) == 1
    assert events[0]["payload"]["detail"] == payload["detail"]


def test_register_timeout_keeps_structured_result_and_event(database: Path) -> None:
    def slow(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd=_command, timeout=10.0)

    timing_out = ApiApp(database, clock=VirtualClock(NOW), task_runner=slow)

    response = call(timing_out, "POST", REGISTER_PATH)

    assert response.status == 200
    payload = payload_of(response)
    assert payload["ok"] is False
    assert payload["detail"] == FAILURE_DETAIL
    assert all(task["detail"] == "schtasks 超时" for task in payload["tasks"])
    events = host_events(database)
    assert len(events) == 1
    assert events[0]["payload"]["detail"] == payload["detail"]


def test_register_requires_token(app: ApiApp) -> None:
    missing = call(app, "POST", REGISTER_PATH, token=None)
    wrong = call(app, "POST", REGISTER_PATH, token="wrong")

    assert missing.status == 401
    assert wrong.status == 401
    assert error_of(wrong)["code"] == "E_AUTH"
