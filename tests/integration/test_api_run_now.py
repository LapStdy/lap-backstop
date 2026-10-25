"""手动触发：必须带 Idempotency-Key，同一键只建一个运行（docs/03 request_idempotency）。"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
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
    application = ApiApp(database, clock=VirtualClock(NOW))
    created = application.handle(
        ApiRequest(
            "POST",
            "/api/v1/schedules",
            {},
            {"authorization": f"Bearer {TOKEN}"},
            json.dumps(
                {
                    "name": "每日",
                    "kind": "cron",
                    "expr": "0 4 * * *",
                    "job_template_id": TEMPLATE_ID,
                }
            ).encode("utf-8"),
        )
    )
    assert isinstance(created.payload, dict)
    schedule = created.payload["schedule"]
    assert isinstance(schedule, dict)
    yield application


def trigger(app: ApiApp, schedule_id: str, key: str | None) -> ApiResponse:
    headers = {"authorization": f"Bearer {TOKEN}"}
    if key is not None:
        headers["idempotency-key"] = key
    return app.handle(ApiRequest("POST", f"/api/v1/schedules/{schedule_id}/run", {}, headers, b""))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def schedule_id_of(app: ApiApp) -> str:
    listing = app.handle(
        ApiRequest("GET", "/api/v1/schedules", {}, {"authorization": f"Bearer {TOKEN}"}, b"")
    )
    schedules = body_of(listing)["schedules"]
    return str(schedules[0]["id"])


def test_run_now_requires_idempotency_key(app: ApiApp) -> None:
    response = trigger(app, schedule_id_of(app), key=None)
    assert response.status == 400
    error = body_of(response)["error"]
    assert isinstance(error, dict)
    assert error["code"] == "E_BAD_REQUEST"


def test_run_now_unknown_schedule_returns_404(app: ApiApp) -> None:
    assert trigger(app, "nope", key="k").status == 404


def test_run_now_same_key_returns_same_run(app: ApiApp) -> None:
    schedule_id = schedule_id_of(app)
    first = trigger(app, schedule_id, key="trigger-1")
    assert first.status == 201
    run = body_of(first)["run"]
    assert run["state"] == "scheduled"
    assert run["schedule_id"] is None  # docs/03：手动运行的 schedule_id 为空
    assert run["template_id"] == TEMPLATE_ID
    assert run["idempotency_key"] == "manual:trigger-1"
    assert run["deadline_at"] == (NOW + timedelta(hours=3)).isoformat()
    assert run["detail"] == {"trigger": "manual"}
    # 同一 key 重放：200 且是同一运行；不同 key 建新运行
    replay = trigger(app, schedule_id, key="trigger-1")
    assert replay.status == 200
    assert body_of(replay)["run"]["id"] == run["id"]
    other = trigger(app, schedule_id, key="trigger-2")
    assert other.status == 201
    assert body_of(other)["run"]["id"] != run["id"]
