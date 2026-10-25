"""运行端点：列表过滤、详情与叫停（docs/03 端点表与 job_runs 字段表）。

运行数据用 core/jobs 的 JobRunService 直接造，保证与调度器写出的形态一致。
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import RunState
from core.api.app import ApiApp
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

NOW = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
TEMPLATE_ID = "template-1"
DATABASE_NAME = "state.db"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Path]:
    connection = open_database(tmp_path / DATABASE_NAME)
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
    yield tmp_path / DATABASE_NAME


@pytest.fixture
def app(database: Path) -> ApiApp:
    return ApiApp(database, clock=VirtualClock(NOW))


def seed_run(database: Path, *, state: RunState, created_at: datetime) -> str:
    """造一条运行并返回其 id；非 scheduled 状态再推入队列。"""
    connection = open_database(database)
    try:
        service = JobRunService(connection, VirtualClock(created_at))
        record, _created = service.create_scheduled(
            NewRun(
                template_id=TEMPLATE_ID,
                schedule_id=None,
                scheduled_at=created_at,
                created_at=created_at,
                idempotency_key=f"seed:{created_at.isoformat()}:{state.value}",
                priority=0,
                deadline_at=created_at + timedelta(hours=3),
            )
        )
        if state is not RunState.SCHEDULED:
            service.enqueue(record.id)
        return record.id
    finally:
        connection.close()


def call(
    app: ApiApp,
    method: str,
    path: str,
    *,
    body: bytes = b"",
    query: dict[str, str] | None = None,
) -> ApiResponse:
    return app.handle(
        ApiRequest(
            method,
            path,
            {} if query is None else query,
            {"authorization": f"Bearer {TOKEN}"},
            body,
        )
    )


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def error_code_of(response: ApiResponse) -> str:
    error = body_of(response)["error"]
    assert isinstance(error, dict)
    return str(error["code"])


def test_list_runs_filters_state_and_template(app: ApiApp, database: Path) -> None:
    seed_run(database, state=RunState.SCHEDULED, created_at=NOW)
    queued_id = seed_run(
        database,
        state=RunState.QUEUED,
        created_at=NOW + timedelta(minutes=1),
    )
    queued = call(app, "GET", "/api/v1/runs", query={"state": "queued"})
    assert [run["id"] for run in body_of(queued)["runs"]] == [queued_id]
    by_template = call(app, "GET", "/api/v1/runs", query={"template_id": TEMPLATE_ID})
    assert len(body_of(by_template)["runs"]) == 2
    assert call(app, "GET", "/api/v1/runs", query={"limit": "1"}).status == 200
    assert call(app, "GET", "/api/v1/runs", query={"state": "flying"}).status == 400
    assert call(app, "GET", "/api/v1/runs", query={"since": "not-a-time"}).status == 400


def test_run_detail_includes_phases_evidence_verdicts(app: ApiApp, database: Path) -> None:
    run_id = seed_run(database, state=RunState.SCHEDULED, created_at=NOW)
    response = call(app, "GET", f"/api/v1/runs/{run_id}")
    assert response.status == 200
    payload = body_of(response)
    assert payload["run"]["id"] == run_id
    assert payload["run"]["state"] == "scheduled"
    assert payload["run"]["template_id"] == TEMPLATE_ID
    assert payload["phases"] == []
    assert payload["evidence"] == []
    assert payload["verdicts"] == []
    assert call(app, "GET", "/api/v1/runs/nope").status == 404


def test_stop_run_cancels_queued_and_rejects_terminal(app: ApiApp, database: Path) -> None:
    queued_id = seed_run(database, state=RunState.QUEUED, created_at=NOW)
    stopped = call(
        app,
        "POST",
        f"/api/v1/runs/{queued_id}/stop",
        body='{"reason": "改配置"}'.encode(),
    )
    assert stopped.status == 200
    assert body_of(stopped)["run"]["state"] == "cancelled"
    again = call(app, "POST", f"/api/v1/runs/{queued_id}/stop")
    assert again.status == 409
    assert error_code_of(again) == "E_CONFLICT"
    assert call(app, "POST", "/api/v1/runs/nope/stop").status == 404
