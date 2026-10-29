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
from core.plugins.registry import PluginRegistry
from core.supervisor import Supervisor
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

NOW = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
TEMPLATE_ID = "template-1"
DATABASE_NAME = "state.db"
_STABLE_DISK_FREE = 100 * 1024**3


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
def app(database: Path, tmp_path: Path) -> Iterator[ApiApp]:
    """叫停端点经注入的 Supervisor 走真实实现：请求线程不碰它的 tick 连接。"""
    connection = open_database(database)
    clock = VirtualClock(NOW)
    try:
        yield ApiApp(
            database,
            clock=clock,
            run_stopper=Supervisor(
                connection,
                clock,
                PluginRegistry(),
                data_root=tmp_path,
                disk_free=lambda: _STABLE_DISK_FREE,
            ),
        )
    finally:
        connection.close()


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


def test_stop_without_injected_stopper_returns_conflict(database: Path) -> None:
    """未注入运行动作时拒绝叫停：端点是执行器的入口，不绕过它直接改库。"""
    queued_id = seed_run(database, state=RunState.QUEUED, created_at=NOW)
    response = call(
        ApiApp(database, clock=VirtualClock(NOW)),
        "POST",
        f"/api/v1/runs/{queued_id}/stop",
    )
    assert response.status == 409
    assert error_code_of(response) == "E_CONFLICT"
    connection = open_database(database)
    try:
        events = JobRunService(connection, VirtualClock(NOW)).events(queued_id)
    finally:
        connection.close()
    assert "run.interrupted" not in [event.type for event in events]


def test_stop_rejects_shutdown_as_cancel_reason(app: ApiApp, database: Path) -> None:
    """shutdown 是停机收尾的保留值：作为取消原因一律拒绝，避免与停机事件混淆。"""
    queued_id = seed_run(database, state=RunState.QUEUED, created_at=NOW)
    response = call(
        app,
        "POST",
        f"/api/v1/runs/{queued_id}/stop",
        body=b'{"reason": "shutdown"}',
    )
    assert response.status == 400
    assert error_code_of(response) == "E_BAD_REQUEST"
