"""status 快照与环境/插件只读端点（docs/03 /status、/environments、/plugins）。"""

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
from core.jobs.run_service import JobRunService
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

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
        connection.execute(
            "INSERT INTO environments (id, provider_id, name, state, last_probe_at, detail) "
            "VALUES ('env-1', 'fake', 'MuMu', 'ready', ?, '{\"dpi\": 240}')",
            (NOW.isoformat(),),
        )
        connection.execute(
            "INSERT INTO plugins (id, name, version, api_version, capabilities, "
            "config_schema, error_map, requirements, enabled, installed_at) "
            "VALUES ('fake', '假插件', '1.0.0', 1, '[\"script\"]', '{\"type\": \"object\"}', "
            "'{}', '{}', 1, ?)",
            (NOW.isoformat(),),
        )
        service = JobRunService(connection, VirtualClock(NOW))
        record, _ = service.create_scheduled(
            NewRun(
                template_id=TEMPLATE_ID,
                schedule_id=None,
                scheduled_at=NOW,
                created_at=NOW,
                idempotency_key="status-seed",
                priority=0,
                deadline_at=None,
            )
        )
        service.enqueue(record.id)
        connection.execute(
            "INSERT INTO verdicts (id, run_id, status, confidence, reason, policy_id, "
            "checks, evidence_ids, created_at) VALUES ('v1', ?, 'completed', 0.98, "
            "'任务完成', 'policy-1', '[]', '[]', ?)",
            (record.id, NOW.isoformat()),
        )
    finally:
        connection.close()
    yield ApiApp(database, clock=VirtualClock(NOW))


def call(app: ApiApp, path: str) -> ApiResponse:
    return app.handle(ApiRequest("GET", path, {}, {"authorization": f"Bearer {TOKEN}"}, b""))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def test_status_snapshot_covers_runs_queue_envs_verdicts(app: ApiApp) -> None:
    payload = body_of(call(app, "/api/v1/status"))
    assert [run["state"] for run in payload["active_runs"]] == ["queued"]
    assert len(payload["queue"]) == 1
    assert payload["environments"][0]["id"] == "env-1"
    assert payload["environments"][0]["state"] == "ready"
    assert payload["recent_verdicts"][0]["status"] == "completed"
    assert payload["recent_verdicts"][0]["confidence"] == 0.98


def test_environment_detail_and_404(app: ApiApp) -> None:
    detail = body_of(call(app, "/api/v1/environments/env-1"))
    assert detail["environment"]["name"] == "MuMu"
    assert detail["environment"]["detail"] == {"dpi": 240}
    assert call(app, "/api/v1/environments/nope").status == 404
    listing = body_of(call(app, "/api/v1/environments"))
    assert [env["id"] for env in listing["environments"]] == ["env-1"]


def test_plugin_detail_and_404(app: ApiApp) -> None:
    detail = body_of(call(app, "/api/v1/plugins/fake"))
    plugin = detail["plugin"]
    assert plugin["capabilities"] == ["script"]
    assert plugin["config_schema"] == {"type": "object"}
    assert plugin["api_version"] == 1
    assert plugin["enabled"] is True
    assert call(app, "/api/v1/plugins/nope").status == 404
    listing = body_of(call(app, "/api/v1/plugins"))
    assert [item["id"] for item in listing["plugins"]] == ["fake"]
