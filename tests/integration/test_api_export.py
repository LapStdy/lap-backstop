"""导出端点：zip 条目、密钥不入包、文件名与错误口径（docs/03 GET /export）。"""

from __future__ import annotations

import io
import json
import re
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime
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
NOTIFY_TOKEN = "SCT-secret-9x"
NOTIFY_URL = "https://ntfy.sh/topic-secret"
EXPORT_PATH = "/api/v1/export"
FILENAME_PATTERN = r"lap-backstop-export-\d{8}-\d{6}\.zip"
ENTRY_NAMES = frozenset(
    {"settings.json", "plugins.json", "job-templates.json", "schedules.json", "runs.json"}
)
RUN_SUMMARY_FIELDS = frozenset(
    {
        "id",
        "template_id",
        "schedule_id",
        "scheduled_at",
        "created_at",
        "state",
        "started_at",
        "finished_at",
        "verdict_id",
        "error_code",
    }
)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Path]:
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        put_settings(
            connection,
            {
                TOKEN_KEY: TOKEN,
                "notify.token": NOTIFY_TOKEN,
                "notify.url": NOTIFY_URL,
                "api.host": "127.0.0.1",
            },
            at=NOW,
        )
        connection.execute(
            "INSERT INTO plugins (id, name, version, api_version, capabilities, "
            "config_schema, error_map, requirements, enabled, installed_at) VALUES "
            "('maa', 'MAA', '6.20.1', 1, '[]', '{}', '{}', '{}', 1, ?)",
            (NOW.isoformat(),),
        )
        connection.execute(
            "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
            "config_schema_version, verification_policy, recovery_policy, enabled, "
            "created_at, updated_at) VALUES (?, '日常', 'maa', 'env-1', '1', '{}', "
            "'{}', 1, ?, ?)",
            ("template-1", NOW.isoformat(), NOW.isoformat()),
        )
        connection.execute(
            "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, "
            "window_minutes, coalesce, priority, deadline_minutes, preheat_minutes, "
            "keep_warm_minutes, job_template_id, enabled, next_run_at, created_at, "
            "updated_at) VALUES (?, '每晨', 'cron', '0 8 * * *', NULL, "
            "'Asia/Shanghai', 'run_once', 10, 1, 0, 60, 15, 0, 'template-1', 1, "
            "NULL, ?, ?)",
            ("schedule-1", NOW.isoformat(), NOW.isoformat()),
        )
        connection.execute(
            "INSERT INTO job_runs (id, template_id, schedule_id, scheduled_at, "
            "created_at, idempotency_key, state, attempt, priority, deadline_at, "
            "started_at, finished_at, verdict_id, error_code, detail) VALUES "
            "('run-1', 'template-1', 'schedule-1', ?, ?, 'seed-export-1', "
            "'succeeded', 1, 0, NULL, ?, ?, NULL, NULL, '{}')",
            (
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    finally:
        connection.close()
    yield tmp_path / "state.db"


@pytest.fixture
def app(database: Path) -> ApiApp:
    return ApiApp(database, clock=VirtualClock(NOW))


def call(app: ApiApp, method: str, path: str, *, token: str | None = TOKEN) -> ApiResponse:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return app.handle(ApiRequest(method=method, path=path, query={}, headers=headers, body=b""))


def entries_of(response: ApiResponse) -> dict[str, Any]:
    """按条目名解开导出 zip 的 JSON 载荷。"""
    assert response.binary is not None
    with zipfile.ZipFile(io.BytesIO(response.binary.data)) as archive:
        assert set(archive.namelist()) == set(ENTRY_NAMES)
        return {name: json.loads(archive.read(name).decode("utf-8")) for name in archive.namelist()}


def error_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    error = response.payload["error"]
    assert isinstance(error, dict)
    return error


def test_export_returns_zip_with_documented_entries(app: ApiApp) -> None:
    response = call(app, "GET", EXPORT_PATH)

    assert response.status == 200
    assert response.binary is not None
    assert response.binary.content_type == "application/zip"
    assert response.binary.data.startswith(b"PK")
    filename = response.binary.filename
    assert filename is not None
    assert re.fullmatch(FILENAME_PATTERN, filename)


def test_export_entries_carry_seed_content(app: ApiApp) -> None:
    entries = entries_of(call(app, "GET", EXPORT_PATH))

    # settings 条目与 JSON 文件导出同一收集器：value 是落库 JSON 原文
    assert entries["settings.json"]["values"] == {"api.host": '"127.0.0.1"'}
    assert entries["plugins.json"]["plugins"] == [
        {"id": "maa", "version": "6.20.1", "enabled": True}
    ]
    assert [row["id"] for row in entries["job-templates.json"]["job_templates"]] == ["template-1"]
    assert [row["id"] for row in entries["schedules.json"]["schedules"]] == ["schedule-1"]
    runs = entries["runs.json"]["runs"]
    assert len(runs) == 1
    assert runs[0]["id"] == "run-1"
    assert runs[0]["state"] == "succeeded"
    # 摘要条目只带运行行字段，不带事件流
    assert set(runs[0]) == RUN_SUMMARY_FIELDS


def test_export_hides_token_and_credentials(app: ApiApp) -> None:
    response = call(app, "GET", EXPORT_PATH)

    entries = entries_of(response)
    assert TOKEN_KEY not in entries["settings.json"]["values"]
    assert "notify.token" not in entries["settings.json"]["values"]
    # ntfy 主题地址等同凭据，一并过滤
    assert "notify.url" not in entries["settings.json"]["values"]
    assert response.binary is not None
    assert TOKEN.encode("utf-8") not in response.binary.data
    assert NOTIFY_TOKEN.encode("utf-8") not in response.binary.data
    assert NOTIFY_URL.encode("utf-8") not in response.binary.data


def test_export_twice_returns_valid_zips(app: ApiApp) -> None:
    first = call(app, "GET", EXPORT_PATH)
    second = call(app, "GET", EXPORT_PATH)

    assert first.status == 200 and second.status == 200
    # 内存组装不落盘，同刻两次调用互不冲突，各自成包
    assert set(entries_of(first)) == set(entries_of(second)) == set(ENTRY_NAMES)
    assert first.binary is not None and second.binary is not None
    assert first.binary.data == second.binary.data


def test_export_requires_token(app: ApiApp) -> None:
    missing = call(app, "GET", EXPORT_PATH, token=None)
    wrong = call(app, "GET", EXPORT_PATH, token="wrong")

    assert missing.status == 401
    assert wrong.status == 401
    assert error_of(wrong)["code"] == "E_AUTH"


def test_export_database_failure_maps_to_500(app: ApiApp, database: Path) -> None:
    connection = open_database(database)
    try:
        connection.execute("DROP TABLE job_runs")
    finally:
        connection.close()

    response = call(app, "GET", EXPORT_PATH)

    assert response.status == 500
    assert error_of(response)["code"] == "E_STORAGE"
    assert "数据库" in error_of(response)["message"]
