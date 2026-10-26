"""证据文件端点：按 id 取回、跨运行隔离与保留策略后的 410（docs/03 端点表）。

运行与证据数据用 core 层服务直接造，保证与执行器写出的形态一致；
响应头与分块写出走独立 HTTP 服务断言，语义分支走进程内 ApiApp。
"""

from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from contracts import EvidenceKind
from core.api.app import ApiApp
from core.api.server import ApiOptions, ApiServer, start_api_server
from core.api.store import TOKEN_KEY, put_settings
from core.api.wire import ApiRequest, ApiResponse, FileBody
from core.clock import VirtualClock
from core.events.evidence import EvidenceService
from core.jobs.run_service import JobRunService
from storage import migrate, open_database
from storage.repositories.evidence import EvidenceRepository
from storage.repositories.job_runs import NewRun

NOW = datetime(2026, 10, 26, 9, 0, tzinfo=UTC)
TOKEN = "secret-token"
TEMPLATE_ID = "template-1"
# 宿主的 http_proxy 只让行 127.0.0.1，测试只打本机监听，一律绕开代理。
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Path]:
    connection = open_database(tmp_path / "state.db")
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
    yield tmp_path / "state.db"


@pytest.fixture
def app(database: Path) -> ApiApp:
    return ApiApp(database, clock=VirtualClock(NOW))


@pytest.fixture
def server(database: Path) -> Iterator[ApiServer]:
    connection = open_database(database)
    try:
        handle = start_api_server(
            connection,
            ApiOptions(host="127.0.0.1", port=0, clock=VirtualClock(NOW)),
        )
    finally:
        connection.close()
    yield handle
    handle.shutdown()


def seed_run(database: Path, key: str) -> str:
    """造一次运行并返回 id；幂等键用 key 区分同刻的多条运行。"""
    connection = open_database(database)
    try:
        service = JobRunService(connection, VirtualClock(NOW))
        record, _created = service.create_scheduled(
            NewRun(
                template_id=TEMPLATE_ID,
                schedule_id=None,
                scheduled_at=NOW,
                created_at=NOW,
                idempotency_key=f"seed:{key}",
                priority=0,
                deadline_at=None,
            )
        )
        return record.id
    finally:
        connection.close()


def record_evidence(database: Path, run_id: str, name: str, content: bytes) -> str:
    """在证据根落一份文件并登记，返回证据 id。"""
    root = database.parent / "evidence"
    root.mkdir(exist_ok=True)
    (root / name).write_bytes(content)
    connection = open_database(database)
    try:
        service = EvidenceService(connection, root, VirtualClock(NOW))
        return service.record(
            run_id,
            kind=EvidenceKind.LOG,
            path=Path(name),
            summary="测试证据",
        )
    finally:
        connection.close()


def call(app: ApiApp, path: str, *, token: str | None = TOKEN) -> ApiResponse:
    headers = {} if token is None else {"authorization": f"Bearer {token}"}
    return app.handle(ApiRequest("GET", path, {}, headers, b""))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def error_code_of(response: ApiResponse) -> str:
    error = body_of(response)["error"]
    assert isinstance(error, dict)
    return str(error["code"])


def get_file(handle: ApiServer, path: str) -> tuple[int, bytes, dict[str, str]]:
    host, port = handle.address
    req = urllib.request.Request(
        f"http://{host}:{port}{path}",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    try:
        with _OPENER.open(req, timeout=30) as response:
            return (
                response.status,
                response.read(),
                {key.lower(): value for key, value in response.headers.items()},
            )
    except urllib.error.HTTPError as error:
        headers = {key.lower(): value for key, value in error.headers.items()}
        return error.code, error.read(), headers


def test_evidence_file_roundtrip_matches_sha256(app: ApiApp, database: Path) -> None:
    run_id = seed_run(database, "a")
    content = b"evidence-bytes"
    evidence_id = record_evidence(database, run_id, "shot.log", content)
    response = call(app, f"/api/v1/runs/{run_id}/evidence/{evidence_id}")
    assert response.status == 200
    body = response.file
    assert isinstance(body, FileBody)
    try:
        assert body.content_type == "text/plain; charset=utf-8"
        assert body.length == len(content)
        fetched = body.stream.read()
    finally:
        body.stream.close()
    assert fetched == content
    connection = open_database(database)
    try:
        stored = EvidenceRepository(connection).get(evidence_id)
    finally:
        connection.close()
    assert stored is not None
    assert stored.sha256 == hashlib.sha256(fetched).hexdigest()


def test_evidence_content_type_follows_extension(app: ApiApp, database: Path) -> None:
    run_id = seed_run(database, "a")
    cases = {
        "shot.png": "image/png",
        "shot.jpg": "image/jpeg",
        "dump.json": "application/json",
        "run.log": "text/plain; charset=utf-8",
        "notes.txt": "text/plain; charset=utf-8",
        "bundle.zip": "application/zip",
        "blob.bin": "application/octet-stream",
    }
    for name, expected in cases.items():
        evidence_id = record_evidence(database, run_id, name, b"x")
        response = call(app, f"/api/v1/runs/{run_id}/evidence/{evidence_id}")
        assert response.status == 200, name
        body = response.file
        assert isinstance(body, FileBody)
        try:
            assert body.content_type == expected, name
        finally:
            body.stream.close()


def test_evidence_of_other_run_is_not_found(app: ApiApp, database: Path) -> None:
    run_a = seed_run(database, "a")
    run_b = seed_run(database, "b")
    evidence_id = record_evidence(database, run_a, "shot.log", b"private")
    response = call(app, f"/api/v1/runs/{run_b}/evidence/{evidence_id}")
    assert response.status == 404
    assert error_code_of(response) == "E_NOT_FOUND"


def test_evidence_unknown_run_or_id_is_not_found(app: ApiApp, database: Path) -> None:
    run_id = seed_run(database, "a")
    evidence_id = record_evidence(database, run_id, "shot.log", b"x")
    unknown_id = "0" * 32
    assert call(app, f"/api/v1/runs/missing/evidence/{evidence_id}").status == 404
    assert call(app, f"/api/v1/runs/{run_id}/evidence/{unknown_id}").status == 404


def test_gone_evidence_returns_410_without_error_code(app: ApiApp, database: Path) -> None:
    run_id = seed_run(database, "a")
    evidence_id = record_evidence(database, run_id, "shot.log", b"x")
    (database.parent / "evidence" / "shot.log").unlink()
    response = call(app, f"/api/v1/runs/{run_id}/evidence/{evidence_id}")
    assert response.status == 410
    error = body_of(response)["error"]
    assert isinstance(error, dict)
    assert isinstance(error.get("message"), str)
    assert error["message"]
    assert "code" not in error


def test_evidence_path_escape_is_not_found(app: ApiApp, database: Path) -> None:
    run_id = seed_run(database, "a")
    outside = database.parent / "outside.txt"
    outside.write_bytes(b"secret")
    forged_id = "f" * 32
    connection = open_database(database)
    try:
        # 越界记录绕过写入口直接落库，验证读取侧用同一路径判定拦得住
        connection.execute(
            "INSERT INTO evidence (id, run_id, kind, source, path, sha256, summary, "
            "created_at) VALUES (?, ?, 'log', 'plugin', '../outside.txt', ?, '伪造', ?)",
            (forged_id, run_id, "0" * 64, NOW.isoformat()),
        )
    finally:
        connection.close()
    response = call(app, f"/api/v1/runs/{run_id}/evidence/{forged_id}")
    assert response.status == 404
    assert error_code_of(response) == "E_NOT_FOUND"
    assert "outside" not in json.dumps(body_of(response))


def test_evidence_requires_token(app: ApiApp, database: Path) -> None:
    run_id = seed_run(database, "a")
    evidence_id = record_evidence(database, run_id, "shot.log", b"x")
    path = f"/api/v1/runs/{run_id}/evidence/{evidence_id}"
    assert call(app, path, token=None).status == 401
    wrong = call(app, path, token="not-the-token")
    assert wrong.status == 401
    assert error_code_of(wrong) == "E_AUTH"


def test_evidence_file_headers_over_http(server: ApiServer, database: Path) -> None:
    run_id = seed_run(database, "a")
    content = b"hello evidence"
    evidence_id = record_evidence(database, run_id, "shot.log", content)
    status, body, headers = get_file(server, f"/api/v1/runs/{run_id}/evidence/{evidence_id}")
    assert status == 200
    assert body == content
    assert headers["content-type"] == "text/plain; charset=utf-8"
    assert headers["content-length"] == str(len(content))
    assert headers["content-disposition"] == "inline"


def test_large_evidence_streams_completely_over_http(server: ApiServer, database: Path) -> None:
    run_id = seed_run(database, "a")
    content = b"0123456789abcdef" * (8 * 1024 * 1024 // 16)
    evidence_id = record_evidence(database, run_id, "bundle.zip", content)
    status, body, headers = get_file(server, f"/api/v1/runs/{run_id}/evidence/{evidence_id}")
    assert status == 200
    assert headers["content-type"] == "application/zip"
    assert int(headers["content-length"]) == len(content)
    assert body == content
