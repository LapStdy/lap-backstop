"""跨连接并发写：请求线程各自开连接，写事务按设计串行，最终状态不撕裂。

同步点是 threading.Barrier：两线程都就位后同时放行，不用 sleep 猜时序；
结果断言与两线程的先后顺序无关。
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator, Sequence
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
TEMPLATE_ID = "template-1"
MINIMAL = {"name": "每日日常", "kind": "cron", "expr": "0 4 * * *", "job_template_id": TEMPLATE_ID}


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
    yield ApiApp(database, clock=VirtualClock(NOW))


def call(
    app: ApiApp,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> ApiResponse:
    headers = {"authorization": f"Bearer {TOKEN}"}
    if extra_headers is not None:
        headers.update(extra_headers)
    payload = b"" if body is None else json.dumps(body).encode("utf-8")
    return app.handle(ApiRequest(method, path, {}, headers, payload))


def body_of(response: ApiResponse) -> dict[str, Any]:
    assert isinstance(response.payload, dict)
    return response.payload


def run_in_two_threads(
    actions: Sequence[Callable[[], ApiResponse]],
) -> list[ApiResponse]:
    """两个线程各跑一个动作，Barrier 对齐起点；任一线程异常在本线程汇合后上抛。"""
    barrier = threading.Barrier(2)
    results: list[ApiResponse | Exception | None] = [None, None]

    def worker(index: int) -> None:
        barrier.wait()
        try:
            results[index] = actions[index]()
        except Exception as error:
            results[index] = error

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    assert all(not thread.is_alive() for thread in threads), "线程 5 秒内未结束"
    for result in results:
        assert isinstance(result, ApiResponse), results
    return [result for result in results if isinstance(result, ApiResponse)]


def test_concurrent_run_now_with_same_key_creates_one_run(app: ApiApp) -> None:
    created = call(app, "POST", "/api/v1/schedules", body=MINIMAL)
    assert created.status == 201
    path = f"/api/v1/schedules/{body_of(created)['schedule']['id']}/run"
    headers = {"idempotency-key": "race-key"}

    responses = run_in_two_threads(
        [
            lambda: call(app, "POST", path, extra_headers=headers),
            lambda: call(app, "POST", path, extra_headers=headers),
        ]
    )

    # 同一幂等键的两次并发提交按设计串行：先到的建 run，后到的命中既有 run
    assert sorted(response.status for response in responses) == [200, 201]
    run_ids = {str(body_of(response)["run"]["id"]) for response in responses}
    assert len(run_ids) == 1
    listing = body_of(call(app, "GET", "/api/v1/runs"))["runs"]
    assert len(listing) == 1
    assert str(listing[0]["id"]) in run_ids
    assert listing[0]["idempotency_key"] == "manual:race-key"
    assert listing[0]["state"] == "scheduled"


def test_concurrent_settings_writes_keep_both_keys(app: ApiApp) -> None:
    responses = run_in_two_threads(
        [
            lambda: call(app, "PUT", "/api/v1/settings", body={"api.host": "127.0.0.2"}),
            lambda: call(app, "PUT", "/api/v1/settings", body={"api.port": 9999}),
        ]
    )

    assert [response.status for response in responses] == [200, 200]
    settings = body_of(call(app, "GET", "/api/v1/settings"))["settings"]
    assert settings["api.host"] == "127.0.0.2"
    assert settings["api.port"] == 9999
