"""serve 挂载 API：token 门禁、监听设置、端口冲突退 4 与端点冒烟（docs/08 单实例）。"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.api.store import TOKEN_KEY, put_settings
from core.clock import VirtualClock
from core.hostops import instance
from core.hostops.serve import ServeOptions, run_serve
from storage import migrate, open_database
from tests.integration.test_serve_shell import (
    autologon_registry,
    fake_task_runner,
    global_events,
)

NOW = datetime(2026, 10, 26, 11, 0, tzinfo=UTC)
TOKEN = "serve-token"


class RecordingLogger:
    """收下全部日志行的测试替身。"""

    def __init__(self) -> None:
        self.lines: list[dict[str, Any]] = []

    def debug(self, fields: dict[str, Any]) -> None:
        self.lines.append(fields)

    def info(self, fields: dict[str, Any]) -> None:
        self.lines.append(fields)

    def warn(self, fields: dict[str, Any]) -> None:
        self.lines.append(fields)

    def error(self, fields: dict[str, Any]) -> None:
        self.lines.append(fields)

    def events(self) -> list[str]:
        return [str(line.get("event")) for line in self.lines]


def serve_options(**kwargs: Any) -> ServeOptions:
    """进程内 serve 注入项：跳过任务注册与注册表读，跑有界轮次。"""
    task_runner, _ = fake_task_runner()
    defaults: dict[str, Any] = {
        "clock": VirtualClock(NOW),
        "max_ticks": 1,
        "tick_interval_seconds": 0.0,
        "register_tasks": False,
        "registry_get": lambda _key, name: autologon_registry("1", "backstop")[name],
        "task_runner": task_runner,
    }
    defaults.update(kwargs)
    return ServeOptions(**defaults)


def http_get(url: str) -> tuple[int, Any]:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


def configure(root: Path, values: dict[str, Any]) -> None:
    """预置或改写数据根里库的 settings；按需建库。"""
    connection = open_database(root / "backstop.sqlite")
    try:
        migrate(connection)
        put_settings(connection, values, at=NOW)
    finally:
        connection.close()


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    """预置好 token 与随机端口的监听设置的数据根。"""
    root = tmp_path / "data"
    configure(root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    return root


def test_api_skipped_without_token(tmp_path: Path) -> None:
    root = tmp_path / "data"
    configure(root, {})
    logger = RecordingLogger()
    assert run_serve(root, serve_options(log=logger)) == 0
    assert "serve.api_skipped" in logger.events()


def test_serve_smokes_endpoints_over_mounted_api(data_root: Path) -> None:
    seen: dict[str, tuple[int, Any]] = {}

    def on_listening(host: str, port: int) -> None:
        seen["health"] = http_get(f"http://{host}:{port}/api/v1/health")
        seen["settings"] = http_get(f"http://{host}:{port}/api/v1/settings")

    assert run_serve(data_root, serve_options(on_api_listening=on_listening)) == 0
    status, payload = seen["health"]
    assert status == 200
    assert payload["status"] == "ok"
    assert "events" in payload["capabilities"]
    assert seen["settings"][0] == 200
    # 正常收尾：锁文件删除，API 随进程收尾关停
    assert not (data_root / "run" / instance.LOCK_FILENAME).exists()


def test_serve_rejects_invalid_port_config(tmp_path: Path) -> None:
    root = tmp_path / "data"
    configure(root, {TOKEN_KEY: TOKEN, "api.port": "oops"})
    logger = RecordingLogger()
    assert run_serve(root, serve_options(log=logger)) == 1
    assert "serve.api_config_invalid" in logger.events()
    assert not (root / "run" / instance.LOCK_FILENAME).exists()


def test_serve_port_conflict_exits_4_and_records_event(data_root: Path) -> None:
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    configure(data_root, {"api.port": port})
    logger = RecordingLogger()
    try:
        assert run_serve(data_root, serve_options(log=logger)) == 4
    finally:
        blocker.close()
    assert "serve.port_conflict" in logger.events()
    assert "core.port_conflict" in global_events(data_root / "backstop.sqlite")
    assert not (data_root / "run" / instance.LOCK_FILENAME).exists()
