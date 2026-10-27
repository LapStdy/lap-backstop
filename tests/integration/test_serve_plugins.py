"""serve 装配插件：真 worker、真 HTTP 环境动作、坏插件隔离与收尾回收。"""

from __future__ import annotations

import json
import shutil
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from contracts import Logger
from core.api.store import TOKEN_KEY
from core.hostops import serve as serve_module
from core.plugins.rpc_host import RpcPluginHost
from storage import open_database
from storage.repositories.environments import EnvironmentRepository
from tests.integration.test_serve_api import (
    TOKEN,
    RecordingLogger,
    configure,
    http_get,
    http_post,
    serve_options,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_ID = "env-1"


def copy_fake_plugin(tmp_path: Path) -> Path:
    """把 fake 插件复制到临时插件根，避免装配到仓库里的 maa。"""
    plugins_root = tmp_path / "plugins"
    shutil.copytree(REPO_ROOT / "plugins" / "fake", plugins_root / "fake")
    return plugins_root


def seed_environment(data_root: Path, provider_id: str = "fake") -> None:
    connection = open_database(data_root / "backstop.sqlite")
    try:
        EnvironmentRepository(connection).ensure(ENV_ID, provider_id=provider_id, name=provider_id)
    finally:
        connection.close()


def global_payloads_by_type(database: Path, event_type: str) -> list[dict[str, Any]]:
    """按事件类型读全局事件载荷；插件事件的事件名在 type 列，不在 payload 里。"""
    reader = sqlite3.connect(database)
    try:
        rows = reader.execute(
            "SELECT payload FROM events_global WHERE type = ? ORDER BY seq", (event_type,)
        ).fetchall()
    finally:
        reader.close()
    return [json.loads(str(row[0])) for row in rows]


@pytest.fixture
def captured_hosts(monkeypatch: pytest.MonkeyPatch) -> list[RpcPluginHost]:
    """截住 serve 装配的宿主实例；装配语义仍由真实 RpcPluginHost 执行。"""
    instances: list[RpcPluginHost] = []

    class CapturingHost(RpcPluginHost):
        def __init__(
            self,
            plugins_root: Path,
            plugin_ids: Sequence[str],
            *,
            log: Logger | None = None,
        ) -> None:
            super().__init__(plugins_root, plugin_ids, log=log)
            instances.append(self)

    monkeypatch.setattr(serve_module, "RpcPluginHost", CapturingHost)
    return instances


def test_serve_registers_fake_plugin_and_environment_action_reaches_ready(
    tmp_path: Path,
    captured_hosts: list[RpcPluginHost],
) -> None:
    plugins_root = copy_fake_plugin(tmp_path)
    data_root = tmp_path / "data"
    configure(data_root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    seed_environment(data_root)
    seen: dict[str, Any] = {}

    def on_listening(host: str, port: int) -> None:
        registry = captured_hosts[0].registry
        assert registry.plugin_ids() == ("fake",)
        assert registry.environment("fake") is not None
        seen["prepare"] = http_post(f"http://{host}:{port}/api/v1/environments/{ENV_ID}/prepare")
        seen["environment"] = http_get(f"http://{host}:{port}/api/v1/environments/{ENV_ID}")

    code = serve_module.run_serve(
        data_root, serve_options(plugins_root=plugins_root, on_api_listening=on_listening)
    )
    assert code == 0
    status, payload = seen["prepare"]
    assert (status, payload["ok"]) == (200, True)
    env_status, env_payload = seen["environment"]
    assert env_status == 200
    assert env_payload["environment"]["state"] == "ready"
    assert global_payloads_by_type(data_root / "backstop.sqlite", "plugin.loaded") == [
        {"plugin_id": "fake", "version": "1.0.0"}
    ]


def test_serve_rejects_broken_manifest_and_keeps_other_plugins(
    tmp_path: Path,
    captured_hosts: list[RpcPluginHost],
) -> None:
    plugins_root = copy_fake_plugin(tmp_path)
    broken = plugins_root / "broken"
    broken.mkdir()
    (broken / "plugin.json").write_text("{", encoding="utf-8")
    shutil.copy2(REPO_ROOT / "plugins" / "fake" / "plugin.py", broken / "plugin.py")
    data_root = tmp_path / "data"
    configure(data_root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    seed_environment(data_root)
    logger = RecordingLogger()
    seen: dict[str, tuple[int, Any]] = {}

    def on_listening(host: str, port: int) -> None:
        seen["prepare"] = http_post(f"http://{host}:{port}/api/v1/environments/{ENV_ID}/prepare")

    code = serve_module.run_serve(
        data_root,
        serve_options(plugins_root=plugins_root, log=logger, on_api_listening=on_listening),
    )
    assert code == 0
    assert seen["prepare"][0] == 200
    host = captured_hosts[0]
    assert host.rejected == ("broken",)
    assert host.registry.plugin_ids() == ("fake",)
    rejections = global_payloads_by_type(data_root / "backstop.sqlite", "plugin.rejected")
    assert len(rejections) == 1
    assert rejections[0]["plugin_id"] == "broken"
    assert "加载失败" in rejections[0]["reason"]
    assert "plugin.start_failed" in logger.events()
    assert [line for line in logger.lines if line.get("event") == "plugin.start_failed"] == [
        {
            "event": "plugin.start_failed",
            "plugin_id": "broken",
            "error": rejections[0]["reason"],
        }
    ]


def test_serve_closes_worker_channels_on_shutdown(
    tmp_path: Path,
    captured_hosts: list[RpcPluginHost],
) -> None:
    plugins_root = copy_fake_plugin(tmp_path)
    data_root = tmp_path / "data"
    configure(data_root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    running: dict[str, Any] = {}

    def on_listening(host: str, port: int) -> None:
        channels = tuple(captured_hosts[0].channels.values())
        running["channels"] = channels
        running["pids"] = tuple(channel.pid for channel in channels)

    assert (
        serve_module.run_serve(
            data_root, serve_options(plugins_root=plugins_root, on_api_listening=on_listening)
        )
        == 0
    )
    channels = running["channels"]
    assert channels and all(pid is not None for pid in running["pids"])
    assert captured_hosts[0].channels == {}
    assert all(channel.pid is None for channel in channels)
