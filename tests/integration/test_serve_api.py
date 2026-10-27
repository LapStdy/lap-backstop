"""serve 挂载 API：token 自生成、监听集合、端口冲突退 4 与端点冒烟（docs/08 单实例）。"""

from __future__ import annotations

import json
import socket
import sqlite3
import subprocess
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.api.store import TOKEN_KEY, bearer_token, put_settings
from core.clock import VirtualClock
from core.hostops import instance
from core.hostops.serve import ServeOptions, run_serve
from core.hostops.tailscale import (
    REMOTE_ENABLED_KEY,
    TAILSCALE_BIND_FAILED_EVENT,
    TAILSCALE_DOWN_EVENT,
    remote_listen_plan,
)
from core.logging import NullLogger
from storage import migrate, open_database
from storage.repositories.environments import EnvironmentRepository
from tests.fixtures.subprocess_fakes import completed
from tests.integration.test_serve_shell import (
    autologon_registry,
    fake_task_runner,
    global_events,
)

NOW = datetime(2026, 10, 26, 11, 0, tzinfo=UTC)
TOKEN = "serve-token"
ENV_ID = "env-1"


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


def http_post(url: str) -> tuple[int, Any]:
    request = urllib.request.Request(
        url, method="POST", headers={"Authorization": f"Bearer {TOKEN}"}
    )
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


def stored_token(root: Path) -> str | None:
    """读数据根里库的当前 token。"""
    connection = open_database(root / "backstop.sqlite")
    try:
        return bearer_token(connection)
    finally:
        connection.close()


def global_payloads(database: Path, event_name: str) -> list[dict[str, Any]]:
    """读全局事件里指定 event 名的载荷，按写入顺序。"""
    reader = sqlite3.connect(database)
    try:
        rows = reader.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    finally:
        reader.close()
    return [
        json.loads(str(row[0]))
        for row in rows
        if json.loads(str(row[0])).get("event") == event_name
    ]


def scripted_tailscale(payload: bytes) -> Any:
    """只应答 tailscale status 的假执行器；其余命令按不可用处理。"""

    def run(command: list[str]) -> subprocess.CompletedProcess[bytes]:
        if tuple(command) == ("tailscale", "status", "--json"):
            return completed(payload)
        return completed(returncode=-1)

    return run


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    """预置好 token 与随机端口的监听设置的数据根。"""
    root = tmp_path / "data"
    configure(root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    return root


def test_token_generated_when_missing_and_api_accepts_it(tmp_path: Path) -> None:
    root = tmp_path / "data"
    configure(root, {"api.host": "127.0.0.1", "api.port": 0})
    logger = RecordingLogger()
    health: list[int] = []

    def on_listening(host: str, port: int) -> None:
        token = stored_token(root)
        assert token is not None and len(token) >= 32
        request = urllib.request.Request(
            f"http://{host}:{port}/api/v1/health",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            health.append(response.status)

    assert run_serve(root, serve_options(log=logger, on_api_listening=on_listening)) == 0
    assert health == [200]
    assert "serve.token_generated" in logger.events()
    token = stored_token(root)
    assert token is not None
    # token 值不进任何日志行（docs/11）
    assert all(token not in json.dumps(line, ensure_ascii=False) for line in logger.lines)


def test_existing_token_is_kept(data_root: Path) -> None:
    logger = RecordingLogger()
    assert run_serve(data_root, serve_options(log=logger)) == 0
    assert "serve.token_generated" not in logger.events()
    assert stored_token(data_root) == TOKEN


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


def test_serve_environment_action_runs_on_request_thread_connection(data_root: Path) -> None:
    """serve 装配的环境动作在 API 线程自开连接：provider 缺失拿 409，而不是跨线程 500。"""
    seed = open_database(data_root / "backstop.sqlite")
    try:
        EnvironmentRepository(seed).ensure(ENV_ID, provider_id="missing", name="MuMu")
    finally:
        seed.close()
    seen: dict[str, tuple[int, Any]] = {}

    def on_listening(host: str, port: int) -> None:
        seen["prepare"] = http_post(f"http://{host}:{port}/api/v1/environments/{ENV_ID}/prepare")

    assert run_serve(data_root, serve_options(on_api_listening=on_listening)) == 0
    status, payload = seen["prepare"]
    assert status == 409
    assert payload["error"]["code"] == "E_CONFLICT"


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


def test_port_conflict_with_remote_enabled_still_exits_4(data_root: Path) -> None:
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    configure(data_root, {"api.port": port, REMOTE_ENABLED_KEY: True})
    logger = RecordingLogger()
    needs_login = json.dumps({"BackendState": "NeedsLogin", "TailscaleIPs": []}).encode()
    try:
        code = run_serve(
            data_root, serve_options(log=logger, tailscale_runner=scripted_tailscale(needs_login))
        )
    finally:
        blocker.close()
    assert code == 4
    assert "core.port_conflict" in global_events(data_root / "backstop.sqlite")


def test_remote_disabled_skips_tailscale_probe(tmp_path: Path) -> None:
    root = tmp_path / "data"
    configure(root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    probes: list[list[str]] = []
    primary: list[str] = []

    def probe_runner(command: list[str]) -> subprocess.CompletedProcess[bytes]:
        probes.append(command)
        return completed(returncode=-1)

    def on_listening(host: str, port: int) -> None:
        primary.append(host)

    options = serve_options(tailscale_runner=probe_runner, on_api_listening=on_listening)
    assert run_serve(root, options) == 0
    assert probes == []
    assert primary == ["127.0.0.1"]


def test_remote_enabled_without_tailscale_records_event_once(tmp_path: Path) -> None:
    root = tmp_path / "data"
    configure(
        root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0, REMOTE_ENABLED_KEY: True}
    )
    needs_login = json.dumps({"BackendState": "NeedsLogin", "TailscaleIPs": []}).encode()
    assert run_serve(root, serve_options(tailscale_runner=scripted_tailscale(needs_login))) == 0
    payloads = global_payloads(root / "backstop.sqlite", TAILSCALE_DOWN_EVENT)
    assert payloads == [{"event": TAILSCALE_DOWN_EVENT, "detail": "NeedsLogin"}]


def test_tailnet_bind_failure_degrades_to_loopback(tmp_path: Path) -> None:
    root = tmp_path / "data"
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]
    blocker.close()
    configure(
        root,
        {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": port, REMOTE_ENABLED_KEY: True},
    )
    running = json.dumps({"BackendState": "Running", "TailscaleIPs": ["100.64.0.1"]}).encode()
    health: list[int] = []

    def on_listening(host: str, bound_port: int) -> None:
        assert host == "127.0.0.1"
        request = urllib.request.Request(
            f"http://{host}:{bound_port}/api/v1/health",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            health.append(response.status)

    options = serve_options(
        tailscale_runner=scripted_tailscale(running), on_api_listening=on_listening
    )
    assert run_serve(root, options) == 0
    assert health == [200]
    database = root / "backstop.sqlite"
    failures = global_payloads(database, TAILSCALE_BIND_FAILED_EVENT)
    assert len(failures) == 1
    assert "100.64.0.1" in failures[0]["detail"]
    assert global_payloads(database, TAILSCALE_DOWN_EVENT) == []


def test_remote_listen_plan_disabled_returns_empty() -> None:
    """remote_enabled 关闭时不探测也不追加地址。"""
    running = json.dumps({"BackendState": "Running", "TailscaleIPs": ["100.64.0.1"]}).encode()
    plan = remote_listen_plan({}, scripted_tailscale(running), NullLogger())
    assert plan == ((), None)


def test_remote_listen_plan_running_returns_tailnet_addresses() -> None:
    """remote_enabled 且 Running 时计划里只有 tailnet IPv4。"""
    running = json.dumps(
        {"BackendState": "Running", "TailscaleIPs": ["100.64.0.1", "fd7a:115c:a1e0::1"]}
    ).encode()
    plan = remote_listen_plan({REMOTE_ENABLED_KEY: True}, scripted_tailscale(running), NullLogger())
    assert plan == (("100.64.0.1",), None)


def test_remote_listen_plan_unavailable_returns_down_detail() -> None:
    """Tailscale 不可查时只回环，detail 带不可查原因。"""
    plan = remote_listen_plan(
        {REMOTE_ENABLED_KEY: True}, lambda command: completed(returncode=-1), NullLogger()
    )
    assert plan == ((), "不可查：命令不可用")


def test_remote_enabled_invalid_type_rejects_startup(tmp_path: Path) -> None:
    root = tmp_path / "data"
    configure(
        root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0, REMOTE_ENABLED_KEY: "yes"}
    )
    logger = RecordingLogger()
    assert run_serve(root, serve_options(log=logger)) == 1
    assert "serve.api_config_invalid" in logger.events()
