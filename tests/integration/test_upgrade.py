r"""升级与回滚编排：成功路径、健康检查失败回滚、保留清理与幂等（docs/08）。

真实库文件与真实目录联接上跑编排；HTTP、schtasks、进程存活与等待全部注入
假件，等待推进虚拟时钟，不在测试里真实睡眠（docs/11 测试代码）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from core.__main__ import EXIT_ERROR, EXIT_OK, main
from core.api.store import TOKEN_KEY, put_settings
from core.clock import VirtualClock
from core.hostops.core_control import UpgradeInputs
from core.hostops.heartbeat import write_heartbeat
from core.hostops.releases import switch_current
from core.hostops.upgrade import run_rollback, run_upgrade
from core.hostops.upgrade_state import (
    UpgradeMarker,
    clear_marker,
    marker_path,
    read_marker,
    write_marker,
)
from storage import migrate, open_database, verify_snapshot

NOW = datetime(2026, 10, 30, 3, 0, tzinfo=UTC)
TOKEN = "upgrade-token"
API_BASE = "http://127.0.0.1:8787"


def _always_healthy() -> bool:
    return True


def _never_healthy() -> bool:
    return False


@dataclass
class StubHttp:
    """HTTP 假件：记录调用，排水固定成功；健康与否由 healthy 谓词现判。

    probe 钩子在每次健康探测前触发一次，模拟新版本核心在健康窗口内的写入。
    """

    calls: list[tuple[str, str]] = field(default_factory=list)
    healthy: Callable[[], bool] = _never_healthy
    probe: Callable[[], None] | None = None

    def __call__(self, method: str, url: str, token: str) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, url))
        if method == "POST":
            return 200, {"status": "draining", "active_runs": 1}
        if self.probe is not None:
            self.probe()
        return (200, {"status": "ok"}) if self.healthy() else (500, {})


@dataclass
class StubRunner:
    """命令假件：记录命令行，一律成功返回；taskkill 模拟为把目标 pid 退场。"""

    commands: list[list[str]] = field(default_factory=list)
    alive: set[int] = field(default_factory=set)

    def __call__(self, command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        self.commands.append(list(command))
        if command[:1] == ["taskkill"]:
            self.alive.discard(int(command[-1]))
        return subprocess.CompletedProcess(command, 0, b"", b"")


@dataclass
class Deployment:
    """一套最小部署：版本目录、current 联接与已初始化的平台库。"""

    root: Path

    @property
    def data_root(self) -> Path:
        return self.root / "data"

    @property
    def app_dir(self) -> Path:
        return self.root / "app"

    @property
    def releases(self) -> Path:
        return self.app_dir / "releases"

    @property
    def database(self) -> Path:
        return self.data_root / "backstop.sqlite"

    @property
    def heartbeat(self) -> Path:
        return self.data_root / "run" / "core.heartbeat"

    def version_dir(self, version: str) -> Path:
        return self.releases / version


def _make_version(directory: Path) -> None:
    directory.mkdir(parents=True)
    (directory / "lap-backstop.exe").write_text("exe", encoding="utf-8")


def _deployment(tmp_path: Path) -> Deployment:
    deploy = Deployment(root=tmp_path / "host")
    for version in ("0.9.0", "1.0.0", "1.1.0"):
        _make_version(deploy.version_dir(version))
    switch_current(deploy.app_dir, deploy.version_dir("1.1.0"))
    connection = open_database(deploy.database)
    try:
        migrate(connection)
        put_settings(
            connection,
            {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 8787},
            at=NOW,
        )
    finally:
        connection.close()
    return deploy


def _beat_heartbeat(deploy: Deployment, *, pid: int, version: str) -> None:
    write_heartbeat(
        deploy.heartbeat,
        {
            "pid": pid,
            "started_at": NOW.isoformat(),
            "version": version,
            "last_tick": NOW.isoformat(),
            "active_run": None,
        },
    )
    os.utime(deploy.heartbeat, (NOW.timestamp(), NOW.timestamp()))


def _step_inputs(
    clock: VirtualClock, http: StubHttp, runner: StubRunner, alive: set[int], exit_pids: set[int]
) -> UpgradeInputs:
    """装配注入项：每轮等待推进虚拟时钟，并让 exit_pids 在首轮退场。"""

    def wait(seconds: float) -> None:
        clock.advance(timedelta(seconds=seconds))
        alive.difference_update(exit_pids)

    return UpgradeInputs(
        clock=clock, http=http, runner=runner, pid_alive=alive.__contains__, wait=wait
    )


def _global_events(database: Path, event_name: str) -> list[dict[str, Any]]:
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


def test_upgrade_success_drains_snapshots_switches_and_records(tmp_path: Path) -> None:
    deploy = _deployment(tmp_path)
    _make_version(deploy.version_dir("1.2.0"))
    _beat_heartbeat(deploy, pid=100, version="1.1.0")
    clock = VirtualClock(NOW)
    alive = {100}
    http = StubHttp(healthy=_always_healthy)
    runner = StubRunner()
    code = run_upgrade(
        deploy.data_root,
        deploy.version_dir("1.2.0"),
        inputs=_step_inputs(clock, http, runner, alive, {100}),
    )

    assert code == EXIT_OK
    assert (deploy.app_dir / "current").resolve().name == "1.2.0"
    assert ("POST", f"{API_BASE}/api/v1/host/drain") in http.calls
    assert ("GET", f"{API_BASE}/api/v1/health") in http.calls
    assert runner.commands == [["schtasks", "/run", "/tn", "lap-backstop-core"]]
    snapshots = sorted((deploy.data_root / "backups").glob("pre-upgrade-1.2.0-*.sqlite"))
    assert len(snapshots) == 1
    assert verify_snapshot(snapshots[0]).kind == "pre-upgrade"
    assert read_marker(marker_path(deploy.data_root)) is None
    assert not deploy.version_dir("0.9.0").exists(), "最旧版本未被保留清理删除"
    assert deploy.version_dir("1.0.0").is_dir() and deploy.version_dir("1.1.0").is_dir()
    finished = _global_events(deploy.database, "host.upgrade_finished")
    assert len(finished) == 1
    assert finished[0]["target_version"] == "1.2.0"
    assert finished[0]["previous_version"] == "1.1.0"


def _new_version_writes(deploy: Deployment) -> Callable[[], None]:
    """模拟新版本核心在健康窗口内的写入：一行设置与它自己的心跳。"""
    state = {"done": False}

    def probe() -> None:
        if state["done"]:
            return
        state["done"] = True
        connection = open_database(deploy.database)
        try:
            put_settings(connection, {"upgrade.canary": "v2"}, at=NOW)
        finally:
            connection.close()
        _beat_heartbeat(deploy, pid=200, version="1.2.0")

    return probe


def _healthy_at(deploy: Deployment, version: str) -> Callable[[], bool]:
    """健康谓词：联接指向哪个版本，哪个版本就应答健康（回滚后才轮到旧版本）。"""

    def healthy() -> bool:
        return (deploy.app_dir / "current").resolve().name == version

    return healthy


def test_upgrade_failed_health_rolls_back_database_junction_and_service(tmp_path: Path) -> None:
    deploy = _deployment(tmp_path)
    _make_version(deploy.version_dir("1.2.0"))
    _beat_heartbeat(deploy, pid=100, version="1.1.0")
    clock = VirtualClock(NOW)
    alive = {100, 200}
    http = StubHttp(healthy=_healthy_at(deploy, "1.1.0"), probe=_new_version_writes(deploy))
    runner = StubRunner(alive=alive)
    inputs = _step_inputs(clock, http, runner, alive, {100})

    code = run_upgrade(deploy.data_root, deploy.version_dir("1.2.0"), inputs=inputs)
    again = run_rollback(deploy.data_root, inputs=inputs)

    assert code == EXIT_ERROR
    assert again == EXIT_OK, "回滚完成后重复执行应判空操作"
    assert (deploy.app_dir / "current").resolve().name == "1.1.0"
    connection = open_database(deploy.database)
    try:
        canary = connection.execute(
            "SELECT value FROM settings WHERE key = 'upgrade.canary'"
        ).fetchone()
    finally:
        connection.close()
    assert canary is None, "库未回到升级前快照"
    assert len(list(deploy.data_root.glob("backstop.pre-rollback-*.sqlite"))) == 1
    assert ["taskkill", "/F", "/T", "/PID", "200"] in runner.commands
    assert runner.commands.count(["schtasks", "/run", "/tn", "lap-backstop-core"]) == 2
    assert read_marker(marker_path(deploy.data_root)) is None
    failures = _global_events(deploy.database, "host.upgrade_failed")
    assert len(failures) == 1, "重复回滚不得产生重复事件"
    assert failures[0]["target_version"] == "1.2.0"
    assert failures[0]["ok"] is True


def test_rollback_twice_on_same_marker_restores_database_only_once(tmp_path: Path) -> None:
    deploy = _deployment(tmp_path)
    _make_version(deploy.version_dir("1.2.0"))
    _beat_heartbeat(deploy, pid=100, version="1.1.0")
    clock = VirtualClock(NOW)
    alive = {100, 200}
    http = StubHttp(healthy=_healthy_at(deploy, "1.1.0"), probe=_new_version_writes(deploy))
    runner = StubRunner(alive=alive)
    inputs = _step_inputs(clock, http, runner, alive, {100})
    run_upgrade(deploy.data_root, deploy.version_dir("1.2.0"), inputs=inputs)
    snapshot = next((deploy.data_root / "backups").glob("pre-upgrade-1.2.0-*.sqlite"))
    write_marker(
        marker_path(deploy.data_root),
        UpgradeMarker(
            target_version="1.2.0",
            previous_version="1.1.0",
            snapshot_name=snapshot.name,
        ),
    )

    code = run_rollback(deploy.data_root, inputs=inputs)

    assert code == EXIT_OK
    assert len(list(deploy.data_root.glob("backstop.pre-rollback-*.sqlite"))) == 1, (
        "重复回滚不得再产生一份保留现场的库副本"
    )


def test_upgrade_drain_timeout_aborts_without_touching_releases(tmp_path: Path) -> None:
    deploy = _deployment(tmp_path)
    _make_version(deploy.version_dir("1.2.0"))
    _beat_heartbeat(deploy, pid=100, version="1.1.0")
    clock = VirtualClock(NOW)
    http = StubHttp()
    runner = StubRunner()
    inputs = UpgradeInputs(
        clock=clock,
        http=http,
        runner=runner,
        pid_alive={100}.__contains__,
        wait=lambda seconds: clock.advance(timedelta(seconds=seconds)),
    )

    code = run_upgrade(deploy.data_root, deploy.version_dir("1.2.0"), inputs=inputs)

    assert code == EXIT_ERROR
    assert (deploy.app_dir / "current").resolve().name == "1.1.0"
    assert not list((deploy.data_root / "backups").glob("pre-upgrade-*.sqlite"))
    assert runner.commands == [], "排水未完成不得拉起新版本"
    assert ("GET", f"{API_BASE}/api/v1/health") not in http.calls
    aborted = _global_events(deploy.database, "host.upgrade_failed")
    assert len(aborted) == 1
    assert aborted[0]["ok"] is True, "核心仍在跑旧版本，服务未中断"


def test_upgrade_same_version_is_noop_without_drain_or_events(tmp_path: Path) -> None:
    deploy = _deployment(tmp_path)
    clock = VirtualClock(NOW)
    http = StubHttp()
    runner = StubRunner()
    inputs = _step_inputs(clock, http, runner, set(), set())

    code = run_upgrade(deploy.data_root, deploy.version_dir("1.1.0"), inputs=inputs)

    assert code == EXIT_OK
    assert http.calls == []
    assert runner.commands == []
    assert not list((deploy.data_root / "backups").glob("pre-upgrade-*.sqlite"))
    assert _global_events(deploy.database, "host.upgrade_finished") == []


def test_rollback_without_marker_is_noop_without_events(tmp_path: Path) -> None:
    deploy = _deployment(tmp_path)
    clock = VirtualClock(NOW)
    runner = StubRunner()
    inputs = _step_inputs(clock, StubHttp(), runner, set(), set())

    code = run_rollback(deploy.data_root, inputs=inputs)

    assert code == EXIT_OK
    assert runner.commands == []
    assert _global_events(deploy.database, "host.upgrade_failed") == []


def test_cli_upgrade_rejects_target_outside_releases(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outside = tmp_path / "elsewhere" / "1.2.0"
    outside.mkdir(parents=True)

    code = main(["upgrade", str(outside), "--data-root", str(tmp_path / "host" / "data")])

    assert code == EXIT_ERROR
    assert "升级未开始" in capsys.readouterr().err


def test_marker_roundtrip_and_missing(tmp_path: Path) -> None:
    path = tmp_path / "upgrade.state.json"
    marker = UpgradeMarker(
        target_version="1.2.0", previous_version="1.1.0", snapshot_name="pre-upgrade-1.2.0.sqlite"
    )

    assert read_marker(path) is None
    write_marker(path, marker)
    assert read_marker(path) == marker
    clear_marker(path)
    assert read_marker(path) is None
