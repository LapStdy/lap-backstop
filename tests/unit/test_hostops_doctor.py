"""doctor：14 项检查的 PASS/WARN/FAIL 三态与退出码聚合，外部依赖全注入。"""

from __future__ import annotations

import io
import json
import os
import socket
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.hostops import doctor, instance, tasks
from core.hostops.heartbeat import HEARTBEAT_FILENAME, write_heartbeat
from storage import migrate, open_database
from tests.fixtures.subprocess_fakes import completed

NOW = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)
AC_ZERO = b"    Current AC Power Setting Index: 0x00000000\n"
TASK_NAMES = (
    tasks.TASK_CORE,
    tasks.TASK_WATCHDOG,
    tasks.TASK_BACKUP,
    tasks.TASK_EVIDENCE_MIRROR,
)


def unavailable_runner(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    """外部命令一律不可用（可执行体缺失的等价物）。"""
    return subprocess.CompletedProcess(command, -1, b"", b"")


def scripted(script: dict[tuple[str, ...], subprocess.CompletedProcess[bytes]]) -> doctor.Runner:
    def run(command: list[str]) -> subprocess.CompletedProcess[bytes]:
        return script.get(tuple(command), unavailable_runner(command))

    return run


def make_deps(tmp_path: Path, **overrides: object) -> doctor.DoctorDeps:
    values: dict[str, object] = {
        "data_root": tmp_path,
        "app_root": tmp_path / "app",
        "now": NOW,
        "registry_get": lambda _key, _name: None,
        "runner": unavailable_runner,
        "disk_free": lambda _path: 100 * 1024**3,
    }
    values.update(overrides)
    return doctor.DoctorDeps(**values)  # type: ignore[arg-type]


def make_database(data_root: Path) -> None:
    connection = open_database(data_root / doctor.DATABASE_FILENAME)
    try:
        migrate(connection)
    finally:
        connection.close()


def single(deps: doctor.DoctorDeps, number: int) -> doctor.CheckResult:
    return doctor.collect(deps)[number - 1]


def test_check_data_writable(tmp_path: Path) -> None:
    assert single(make_deps(tmp_path), 1).level is doctor.Level.PASS
    blocked = tmp_path / "occupied"
    blocked.write_text("x", encoding="utf-8")
    assert single(make_deps(blocked), 1).level is doctor.Level.FAIL


def test_check_database_integrity(tmp_path: Path) -> None:
    make_database(tmp_path)
    assert single(make_deps(tmp_path), 2).level is doctor.Level.PASS
    (tmp_path / doctor.DATABASE_FILENAME).write_bytes(b"garbage bytes")
    assert single(make_deps(tmp_path), 2).level is doctor.Level.FAIL


def test_check_migration_version(tmp_path: Path) -> None:
    make_database(tmp_path)
    assert single(make_deps(tmp_path), 3).level is doctor.Level.PASS
    # 空库 integrity 是好的，但版本落后于代码
    (tmp_path / doctor.DATABASE_FILENAME).write_bytes(b"")
    result = single(make_deps(tmp_path), 3)
    assert result.level is doctor.Level.FAIL
    assert "库为 0" in result.detail


def test_check_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert single(make_deps(tmp_path), 4).level is doctor.Level.PASS

    def broken() -> dict[str, object]:
        raise OSError("file missing")

    monkeypatch.setattr(doctor, "load_defaults", broken)
    assert single(make_deps(tmp_path), 4).level is doctor.Level.FAIL


def test_check_app_current(tmp_path: Path) -> None:
    assert single(make_deps(tmp_path), 5).level is doctor.Level.FAIL
    (tmp_path / "app" / "current").mkdir(parents=True)
    assert single(make_deps(tmp_path), 5).level is doctor.Level.PASS


def test_check_port_free(tmp_path: Path) -> None:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    address = server.getsockname()
    try:
        assert single(make_deps(tmp_path, api_address=address), 6).level is doctor.Level.FAIL
    finally:
        server.close()
    assert single(make_deps(tmp_path, api_address=address), 6).level is doctor.Level.PASS


def test_check_disk_watermark(tmp_path: Path) -> None:
    gib = 1024**3
    assert single(make_deps(tmp_path, disk_free=lambda _p: 6 * gib), 7).level is doctor.Level.PASS
    assert single(make_deps(tmp_path, disk_free=lambda _p: 4 * gib), 7).level is doctor.Level.WARN
    assert single(make_deps(tmp_path, disk_free=lambda _p: 1 * gib), 7).level is doctor.Level.FAIL
    assert single(make_deps(tmp_path, disk_free=lambda _p: None), 7).level is doctor.Level.FAIL


def test_check_heartbeat_fresh(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    path = run_dir / HEARTBEAT_FILENAME
    write_heartbeat(
        path,
        {
            "pid": 1234,
            "started_at": NOW.isoformat(),
            "version": "0.1.0",
            "last_tick": NOW.isoformat(),
            "active_run": None,
        },
    )
    stamp = NOW.timestamp() - 10
    os.utime(path, (stamp, stamp))
    assert single(make_deps(tmp_path), 8).level is doctor.Level.PASS
    stale = NOW.timestamp() - 400
    os.utime(path, (stale, stale))
    assert single(make_deps(tmp_path), 8).level is doctor.Level.WARN
    path.unlink()
    assert single(make_deps(tmp_path), 8).level is doctor.Level.WARN


def test_check_lock_and_mutex(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """锁文件与互斥量的对应关系；互斥量用假件，不碰本机全局命名互斥量。"""
    held: list[bool] = []

    def fake_acquire(_name: str) -> int | None:
        if held:
            return None
        held.append(True)
        return 1

    def fake_release(_handle: int) -> None:
        held.clear()

    monkeypatch.setattr(instance, "acquire_named_mutex", fake_acquire)
    monkeypatch.setattr(instance, "release_named_mutex", fake_release)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    lock_path = run_dir / instance.LOCK_FILENAME
    assert single(make_deps(tmp_path), 9).level is doctor.Level.PASS

    def lock_for(pid: int) -> instance.LockContent:
        return {"pid": pid, "started_at": NOW.isoformat(), "exe_path": "x", "boot_id": ""}

    instance.write_lock_file(lock_path, lock_for(4_000_000))
    assert single(make_deps(tmp_path), 9).level is doctor.Level.WARN

    instance.write_lock_file(lock_path, lock_for(os.getpid()))
    handle = instance.acquire_named_mutex(instance.MUTEX_NAME)
    assert handle is not None
    try:
        assert single(make_deps(tmp_path), 9).level is doctor.Level.PASS
    finally:
        instance.release_named_mutex(handle)
    assert single(make_deps(tmp_path), 9).level is doctor.Level.WARN


def test_check_autologon(tmp_path: Path) -> None:
    good = {"AutoAdminLogon": "1", "DefaultUserName": "backstop"}
    deps = make_deps(tmp_path, registry_get=lambda _key, name: good.get(name))
    assert single(deps, 10).level is doctor.Level.PASS
    bad = {"AutoAdminLogon": "0", "DefaultUserName": None}
    deps = make_deps(tmp_path, registry_get=lambda _key, name: bad.get(name))
    result = single(deps, 10)
    assert result.level is doctor.Level.WARN
    assert "登录界面" in result.detail


def _task_query_script(tmp_path: Path) -> dict[tuple[str, ...], subprocess.CompletedProcess[bytes]]:
    tasks_dir = tmp_path / tasks.TASKS_DIRNAME
    tasks.write_definitions(
        tasks_dir,
        {
            tasks.TASK_CORE: tasks.TaskAction("exe", "serve"),
            tasks.TASK_WATCHDOG: tasks.TaskAction("exe", "watchdog check"),
            tasks.TASK_BACKUP: tasks.TaskAction("exe", "backup"),
            tasks.TASK_EVIDENCE_MIRROR: tasks.TaskAction("exe", "evidence-mirror"),
        },
        registered_at=NOW,
    )
    script: dict[tuple[str, ...], subprocess.CompletedProcess[bytes]] = {}
    for name in TASK_NAMES:
        local = tasks_dir / f"{name}.xml"
        script[("schtasks", "/query", "/tn", name)] = completed(
            local.read_text(encoding="utf-16").encode("utf-16")
        )
    return script


def test_check_scheduled_tasks(tmp_path: Path) -> None:
    script = _task_query_script(tmp_path)
    deps = make_deps(tmp_path, runner=scripted(script))
    result = single(deps, 11)
    assert result.level is doctor.Level.PASS
    assert result.detail == "4 个任务在位"

    script[("schtasks", "/query", "/tn", tasks.TASK_WATCHDOG)] = completed(returncode=1)
    result = single(deps, 11)
    assert result.level is doctor.Level.WARN
    assert result.detail == f"{tasks.TASK_WATCHDOG}；跑 lap-backstop tasks register 修复"

    # backup 缺失同样按可修复项报出，不再有「随后续里程碑启用」的观察项
    script = _task_query_script(tmp_path)
    script[("schtasks", "/query", "/tn", tasks.TASK_BACKUP)] = completed(returncode=1)
    result = single(make_deps(tmp_path, runner=scripted(script)), 11)
    assert result.level is doctor.Level.WARN
    assert result.detail == f"{tasks.TASK_BACKUP}；跑 lap-backstop tasks register 修复"

    script = _task_query_script(tmp_path)
    drifted = (
        "<Task><Actions><Exec><Command>other.exe</Command>"
        "<Arguments>x</Arguments></Exec></Actions></Task>"
    )
    script[("schtasks", "/query", "/tn", tasks.TASK_WATCHDOG)] = completed(drifted.encode())
    result = single(make_deps(tmp_path, runner=scripted(script)), 11)
    assert result.level is doctor.Level.WARN
    assert f"{tasks.TASK_WATCHDOG} 动作漂移" in result.detail


def test_check_time_service(tmp_path: Path) -> None:
    script: dict[tuple[str, ...], subprocess.CompletedProcess[bytes]] = {
        ("sc", "query", "w32time"): completed(b"STATE : 4 RUNNING\n"),
        ("w32tm", "/query", "/status"): completed(b"synchronised"),
    }
    deps = make_deps(tmp_path, runner=scripted(script))
    assert single(deps, 12).level is doctor.Level.PASS

    script[("sc", "query", "w32time")] = completed(b"STATE : 1 STOPPED\n")
    assert single(deps, 12).level is doctor.Level.WARN

    script[("sc", "query", "w32time")] = completed(b"STATE : 4 RUNNING\n")
    del script[("w32tm", "/query", "/status")]
    assert single(deps, 12).level is doctor.Level.WARN


def test_check_tailscale_and_power(tmp_path: Path) -> None:
    running = completed(json.dumps({"BackendState": "Running"}).encode())
    script: dict[tuple[str, ...], subprocess.CompletedProcess[bytes]] = {
        ("tailscale", "status", "--json"): running,
        ("powercfg", "/q", "SCHEME_CURRENT", "SUB_SLEEP", "STANDBYIDLE"): completed(AC_ZERO),
        ("powercfg", "/q", "SCHEME_CURRENT", "SUB_SLEEP", "HIBERNATEIDLE"): completed(AC_ZERO),
    }
    deps = make_deps(tmp_path, runner=scripted(script))
    assert single(deps, 13).level is doctor.Level.PASS

    script[("tailscale", "status", "--json")] = completed(returncode=1)
    result = single(deps, 13)
    assert result.level is doctor.Level.WARN
    assert "Tailscale" in result.detail

    script[("tailscale", "status", "--json")] = running
    script[("powercfg", "/q", "SCHEME_CURRENT", "SUB_SLEEP", "STANDBYIDLE")] = completed(
        b"    Current AC Power Setting Index: 0x00000001\n"
    )
    result = single(deps, 13)
    assert result.level is doctor.Level.WARN
    assert "睡眠" in result.detail


FIREWALL_QUERY: tuple[str, ...] = (
    "netsh",
    "advfirewall",
    "firewall",
    "show",
    "rule",
    f"name={doctor.FIREWALL_RULE_NAME}",
)


def firewall_block(enabled: str, direction: str, action: str, remote: str, port: str) -> str:
    """构造一段 netsh show rule 的中文输出。"""
    return (
        f"规则名称:                            {doctor.FIREWALL_RULE_NAME}\n"
        "----------------------------------------------------------------------\n"
        f"已启用:                               {enabled}\n"
        f"方向:                                 {direction}\n"
        "配置文件:                             域, 专用, 公用\n"
        "本地IP:                               Any\n"
        f"远程IP:                               {remote}\n"
        "协议:                                 TCP\n"
        f"本地端口:                             {port}\n"
        "远程端口:                             Any\n"
        f"操作:                                 {action}\n"
    )


def test_check_inbound_firewall_rule_in_scope_passes(tmp_path: Path) -> None:
    script = {
        FIREWALL_QUERY: completed(
            firewall_block("是", "入", "允许", "100.64.0.0/10,127.0.0.0/8", "8787").encode()
        )
    }
    deps = make_deps(tmp_path, runner=scripted(script))
    assert single(deps, 14).level is doctor.Level.PASS

    # 英文字段名同样可解析
    english = (
        "Rule Name:                            lap-backstop-api\n"
        "----------------------------------------------------------------------\n"
        "Enabled:                              Yes\n"
        "Direction:                            In\n"
        "RemoteIP:                             100.64.0.0/10,127.0.0.0/8\n"
        "Protocol:                             TCP\n"
        "LocalPort:                            8787\n"
        "Action:                               Allow\n"
    )
    script[FIREWALL_QUERY] = completed(english.encode())
    assert single(deps, 14).level is doctor.Level.PASS

    # 中文宿主控制台的 GBK 输出也能解出字段
    script[FIREWALL_QUERY] = completed(
        firewall_block("是", "入", "允许", "100.64.0.0/10,127.0.0.0/8", "8787").encode("gbk")
    )
    assert single(deps, 14).level is doctor.Level.PASS


def test_check_inbound_firewall_missing_or_out_of_scope_warns(tmp_path: Path) -> None:
    script: dict[tuple[str, ...], subprocess.CompletedProcess[bytes]] = {}
    deps = make_deps(tmp_path, runner=scripted(script))
    # 规则缺失（命令不可用是等价形态）
    assert single(deps, 14).level is doctor.Level.WARN

    script[FIREWALL_QUERY] = completed(firewall_block("是", "入", "允许", "Any", "8787").encode())
    result = single(deps, 14)
    assert result.level is doctor.Level.WARN
    assert "超出回环与 tailnet" in result.detail

    script[FIREWALL_QUERY] = completed(
        firewall_block("否", "入", "允许", "100.64.0.0/10,127.0.0.0/8", "8787").encode()
    )
    result = single(deps, 14)
    assert result.level is doctor.Level.WARN
    assert "未启用" in result.detail

    script[FIREWALL_QUERY] = completed(
        firewall_block("是", "入", "允许", "100.64.0.0/10,127.0.0.0/8", "9999").encode()
    )
    result = single(deps, 14)
    assert result.level is doctor.Level.WARN
    assert "8787" in result.detail


def test_exit_code_aggregation() -> None:
    def result(level: doctor.Level) -> doctor.CheckResult:
        return doctor.CheckResult(number=1, name="n", level=level, detail="")

    assert doctor.exit_code([result(doctor.Level.PASS)]) == 0
    assert doctor.exit_code([result(doctor.Level.PASS), result(doctor.Level.WARN)]) == 2
    mixed = [result(doctor.Level.PASS), result(doctor.Level.WARN), result(doctor.Level.FAIL)]
    assert doctor.exit_code(mixed) == 1


def test_run_doctor_prints_each_check_and_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """14 项逐条打印并给汇总；外部依赖换成假件，结论不随本机状态变化。"""
    make_database(tmp_path)
    (tmp_path / "app" / "current").mkdir(parents=True)
    fake = make_deps(tmp_path, api_address=("127.0.0.1", 1))
    real_collect = doctor.collect
    monkeypatch.setattr(
        doctor,
        "collect",
        lambda deps: real_collect(
            replace(
                deps,
                now=fake.now,
                registry_get=fake.registry_get,
                runner=fake.runner,
                disk_free=fake.disk_free,
                api_address=fake.api_address,
            )
        ),
    )
    stream = io.StringIO()
    code = doctor.run_doctor(tmp_path, app_root=tmp_path / "app", stream=stream)
    lines = stream.getvalue().splitlines()
    assert len(lines) == 15  # 14 项 + 汇总
    assert lines[-1].startswith("汇总：")
    # 库与目录健康，心跳、自动登录、计划任务等报 WARN：退出码 2
    assert code == 2
