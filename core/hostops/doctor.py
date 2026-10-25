"""doctor 自检：13 项宿主环境检查，只报告不修复（docs/08 排障入口）。

外部依赖全注入，单元测试不碰真实注册表与 schtasks；退出码 0 全过、1 有 FAIL、2 仅 WARN。
"""

from __future__ import annotations

import json
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import winreg
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TextIO

from core.hostops import instance, tasks
from core.hostops.heartbeat import HEARTBEAT_FILENAME, heartbeat_age_seconds, read_heartbeat
from storage.db.migrations import MIGRATIONS
from storage.defaults import DEFAULTS, load_defaults

DATABASE_FILENAME = "backstop.sqlite"
DEFAULT_API_ADDRESS = ("127.0.0.1", 8787)
WARN_BYTES = 5 * 1024**3
FAIL_BYTES = 2 * 1024**3
# 排障入口可以比看门狗宽裕，但仍必须有界（docs/11 每个外部调用都必须带超时）。
_EXTERNAL_TIMEOUT_SECONDS = 10.0
WINLOGON_KEY = "SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Winlogon"

Runner = Callable[[list[str]], subprocess.CompletedProcess[bytes]]
RegistryGet = Callable[[str, str], str | None]


def registry_get(key_path: str, value_name: str) -> str | None:
    """读 HKLM 字符串值；键不存在或类型不符返回 None。"""
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path) as key:
            value, _kind = winreg.QueryValueEx(key, value_name)
    except OSError:
        return None
    return value if isinstance(value, str) else None


class Level(StrEnum):
    """检查结果级别：PASS 通过、WARN 提醒、FAIL 恢复链路已断。"""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass(frozen=True, slots=True)
class CheckResult:
    """一项检查的结论；编号与检查项对应 doctor 设计清单。"""

    number: int
    name: str
    level: Level
    detail: str


@dataclass(frozen=True, slots=True)
class DoctorDeps:
    """检查的注入项；生产用默认实现，测试给假件。"""

    data_root: Path
    app_root: Path
    now: datetime = field(default_factory=lambda: datetime.now(UTC))
    registry_get: RegistryGet = field(default_factory=lambda: registry_get)
    runner: Runner = field(default_factory=lambda: _run)
    disk_free: Callable[[Path], int | None] = field(default_factory=lambda: _disk_free)
    api_address: tuple[str, int] = DEFAULT_API_ADDRESS


def run_doctor(
    data_root: Path, *, app_root: Path | None = None, stream: TextIO = sys.stdout
) -> int:
    """跑全部检查、打印逐项与汇总，返回退出码。"""
    root = app_root if app_root is not None else data_root.parent / "app"
    results = collect(DoctorDeps(data_root=data_root, app_root=root))
    for result in results:
        print(f"{result.level} {result.number} {result.name} {result.detail}", file=stream)
    passed = sum(1 for result in results if result.level is Level.PASS)
    warned = sum(1 for result in results if result.level is Level.WARN)
    failed = sum(1 for result in results if result.level is Level.FAIL)
    print(f"汇总：PASS {passed}，WARN {warned}，FAIL {failed}", file=stream)
    return exit_code(results)


def exit_code(results: list[CheckResult]) -> int:
    """聚合退出码：0 全过、1 有 FAIL、2 仅 WARN。"""
    if any(result.level is Level.FAIL for result in results):
        return 1
    if any(result.level is Level.WARN for result in results):
        return 2
    return 0


def collect(deps: DoctorDeps) -> list[CheckResult]:
    """按编号顺序跑 13 项检查。"""
    checks = [
        _check_data_writable,
        _check_database_integrity,
        _check_migration_version,
        _check_defaults,
        _check_app_current,
        _check_port_free,
        _check_disk_watermark,
        _check_heartbeat_fresh,
        _check_lock_and_mutex,
        _check_autologon,
        _check_scheduled_tasks,
        _check_time_service,
        _check_tailscale_and_power,
    ]
    return [check(deps) for check in checks]


def _check_data_writable(deps: DoctorDeps) -> CheckResult:
    """第 1 项：data 目录可写；恢复链路的第一步。"""
    probe = deps.data_root / ".doctor-probe"
    try:
        deps.data_root.mkdir(parents=True, exist_ok=True)
        probe.write_text("", encoding="utf-8")
        probe.unlink()
    except OSError as error:
        return _result(1, "数据目录可写", Level.FAIL, f"不可写：{error}")
    return _result(1, "数据目录可写", Level.PASS, str(deps.data_root))


def _readonly_query(database: Path, sql: str) -> str:
    """只读查询单值；打不开或查询失败抛 sqlite3.Error，由调用方分级。"""
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        row = connection.execute(sql).fetchone()
    finally:
        connection.close()
    return str(row[0]) if row is not None else ""


def _check_database_integrity(deps: DoctorDeps) -> CheckResult:
    """第 2 项：库可打开且 integrity_check 为 ok（docs/08 恢复顺序的 db check）。"""
    database = deps.data_root / DATABASE_FILENAME
    try:
        verdict = _readonly_query(database, "PRAGMA integrity_check")
    except sqlite3.Error as error:
        return _result(2, "数据库完整性", Level.FAIL, f"打不开或检查失败：{error}")
    if verdict != "ok":
        return _result(2, "数据库完整性", Level.FAIL, f"integrity_check={verdict}")
    return _result(2, "数据库完整性", Level.PASS, str(database))


def _check_migration_version(deps: DoctorDeps) -> CheckResult:
    """第 3 项：库迁移版本与代码一致。"""
    database = deps.data_root / DATABASE_FILENAME
    expected = MIGRATIONS[-1].version
    try:
        version = int(_readonly_query(database, "PRAGMA user_version") or "0")
    except sqlite3.Error as error:
        return _result(3, "迁移版本", Level.FAIL, f"打不开库或读版本失败：{error}")
    if version != expected:
        return _result(3, "迁移版本", Level.FAIL, f"库为 {version}，代码为 {expected}")
    return _result(3, "迁移版本", Level.PASS, f"user_version={version}")


def _check_defaults(deps: DoctorDeps) -> CheckResult:
    """第 4 项：默认值文件可加载、结构合法且 [host] 段齐全。"""
    try:
        data = load_defaults()
    except (OSError, ValueError) as error:
        return _result(4, "默认值基线", Level.FAIL, f"加载失败：{error}")
    host = data.get("host")
    required = (
        "heartbeat_interval_seconds",
        "stale_seconds",
        "acceptance_seconds",
        "give_up_window_minutes",
        "give_up_max_failures",
    )
    if not isinstance(host, dict) or any(key not in host for key in required):
        return _result(4, "默认值基线", Level.FAIL, "config/defaults.toml 缺 [host] 段或键不全")
    return _result(4, "默认值基线", Level.PASS, f"{len(DEFAULTS)} 个配置段")


def _check_app_current(deps: DoctorDeps) -> CheckResult:
    r"""第 5 项：app\current 存在且指向有效版本目录。"""
    current = deps.app_root / "current"
    if not current.is_dir():
        detail = f"{current} 不存在，计划任务与看门狗拉起都指向它"
        return _result(5, "当前版本目录", Level.FAIL, detail)
    return _result(5, "当前版本目录", Level.PASS, str(current.resolve()))


def _check_port_free(deps: DoctorDeps) -> CheckResult:
    """第 6 项：API 端口未被任何进程监听（P0 只查不绑）。"""
    host, port = deps.api_address
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        occupied = probe.connect_ex((host, port)) == 0
    if occupied:
        return _result(6, "API 端口", Level.FAIL, f"{host}:{port} 已被占用")
    return _result(6, "API 端口", Level.PASS, f"{host}:{port} 空闲")


def _check_disk_watermark(deps: DoctorDeps) -> CheckResult:
    """第 7 项：data 所在卷可用空间水位（docs/08 磁盘一节）。"""
    free = deps.disk_free(deps.data_root)
    if free is None:
        return _result(7, "磁盘水位", Level.FAIL, "查不到可用空间")
    gib = free / 1024**3
    if free < FAIL_BYTES:
        return _result(7, "磁盘水位", Level.FAIL, f"可用 {gib:.1f} GiB，低于 2 GiB")
    if free < WARN_BYTES:
        return _result(7, "磁盘水位", Level.WARN, f"可用 {gib:.1f} GiB，低于 5 GiB")
    return _result(7, "磁盘水位", Level.PASS, f"可用 {gib:.1f} GiB")


def _check_heartbeat_fresh(deps: DoctorDeps) -> CheckResult:
    """第 8 项：心跳文件新鲜；核心在跑时才应 PASS。"""
    path = deps.data_root / "run" / HEARTBEAT_FILENAME
    content = read_heartbeat(path)
    age = heartbeat_age_seconds(path, now=deps.now.timestamp())
    stale = float(DEFAULTS["host"]["stale_seconds"])
    if content is not None and age is not None and age < stale:
        return _result(8, "核心心跳", Level.PASS, f"age={age:.0f}s，pid={content['pid']}")
    return _result(8, "核心心跳", Level.WARN, "核心未运行或心跳已停")


def _check_lock_and_mutex(deps: DoctorDeps) -> CheckResult:
    """第 9 项：锁文件与互斥量状态对得上。"""
    lock_path = deps.data_root / "run" / instance.LOCK_FILENAME
    lock = instance.read_lock_file(lock_path)
    if lock is None:
        return _result(9, "单实例残留", Level.PASS, "无锁文件")
    if instance.is_pid_alive(lock["pid"]):
        handle = instance.acquire_named_mutex(instance.MUTEX_NAME)
        if handle is None:
            return _result(9, "单实例残留", Level.PASS, f"核心在跑（pid={lock['pid']}）")
        instance.release_named_mutex(handle)
        return _result(
            9,
            "单实例残留",
            Level.WARN,
            f"锁内 pid 活着但互斥量空闲（pid={lock['pid']}）",
        )
    return _result(9, "单实例残留", Level.WARN, f"锁文件残留，pid={lock['pid']} 已退出，可删除")


def _check_autologon(deps: DoctorDeps) -> CheckResult:
    """第 10 项：自动登录注册表配好（重启后无人按键的起点）。"""
    logon = deps.registry_get(WINLOGON_KEY, "AutoAdminLogon")
    user = deps.registry_get(WINLOGON_KEY, "DefaultUserName")
    if logon == "1" and user == "backstop":
        return _result(10, "自动登录", Level.PASS, "AutoAdminLogon=1，用户 backstop")
    detail = f"AutoAdminLogon={logon}，DefaultUserName={user}；重启后会停在登录界面"
    return _result(10, "自动登录", Level.WARN, detail)


def _check_scheduled_tasks(deps: DoctorDeps) -> CheckResult:
    """第 11 项：计划任务已注册且定义文件在位。

    schtasks 实际注册与登录触发按 docs/12 手工核对；这里查注册存在性，
    已注册任务的动作与本地 XML 的动作字段一致。preheat 与 backup 随
    API 里程碑启用，缺失只提示不算可修复项。
    """
    tasks_dir = deps.data_root / tasks.TASKS_DIRNAME
    fixable: list[str] = []
    upcoming: list[str] = []
    drifted: list[str] = []
    names = (tasks.TASK_CORE, tasks.TASK_WATCHDOG, tasks.TASK_PREHEAT, tasks.TASK_BACKUP)
    for name in names:
        completed = deps.runner(["schtasks", "/query", "/tn", name])
        local = tasks_dir / f"{name}.xml"
        p0 = name in tasks.REGISTERED_TASKS
        if completed.returncode != 0 or not local.is_file():
            label = name if completed.returncode != 0 else f"{name}（无定义文件）"
            (fixable if p0 else upcoming).append(label)
        elif p0 and not _registered_action_matches(completed, local):
            drifted.append(name)
    problems = fixable + [f"{name} 动作漂移" for name in drifted]
    if not problems and not upcoming:
        return _result(11, "计划任务", Level.PASS, "四个任务在位")
    parts = ["、".join(problems)] if problems else []
    if upcoming:
        parts.append("、".join(upcoming) + " 随后续里程碑启用")
    detail = "；".join(parts)
    if problems:
        detail += "；P0 任务跑 lap-backstop tasks register 修复"
    return _result(11, "计划任务", Level.WARN, detail)


def _check_time_service(deps: DoctorDeps) -> CheckResult:
    """第 12 项：Windows Time 在运行且 w32tm 状态可查。"""
    service = deps.runner(["sc", "query", "w32time"])
    if service.returncode != 0 or b"RUNNING" not in service.stdout:
        return _result(12, "时间服务", Level.WARN, "w32time 未运行，时钟漂移会干扰判死")
    status = deps.runner(["w32tm", "/query", "/status"])
    if status.returncode != 0:
        return _result(12, "时间服务", Level.WARN, "w32tm /query /status 失败，未与时间源同步")
    return _result(12, "时间服务", Level.PASS, "w32time 运行中")


def _check_tailscale_and_power(deps: DoctorDeps) -> CheckResult:
    """第 13 项：Tailscale 在 Running、交流供电下睡眠与休眠超时为 0。"""
    tailscale = deps.runner(["tailscale", "status", "--json"])
    state = ""
    if tailscale.returncode == 0:
        try:
            text = tailscale.stdout.decode("utf-8", errors="replace")
            state = str(json.loads(text).get("BackendState", ""))
        except ValueError:
            state = ""
    problems: list[str] = []
    if state != "Running":
        problems.append(f"Tailscale BackendState={state or '不可查'}")
    standby = _ac_timeout(deps, "STANDBYIDLE")
    hibernate = _ac_timeout(deps, "HIBERNATEIDLE")
    if standby is None or standby != 0:
        problems.append(f"交流睡眠超时={standby if standby is not None else '不可查'}")
    if hibernate is None or hibernate != 0:
        problems.append(f"交流休眠超时={hibernate if hibernate is not None else '不可查'}")
    if problems:
        return _result(13, "远程访问与电源", Level.WARN, "；".join(problems))
    return _result(13, "远程访问与电源", Level.PASS, "Tailscale Running，交流下不睡眠不休眠")


def _registered_action_matches(completed: subprocess.CompletedProcess[bytes], local: Path) -> bool:
    """已注册任务的输出与本地定义的动作字段是否一致（注册输出编码容错）。"""
    try:
        registered = completed.stdout.decode("utf-16")
    except UnicodeDecodeError:
        registered = completed.stdout.decode("utf-8", errors="replace")
    return _action_text(registered) == _action_text(local.read_text(encoding="utf-16"))


def _action_text(task_xml: str) -> str:
    """抽取任务的命令与参数字段，作为一致性比对的最小口径。"""
    command = re.search(r"<Command>(.*?)</Command>", task_xml, re.DOTALL)
    arguments = re.search(r"<Arguments>(.*?)</Arguments>", task_xml, re.DOTALL)
    return f"{command.group(1) if command else ''}|{arguments.group(1) if arguments else ''}"


def _ac_timeout(deps: DoctorDeps, setting: str) -> int | None:
    """交流电源下某睡眠类超时的分钟值；powercfg 输出本地化，解析失败返回 None。"""
    completed = deps.runner(["powercfg", "/q", "SCHEME_CURRENT", "SUB_SLEEP", setting])
    if completed.returncode != 0:
        return None
    text = completed.stdout.decode("utf-8", errors="replace")
    patterns = (
        r"交流电源设置索引.*?0x([0-9a-fA-F]+)",
        r"AC Power Setting Index.*?0x([0-9a-fA-F]+)",
    )
    for pattern in patterns:
        found = re.search(pattern, text, re.DOTALL)
        if found is not None:
            return int(found.group(1), 16)
    return None


def _result(number: int, name: str, level: Level, detail: str) -> CheckResult:
    return CheckResult(number=number, name=name, level=level, detail=detail)


def _run(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    """跑外部命令；可执行体不存在或超时按退出码 -1 处理，不炸整个 doctor。"""
    try:
        return subprocess.run(
            command,
            capture_output=True,
            check=False,
            timeout=_EXTERNAL_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return subprocess.CompletedProcess(command, -1, b"", b"")


def _disk_free(path: Path) -> int | None:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return None


__all__ = [
    "WINLOGON_KEY",
    "CheckResult",
    "DoctorDeps",
    "Level",
    "collect",
    "exit_code",
    "registry_get",
    "run_doctor",
]
