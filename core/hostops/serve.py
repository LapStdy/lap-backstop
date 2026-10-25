"""serve 外壳：单实例启动序、Supervisor tick 循环、心跳与停机收尾。

启动序固定为互斥量→锁文件→数据库与迁移→启动扫描（docs/08 单实例一节）。
Windows 的 TerminateProcess 没有处理器可走，优雅停机靠可捕获的控制台信号
（SIGINT、SIGBREAK）；SIGTERM 一并注册，同一份代码在 POSIX 上也成立。
硬杀留下的锁文件不清理，由互斥量兜底、下次启动覆盖。
"""

from __future__ import annotations

import contextlib
import importlib.metadata
import os
import signal
import sqlite3
import subprocess
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType

from contracts import Clock, EventType, Logger, RunState
from core.__main__ import (
    EXIT_DISK_LOW,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_SECOND_INSTANCE,
    resolve_executable_path,
)
from core.clock import SystemClock
from core.hostops import heartbeat, instance, tasks, watchdog
from core.hostops.doctor import WINLOGON_KEY, RegistryGet, registry_get
from core.jobs.run_service import JobRunService
from core.logging import NullLogger
from core.plugins.registry import PluginRegistry
from core.supervisor import Supervisor
from storage import MigrationError, migrate, open_database
from storage.defaults import default
from storage.repositories.events import EventLog

TICK_INTERVAL_SECONDS = 1.0
AUTOLOGON_CHECKED_EVENT = "host.autologon_checked"
TASKS_REGISTERED_EVENT = "host.tasks_registered"
WATCHDOG_GAVE_UP_EVENT = "host.watchdog_gave_up"
_HEARTBEAT_INTERVAL = timedelta(seconds=int(default("host", "heartbeat_interval_seconds")))
_ACTIVE_RUN_STATES = frozenset(
    {RunState.PREPARING, RunState.WAITING, RunState.EXECUTING, RunState.VERIFYING}
)


@dataclass(frozen=True, slots=True)
class ServeOptions:
    """serve 的注入项：时钟、日志、停机事件与循环旋钮；生产路径全用默认值。"""

    clock: Clock = field(default_factory=SystemClock)
    log: Logger = field(default_factory=NullLogger)
    stop: threading.Event = field(default_factory=threading.Event)
    max_ticks: int | None = None
    tick_interval_seconds: float = TICK_INTERVAL_SECONDS
    register_tasks: bool = True
    task_runner: tasks.Runner = field(default_factory=lambda: subprocess.run)
    registry_get: RegistryGet = field(default_factory=lambda: registry_get)


def install_stop_handlers() -> threading.Event:
    """注册停机信号并返回停机事件；事件置位后 tick 循环在当前轮结束处退出。

    Returns:
        停机事件；测试可直接注入自己的事件而不装信号处理器。
    """
    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    for name in ("SIGBREAK", "SIGINT", "SIGTERM"):
        number = getattr(signal, name, None)
        if number is not None:
            signal.signal(number, request_stop)
    return stop


def executable_version() -> str:
    """发行包版本号；源码直跑（环境里未安装发行包）记 ``dev``。"""
    try:
        return importlib.metadata.version("lap-backstop")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


@dataclass(frozen=True, slots=True)
class HeartbeatWriter:
    """心跳写入件：进程身份与目标路径固定，每次写取当前时刻与在途运行。"""

    path: Path
    started_at: datetime
    pid: int
    version: str

    def write(self, *, last_tick_at: datetime, active_run: str | None) -> None:
        """原子写一次心跳。

        Raises:
            OSError: 目录不可写或替换失败；由调用方决定重试。
        """
        heartbeat.write_heartbeat(
            self.path,
            {
                "pid": self.pid,
                "started_at": self.started_at.isoformat(),
                "version": self.version,
                "last_tick": last_tick_at.isoformat(),
                "active_run": active_run,
            },
        )


class ServeLoop:
    """tick 循环：每轮推进一次 Supervisor，到心跳间隔写一次心跳。"""

    def __init__(
        self,
        supervisor: Supervisor,
        runs: JobRunService,
        writer: HeartbeatWriter,
        options: ServeOptions,
    ) -> None:
        self._supervisor = supervisor
        self._runs = runs
        self._writer = writer
        self._options = options
        self._last_heartbeat_at: datetime | None = None

    def run(self) -> int:
        """循环到停机信号、tick 上限或磁盘 critical；正常路径返回 0，磁盘水位返回 5。"""
        ticks = 0
        while not self._options.stop.is_set() and (
            self._options.max_ticks is None or ticks < self._options.max_ticks
        ):
            self._supervisor.tick()
            if self._supervisor.is_disk_critical:
                return EXIT_DISK_LOW
            ticks += 1
            self._write_heartbeat_if_due()
            self._options.stop.wait(self._options.tick_interval_seconds)
        return EXIT_OK

    def _write_heartbeat_if_due(self) -> None:
        """到点写心跳；写失败保留上次时刻，下一轮立刻重试。"""
        last_tick_at = self._options.clock.utc_now()
        if (
            self._last_heartbeat_at is not None
            and last_tick_at - self._last_heartbeat_at < _HEARTBEAT_INTERVAL
        ):
            return
        try:
            self._writer.write(last_tick_at=last_tick_at, active_run=_active_run(self._runs))
        except OSError as error:
            self._options.log.error({"event": "serve.heartbeat_write_failed", "error": str(error)})
            return
        self._last_heartbeat_at = last_tick_at


def _active_run(runs: JobRunService) -> str | None:
    """当前在途运行的 id（准备、等待、执行、验证都算）；没有则 None。"""
    for run in runs.non_terminal():
        if run.state in _ACTIVE_RUN_STATES:
            return run.id
    return None


def _host_setup_events(
    data_root: Path,
    options: ServeOptions,
    *,
    at: datetime,
) -> list[tuple[str, str]]:
    """宿主启动动作的结果事件：看门狗停手补报、自动登录自检、任务注册。

    三件事都尽力而为，结果进事件 detail；单项失败不阻断核心启动
    （docs/08 自动登录自检与计划任务两节）。
    """
    events: list[tuple[str, str]] = []
    gave_up = watchdog.claim_gave_up_report(data_root / "run" / watchdog.STATE_FILENAME)
    if gave_up is not None:
        events.append((WATCHDOG_GAVE_UP_EVENT, gave_up))
    logon = options.registry_get(WINLOGON_KEY, "AutoAdminLogon")
    user = options.registry_get(WINLOGON_KEY, "DefaultUserName")
    if logon == "1" and user:
        events.append((AUTOLOGON_CHECKED_EVENT, f"AutoAdminLogon=1，DefaultUserName={user}"))
    else:
        events.append(
            (
                AUTOLOGON_CHECKED_EVENT,
                f"AutoAdminLogon={logon}，DefaultUserName={user}；重启后会停在登录界面",
            )
        )
    if options.register_tasks:
        _ok, detail = tasks.register_registered_tasks(
            data_root,
            runner=options.task_runner,
            registered_at=at,
        )
        events.append((TASKS_REGISTERED_EVENT, detail))
    return events


def _record_events(
    connection: sqlite3.Connection,
    events: Iterable[tuple[str, str]],
    *,
    at: datetime,
) -> None:
    """把启动事件作为全局核心事件落库；写不进说明库有故障，由调用方处置。"""
    log = EventLog(connection)
    for event, detail in events:
        log.append_global(EventType.CORE_EVENT, {"event": event, "detail": detail}, at=at)


def run_serve(data_root: Path, options: ServeOptions | None = None) -> int:
    """serve 主流程，返回进程退出码。

    启动序：互斥量（被占写事件退 3）→ 锁文件（写不了退 1）→ 数据库与迁移 →
    启动扫描 → tick 循环。收尾永远删锁文件并释放互斥量。
    """
    active = ServeOptions() if options is None else options
    started_at = active.clock.utc_now()
    lock_path = data_root / "run" / instance.LOCK_FILENAME
    handle = instance.acquire_named_mutex(instance.MUTEX_NAME)
    if handle is None:
        instance.report_second_instance(data_root / "backstop.sqlite", at=started_at)
        active.log.warn({"event": "serve.second_instance"})
        return EXIT_SECOND_INSTANCE
    try:
        if not _write_lock(lock_path, started_at, active.log):
            return EXIT_ERROR
        return _run_core(data_root, active, started_at)
    finally:
        with contextlib.suppress(OSError):
            lock_path.unlink(missing_ok=True)
        instance.release_named_mutex(handle)


def _write_lock(lock_path: Path, started_at: datetime, log: Logger) -> bool:
    """写锁文件；目录不可写时记指向权限的错误并返回 False。"""
    boot_id = instance.read_boot_id()
    if not boot_id:
        log.warn({"event": "serve.boot_id_unavailable"})
    try:
        instance.write_lock_file(
            lock_path,
            {
                "pid": os.getpid(),
                "started_at": started_at.isoformat(),
                "exe_path": resolve_executable_path(),
                "boot_id": boot_id,
            },
        )
    except OSError as error:
        log.error(
            {
                "event": "serve.lock_write_failed",
                "error": str(error),
                "path": str(lock_path),
                "hint": f"检查 {lock_path.parent} 目录写权限",
            }
        )
        return False
    return True


def _run_core(data_root: Path, options: ServeOptions, started_at: datetime) -> int:
    """库、迁移、启动扫描与 tick 循环；打开失败与运行期错误都记日志退 1。"""
    connection: sqlite3.Connection | None = None
    try:
        connection = open_database(data_root / "backstop.sqlite")
        migrate(connection)
        supervisor = Supervisor(
            connection,
            options.clock,
            PluginRegistry(),
            data_root=data_root,
            log=options.log,
        )
        supervisor.startup()
        setup_at = options.clock.utc_now()
        events = _host_setup_events(data_root, options, at=setup_at)
        _record_events(connection, events, at=setup_at)
        writer = HeartbeatWriter(
            path=data_root / "run" / heartbeat.HEARTBEAT_FILENAME,
            started_at=started_at,
            pid=os.getpid(),
            version=executable_version(),
        )
        loop = ServeLoop(supervisor, JobRunService(connection, options.clock), writer, options)
        return loop.run()
    except (OSError, sqlite3.Error, MigrationError) as error:
        options.log.error({"event": "serve.run_failed", "error": str(error)})
        return EXIT_ERROR
    finally:
        if connection is not None:
            connection.close()


__all__ = [
    "AUTOLOGON_CHECKED_EVENT",
    "TASKS_REGISTERED_EVENT",
    "TICK_INTERVAL_SECONDS",
    "WATCHDOG_GAVE_UP_EVENT",
    "HeartbeatWriter",
    "ServeLoop",
    "ServeOptions",
    "executable_version",
    "install_stop_handlers",
    "run_serve",
]
