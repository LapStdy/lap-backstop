"""serve 外壳：单实例启动序、Supervisor tick 循环、心跳与停机收尾。

启动序固定为互斥量→锁文件→数据库与迁移→启动扫描（docs/08 单实例一节）。
Windows 的 TerminateProcess 没有处理器可走，优雅停机靠可捕获的控制台信号
（SIGINT、SIGBREAK）；SIGTERM 一并注册，同一份代码在 POSIX 上也成立。
硬杀留下的锁文件不清理，由互斥量兜底、下次启动覆盖。
"""

from __future__ import annotations

import contextlib
import errno
import os
import signal
import sqlite3
import subprocess
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any

from contracts import Clock, EventType, Logger, RunState
from core.__main__ import (
    EXIT_DISK_LOW,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_PORT_CONFLICT,
    EXIT_SECOND_INSTANCE,
    resolve_executable_path,
)
from core.api.server import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    ApiOptions,
    ApiServer,
    start_api_server,
)
from core.api.store import all_settings, bearer_token, ensure_token
from core.api.version import executable_version
from core.clock import SystemClock
from core.hostops import heartbeat, instance, tailscale, tasks, watchdog
from core.hostops.doctor import WINLOGON_KEY, RegistryGet, registry_get
from core.jobs.run_service import JobRunService
from core.logging import NullLogger
from core.notify.service import NotifyService
from core.plugins.registry import PluginRegistry
from core.supervisor import Supervisor
from storage import MigrationError, migrate, open_database
from storage.defaults import default
from storage.repositories.events import EventLog

TICK_INTERVAL_SECONDS = 1.0
AUTOLOGON_CHECKED_EVENT = "host.autologon_checked"
TASKS_REGISTERED_EVENT = "host.tasks_registered"
# 事件名的属主是 watchdog 模块；这里保留同名导出，供既有引用方使用。
WATCHDOG_GAVE_UP_EVENT = watchdog.WATCHDOG_GAVE_UP_EVENT
PORT_CONFLICT_EVENT = "core.port_conflict"
API_HOST_KEY = "api.host"
API_PORT_KEY = "api.port"
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
    tailscale_runner: tailscale.Runner = field(default_factory=lambda: tailscale.default_runner)
    on_api_listening: Callable[[str, int], None] | None = None


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
    """tick 循环：每轮推进一次 Supervisor，追一次通知，到心跳间隔写一次心跳。"""

    def __init__(
        self,
        supervisor: Supervisor,
        runs: JobRunService,
        writer: HeartbeatWriter,
        options: ServeOptions,
        notify: NotifyService | None = None,
    ) -> None:
        self._supervisor = supervisor
        self._runs = runs
        self._writer = writer
        self._options = options
        self._notify = notify
        self._last_heartbeat_at: datetime | None = None

    def run(self) -> int:
        """循环到停机信号、tick 上限或磁盘 critical；正常路径返回 0，磁盘水位返回 5。"""
        ticks = 0
        while not self._options.stop.is_set() and (
            self._options.max_ticks is None or ticks < self._options.max_ticks
        ):
            self._supervisor.tick()
            self._poll_notify()
            if self._supervisor.is_disk_critical:
                return EXIT_DISK_LOW
            ticks += 1
            self._write_heartbeat_if_due()
            self._options.stop.wait(self._options.tick_interval_seconds)
        return EXIT_OK

    def _poll_notify(self) -> None:
        """通知轮询：任何失败只记日志，通知故障不拖垮核心循环。"""
        if self._notify is None:
            return
        try:
            self._notify.poll()
        except Exception as error:
            self._options.log.error({"event": "notify.poll_failed", "error": str(error)})

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
    event_type: EventType = EventType.CORE_EVENT,
) -> None:
    """把启动事件作为全局事件落库；写不进说明库有故障，由调用方处置。"""
    log = EventLog(connection)
    for event, detail in events:
        log.append_global(event_type, {"event": event, "detail": detail}, at=at)


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
    """库、迁移、启动扫描、API 挂载与 tick 循环；打开失败与运行期错误都记日志退 1。"""
    connection: sqlite3.Connection | None = None
    api_server: ApiServer | None = None
    notify: NotifyService | None = None
    try:
        connection = open_database(data_root / "backstop.sqlite")
        migrate(connection)
        # 通知游标在启动扫描与启动事件落库之前定桩：崩溃收尾（运行置 unknown）与
        # 看门狗停手补报都要进通知，重启不能漏报。
        notify = NotifyService(connection, options.clock, data_root=data_root, log=options.log)
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
        api_server, exit_code = _start_api(connection, options)
        if exit_code is not None:
            return exit_code
        writer = HeartbeatWriter(
            path=data_root / "run" / heartbeat.HEARTBEAT_FILENAME,
            started_at=started_at,
            pid=os.getpid(),
            version=executable_version(),
        )
        loop = ServeLoop(
            supervisor,
            JobRunService(connection, options.clock),
            writer,
            options,
            notify=notify,
        )
        return loop.run()
    except (OSError, sqlite3.Error, MigrationError) as error:
        options.log.error({"event": "serve.run_failed", "error": str(error)})
        return EXIT_ERROR
    finally:
        if notify is not None:
            notify.close()
        if api_server is not None:
            api_server.shutdown()
        if connection is not None:
            connection.close()


@dataclass(frozen=True, slots=True)
class ListenPlan:
    """一次启动的监听计划：主地址、端口与追加监听的 tailnet 地址。"""

    host: str
    port: int
    extra_hosts: tuple[str, ...] = ()


def _start_api(
    connection: sqlite3.Connection,
    options: ServeOptions,
) -> tuple[ApiServer | None, int | None]:
    """按 settings 挂载 API：无 token 先自生成，端口被占退 4，tailnet 绑定失败降级。"""
    _ensure_token_logged(connection, options)
    try:
        settings = all_settings(connection)
        host, port = _api_listen(settings)
        extra_hosts, down_detail = tailscale.remote_listen_plan(
            settings, options.tailscale_runner, options.log
        )
    except ValueError as error:
        options.log.error({"event": "serve.api_config_invalid", "error": str(error)})
        return None, EXIT_ERROR
    if down_detail is not None:
        _record_events(
            connection,
            [(tailscale.TAILSCALE_DOWN_EVENT, down_detail)],
            at=options.clock.utc_now(),
            event_type=EventType.HOST_EVENT,
        )
        options.log.warn({"event": "serve.tailscale_down", "detail": down_detail})
    # api.host 直接配成 tailnet 地址时不再重复追加，避免同一地址绑两次。
    plan = tailscale.ListenPlan(host, port, tuple(extra for extra in extra_hosts if extra != host))
    server = _bind_api(connection, options, plan)
    if server is None:
        return None, EXIT_PORT_CONFLICT
    tailscale.record_bind_degradation(
        connection, failures=server.bind_failures, at=options.clock.utc_now()
    )
    bound_hosts = {bound_host for bound_host, _bound_port in server.addresses}
    options.log.info(
        {
            "event": "serve.api_listening",
            "host": plan.host,
            "port": plan.port,
            "extra_hosts": [extra for extra in plan.extra_hosts if extra in bound_hosts],
        }
    )
    if options.on_api_listening is not None:
        options.on_api_listening(*server.address)
    return server, None


def _ensure_token_logged(connection: sqlite3.Connection, options: ServeOptions) -> None:
    """无可用 token 时生成并落库；日志只记已生成，不记值（docs/11）。"""
    if bearer_token(connection) is not None:
        return
    ensure_token(connection, at=options.clock.utc_now())
    options.log.info({"event": "serve.token_generated", "detail": "已生成访问令牌"})


def _bind_api(
    connection: sqlite3.Connection,
    options: ServeOptions,
    plan: tailscale.ListenPlan,
) -> ApiServer | None:
    """启动 API 服务；主地址端口被占写 core.port_conflict 返回 None，其余 OSError 上抛。"""
    try:
        return start_api_server(
            connection,
            ApiOptions(
                host=plan.host,
                port=plan.port,
                extra_hosts=plan.extra_hosts,
                clock=options.clock,
                log=options.log,
                stop=options.stop,
            ),
        )
    except OSError as error:
        # docs/08 的退出码 4 只针对端口被占；地址不可达等其它绑定失败走通用失败路径。
        if error.errno != errno.EADDRINUSE:
            raise
        _record_events(
            connection,
            [(PORT_CONFLICT_EVENT, f"{plan.host}:{plan.port} 已被其它进程监听：{error}")],
            at=options.clock.utc_now(),
        )
        options.log.error(
            {
                "event": "serve.port_conflict",
                "host": plan.host,
                "port": plan.port,
                "error": str(error),
            }
        )
        return None


def _api_listen(settings: Mapping[str, Any]) -> tuple[str, int]:
    """读监听地址设置；缺项用默认值，取值非法按坏配置拒绝启动并指出键名。

    Raises:
        ValueError: api.host 或 api.port 的类型或取值范围不合法。
    """
    host = settings.get(API_HOST_KEY, DEFAULT_HOST)
    port = settings.get(API_PORT_KEY, DEFAULT_PORT)
    if not isinstance(host, str) or not host:
        raise ValueError(f"{API_HOST_KEY} 必须是非空字符串")
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65535:
        raise ValueError(f"{API_PORT_KEY} 必须是 0 到 65535 的整数")
    return host, port


__all__ = [
    "AUTOLOGON_CHECKED_EVENT",
    "PORT_CONFLICT_EVENT",
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
