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
from core.api.hostops_api import DrainRequester
from core.api.runs_api import RunStopper
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
from core.environments.actions import EnvironmentActions
from core.events.maintenance import GlobalEventsPruner
from core.hostops import heartbeat, instance, tailscale, tasks, timesync, watchdog
from core.hostops.doctor import DATABASE_FILENAME, WINLOGON_KEY, RegistryGet, registry_get
from core.hostops.environment_actions import EnvironmentActionService
from core.hostops.retention import RetentionRunner
from core.jobs.run_service import JobRunService
from core.logging import NullLogger
from core.notify.service import NotifyService
from core.plugins.rpc_host import RpcPluginHost, installed_plugin_ids
from core.supervisor import Supervisor
from storage import BackupError, MigrationError, migrate, open_database
from storage.defaults import default
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRecord

TICK_INTERVAL_SECONDS = 1.0
AUTOLOGON_CHECKED_EVENT = "host.autologon_checked"
# 事件名的属主是 tasks 模块；这里保留同名导出，供既有引用方使用。
TASKS_REGISTERED_EVENT = tasks.TASKS_REGISTERED_EVENT
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
    # 校时真的改宿主时钟：进程内测试关掉这个开关，避免 pytest 动系统时间（docs/08）。
    run_timesync: bool = True
    # 插件根缺省与 data 同级（docs/08 路径布局）；测试指向只含 fake 的临时目录。
    plugins_root: Path | None = None
    task_runner: tasks.Runner = field(default_factory=lambda: subprocess.run)
    # 校时命令的执行器（docs/08 时钟与 NTP）；测试替换为假件，不碰真实 w32tm。
    timesync_runner: timesync.Runner = field(default_factory=lambda: subprocess.run)
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
    """tick 循环：每轮推进一次 Supervisor、追一次通知、按日闸门清一次过期数据，到心跳间隔写心跳。"""

    def __init__(
        self,
        supervisor: Supervisor,
        runs: JobRunService,
        writer: HeartbeatWriter,
        options: ServeOptions,
        notify: NotifyService | None = None,
        pruner: GlobalEventsPruner | None = None,
        retention: RetentionRunner | None = None,
    ) -> None:
        """装配循环部件；协作对象传 None 即关闭对应侧工作。

        构造参数超过上限 4 属评审已说明的例外：supervisor、runs、writer、
        options 是循环的既定部件，notify、pruner 与 retention 是可选的协作对象，
        测试按需注入，语义互不重叠。
        """
        self._supervisor = supervisor
        self._runs = runs
        self._writer = writer
        self._options = options
        self._notify = notify
        self._pruner = pruner
        self._retention = retention
        self._last_heartbeat_at: float | None = None

    def run(self) -> int:
        """循环到停机信号、tick 上限或磁盘 critical；正常路径返回 0，磁盘水位返回 5。"""
        ticks = 0
        while not self._options.stop.is_set() and (
            self._options.max_ticks is None or ticks < self._options.max_ticks
        ):
            self._supervisor.tick()
            self._poll_notify()
            self._prune_global_events()
            self._prune_retention()
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

    def _prune_global_events(self) -> None:
        """全局事件保留清理：任何失败只记日志，维护故障不拖垮核心循环。"""
        if self._pruner is None:
            return
        try:
            self._pruner.run_once()
        except Exception as error:
            self._options.log.error(
                {"event": "maintenance.global_events_prune_failed", "error": str(error)}
            )

    def _prune_retention(self) -> None:
        """过期数据保留清理：文件或库失败只记日志，当天不推进，下个 tick 重试。"""
        if self._retention is None:
            return
        try:
            self._retention.run_once()
        except (OSError, sqlite3.Error) as error:
            self._options.log.error(
                {"event": "maintenance.retention_prune_failed", "error": str(error)}
            )

    def _write_heartbeat_if_due(self) -> None:
        """到点写心跳；写失败保留上次时刻，下一轮立刻重试。

        间隔按单调时钟判定，校时与休眠恢复造成的墙钟跳变不会让心跳停发或连发；
        心跳内容里的 last_tick 仍是墙钟 UTC（看门狗按文件 mtime 算年龄）。
        """
        now_mono = self._options.clock.monotonic()
        last_tick_at = self._options.clock.utc_now()
        if (
            self._last_heartbeat_at is not None
            and now_mono - self._last_heartbeat_at < _HEARTBEAT_INTERVAL.total_seconds()
        ):
            return
        try:
            self._writer.write(last_tick_at=last_tick_at, active_run=_active_run(self._runs))
        except OSError as error:
            self._options.log.error({"event": "serve.heartbeat_write_failed", "error": str(error)})
            return
        self._last_heartbeat_at = now_mono


def _active_runs(runs: JobRunService) -> list[JobRunRecord]:
    """在途运行清单：准备、等待、执行、验证都算。"""
    return [run for run in runs.non_terminal() if run.state in _ACTIVE_RUN_STATES]


def _active_run(runs: JobRunService) -> str | None:
    """当前在途运行的 id；没有则 None。"""
    active = _active_runs(runs)
    return active[0].id if active else None


def _drain_requester(stop: threading.Event, database: Path, clock: Clock) -> DrainRequester:
    """排水动作：置位停机事件并返回当时的在跑运行数。

    API 请求线程自开连接（docs/11 并发）：tick 线程的连接不跨线程使用。
    置位先于统计：在跑数是请求被接受那一刻的口径，之后的推进归停机收尾。
    """

    def request() -> int:
        stop.set()
        connection = open_database(database)
        try:
            return len(_active_runs(JobRunService(connection, clock)))
        finally:
            connection.close()

    return request


def _startup_clock_events(
    connection: sqlite3.Connection,
    options: ServeOptions,
) -> list[tuple[str, Mapping[str, object]]]:
    """启动校时的结果事件：命令失败不阻断启动；开关关闭时不校时也不记事件（docs/08）。"""
    if not options.run_timesync:
        return []
    clock_check = timesync.startup_check(options.timesync_runner)
    return timesync.apply_startup_check(connection, clock_check, at=options.clock.utc_now())


def _host_setup_events(
    data_root: Path,
    options: ServeOptions,
    *,
    at: datetime,
) -> list[tuple[str, Mapping[str, object]]]:
    """宿主启动动作的结果事件：看门狗停手补报、自动登录自检、任务注册。

    三件事都尽力而为，结果进事件 detail；单项失败不阻断核心启动，也不
    影响其余宿主事件（docs/08 自动登录自检与计划任务两节）。看门狗停手
    补报是取走式：claim 会取走并清除状态文件里的 gave_up_pending_report
    标记，属破坏性副作用，取走后不再重报。停手事件另带失败次数、最近
    失败时刻、心跳内容与日志尾部四个证据字段。自动登录自检额外带 ok，
    供通知分支判断是否需要告警。
    """
    events: list[tuple[str, Mapping[str, object]]] = []
    gave_up = watchdog.claim_gave_up_report(data_root / "run" / watchdog.STATE_FILENAME)
    if gave_up is not None:
        events.append((WATCHDOG_GAVE_UP_EVENT, gave_up))
    logon = options.registry_get(WINLOGON_KEY, "AutoAdminLogon")
    user = options.registry_get(WINLOGON_KEY, "DefaultUserName")
    healthy = logon == "1" and bool(user)
    if healthy:
        detail = f"AutoAdminLogon=1，DefaultUserName={user}"
    else:
        detail = f"AutoAdminLogon={logon}，DefaultUserName={user}；重启后会停在登录界面"
    events.append((AUTOLOGON_CHECKED_EVENT, {"detail": detail, "ok": healthy}))
    if options.register_tasks:
        registration = tasks.register_registered_tasks(
            data_root,
            runner=options.task_runner,
            registered_at=at,
        )
        events.append((TASKS_REGISTERED_EVENT, {"detail": registration.detail}))
    return events


def _record_events(
    connection: sqlite3.Connection,
    events: Iterable[tuple[str, Mapping[str, object]]],
    *,
    at: datetime,
    event_type: EventType = EventType.HOST_EVENT,
) -> None:
    """把全局事件落库；顶层类型随事件归属选 host.event 或 core.event（docs/03）。

    写不进说明库有故障，由调用方处置。
    """
    log = EventLog(connection)
    for event, fields in events:
        # 事件名以调用方给的为准：payload 自带的 event 字段不允许覆盖事件名。
        log.append_global(event_type, {**fields, "event": event}, at=at)


def _record_plugin_events(
    connection: sqlite3.Connection,
    host: RpcPluginHost,
    *,
    at: datetime,
) -> None:
    """装配结果落全局事件：成功与拒绝都留痕，排障时能看出加载了什么（docs/03）。"""
    log = EventLog(connection)
    for plugin_id in host.registry.plugin_ids():
        log.append_global(
            EventType.PLUGIN_LOADED,
            {"plugin_id": plugin_id, "version": host.versions.get(plugin_id, "")},
            at=at,
        )
    for plugin_id, reason in host.rejections.items():
        log.append_global(
            EventType.PLUGIN_REJECTED,
            {"plugin_id": plugin_id, "reason": reason},
            at=at,
        )


def _plugins_root(data_root: Path, options: ServeOptions) -> Path:
    """插件根与 data 同级（docs/08 路径布局）；测试可显式指向临时目录。"""
    if options.plugins_root is not None:
        return options.plugins_root
    return data_root.parent / "plugins"


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
        instance.report_second_instance(data_root / DATABASE_FILENAME, at=started_at)
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
    plugin_host: RpcPluginHost | None = None
    api_server: ApiServer | None = None
    notify: NotifyService | None = None
    try:
        database = data_root / DATABASE_FILENAME
        connection = open_database(database)
        migrate(connection, backups_dir=data_root / "backups", clock=options.clock)
        # 通知游标在启动扫描与启动事件落库之前定桩：崩溃收尾（运行置 unknown）与
        # 看门狗停手补报都要进通知，重启不能漏报。
        notify = NotifyService(connection, options.clock, data_root=data_root, log=options.log)
        plugins_root = _plugins_root(data_root, options)
        plugin_host = RpcPluginHost(
            plugins_root, installed_plugin_ids(plugins_root), log=options.log
        )
        registry = plugin_host.registry
        _record_plugin_events(connection, plugin_host, at=options.clock.utc_now())
        supervisor = Supervisor(
            connection,
            options.clock,
            registry,
            data_root=data_root,
            log=options.log,
            shutdown=options.stop.is_set,
        )
        supervisor.startup()
        # 校时先于其余宿主动作：注册与补报落在尽量校准过的钟上。
        clock_events = _startup_clock_events(connection, options)
        setup_at = options.clock.utc_now()
        _record_events(
            connection,
            [*clock_events, *_host_setup_events(data_root, options, at=setup_at)],
            at=setup_at,
        )
        # 环境动作在请求线程自开连接：supervisor 的连接只归 tick 线程。
        actions = EnvironmentActionService(database, options.clock, registry, log=options.log)
        api_server, exit_code = _start_api(
            connection,
            options,
            actions,
            supervisor,
            _drain_requester(options.stop, database, options.clock),
        )
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
            pruner=GlobalEventsPruner(connection, options.clock, log=options.log),
            retention=RetentionRunner(
                connection, options.clock, data_root=data_root, log=options.log
            ),
        )
        return loop.run()
    except BackupError as error:
        # 迁移前快照是强制前置（docs/08）：快照失败则不迁移，拒绝启动。
        options.log.error({"event": "serve.migrate_snapshot_failed", "error": str(error)})
        return EXIT_ERROR
    except (OSError, sqlite3.Error, MigrationError) as error:
        _log_run_failed(options.log, error)
        return EXIT_ERROR
    except Exception as error:
        # 程序错误也要留可检索记录；堆栈留在 stderr，由照旧上抛提供。
        _log_run_failed(options.log, error)
        raise
    finally:
        # 逐项回收：任一步抛错不能跳过后面的清理，worker 子进程随核心先结束。
        if plugin_host is not None:
            _recycle(plugin_host.close, event="serve.close_plugin_host_failed", log=options.log)
        if notify is not None:
            _recycle(notify.close, event="serve.close_notify_failed", log=options.log)
        if api_server is not None:
            _recycle(api_server.shutdown, event="serve.shutdown_api_failed", log=options.log)
        if connection is not None:
            _recycle(connection.close, event="serve.close_database_failed", log=options.log)


def _log_run_failed(log: Logger, error: BaseException) -> None:
    """核心运行失败的结构化记录；事件名与字段固定，供按 event 与 error_type 检索。"""
    log.error(
        {"event": "serve.run_failed", "error": str(error), "error_type": type(error).__name__}
    )


def _recycle(step: Callable[[], object], *, event: str, log: Logger) -> None:
    """执行一步回收；失败记日志后继续，不让一项异常把后面的清理全跳过。"""
    try:
        step()
    except Exception as error:
        log.error({"event": event, "error": str(error)})


def _start_api(
    connection: sqlite3.Connection,
    options: ServeOptions,
    environment_actions: EnvironmentActions,
    run_stopper: RunStopper,
    drain_requester: DrainRequester,
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
            [(tailscale.TAILSCALE_DOWN_EVENT, {"detail": down_detail})],
            at=options.clock.utc_now(),
            event_type=EventType.HOST_EVENT,
        )
        options.log.warn({"event": "serve.tailscale_down", "detail": down_detail})
    # api.host 直接配成 tailnet 地址时不再重复追加，避免同一地址绑两次。
    plan = tailscale.ListenPlan(host, port, tuple(extra for extra in extra_hosts if extra != host))
    server = _bind_api(connection, options, plan, environment_actions, run_stopper, drain_requester)
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
    environment_actions: EnvironmentActions,
    run_stopper: RunStopper,
    drain_requester: DrainRequester,
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
                task_runner=options.task_runner,
                environment_actions=environment_actions,
                run_stopper=run_stopper,
                drain_requester=drain_requester,
            ),
        )
    except OSError as error:
        # docs/08 的退出码 4 只针对端口被占；地址不可达等其它绑定失败走通用失败路径。
        if error.errno != errno.EADDRINUSE:
            raise
        _record_events(
            connection,
            [
                (
                    PORT_CONFLICT_EVENT,
                    {"detail": f"{plan.host}:{plan.port} 已被其它进程监听：{error}"},
                )
            ],
            at=options.clock.utc_now(),
            event_type=EventType.CORE_EVENT,
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
