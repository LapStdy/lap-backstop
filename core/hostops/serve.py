"""serve 外壳：单实例启动序、核心装配、Supervisor tick 循环与停机收尾。

启动序固定为互斥量→锁文件→数据库与迁移→通知清扫→启动扫描（docs/08 单实例一节）。
硬杀留下的锁文件不清理，由互斥量兜底、下次启动覆盖。注入项与停机信号注册在
serve_options，心跳写入件与在途运行口径在 serve_loop，API 挂载在 serve_api，
启动事件在 serve_events。
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from contracts import Logger
from core.__main__ import (
    EXIT_DISK_LOW,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_SECOND_INSTANCE,
    resolve_executable_path,
)
from core.api.server import ApiServer
from core.api.version import executable_version
from core.events.maintenance import GlobalEventsPruner
from core.hostops import heartbeat, instance
from core.hostops.doctor import DATABASE_FILENAME
from core.hostops.environment_actions import ActionOptions, EnvironmentActionService
from core.hostops.retention import RetentionOptions, RetentionRunner
from core.hostops.serve_api import (
    PORT_CONFLICT_EVENT,
    ApiServices,
    _drain_requester,
    _start_api,
)
from core.hostops.serve_events import (
    AUTOLOGON_CHECKED_EVENT,
    TASKS_REGISTERED_EVENT,
    WATCHDOG_GAVE_UP_EVENT,
    _host_setup_events,
    _plugins_root,
    _record_events,
    _record_plugin_events,
    _startup_clock_events,
)
from core.hostops.serve_loop import HeartbeatWriter, _active_run
from core.hostops.serve_options import TICK_INTERVAL_SECONDS, ServeOptions, install_stop_handlers
from core.jobs.run_service import JobRunService
from core.notify.service import NotifyOptions, NotifyService
from core.notify.sweep import sweep_pending_notifications
from core.plugins.rpc_host import RpcPluginHost, installed_plugin_ids
from core.supervisor import Supervisor
from storage import BackupError, MigrationError, migrate, open_database
from storage.defaults import default

# 停机与循环退出时等待执行线程的上限：等待有界，超时按下次启动回收（docs/08）；
# 不做成配置键，把收尾时限留在代码里。
_EXECUTOR_JOIN_SECONDS = 60.0
# 停机收尾的强制终止宽限：执行线程超过它仍未收尾，才结束承载在途调用的 worker
# 子进程树（docs/08 停机与阶段边界）；同样留在代码里，不做成配置键。
_EXECUTOR_ABORT_GRACE_SECONDS = 30.0
_HEARTBEAT_INTERVAL = timedelta(seconds=int(default("host", "heartbeat_interval_seconds")))


@dataclass(frozen=True, slots=True)
class ServeLoopParts:
    """tick 循环的部件：推进、记账、心跳与注入项；维护侧协作对象可缺省。"""

    supervisor: Supervisor
    runs: JobRunService
    writer: HeartbeatWriter
    options: ServeOptions
    notify: NotifyService | None = None
    pruner: GlobalEventsPruner | None = None
    retention: RetentionRunner | None = None


class ServeLoop:
    """tick 循环：每轮推进一次 Supervisor、追一次通知、按日闸门清一次过期数据，到心跳间隔写心跳。"""

    def __init__(self, parts: ServeLoopParts) -> None:
        """装配循环部件；维护侧协作对象传 None 即关闭对应侧工作。"""
        self._supervisor = parts.supervisor
        self._runs = parts.runs
        self._writer = parts.writer
        self._options = parts.options
        self._notify = parts.notify
        self._pruner = parts.pruner
        self._retention = parts.retention
        self._last_heartbeat_at: float | None = None

    def run(self) -> int:
        """循环到停机信号、tick 上限或磁盘 critical；正常路径返回 0，磁盘水位返回 5。

        退出前按模块上限等待执行线程收尾：超时的线程不再等待，运行状态与租约留给
        下次启动回收（docs/08 停机与阶段边界）。
        """
        ticks = 0
        while not self._options.stop.is_set() and (
            self._options.max_ticks is None or ticks < self._options.max_ticks
        ):
            self._supervisor.tick()
            self._poll_notify()
            self._prune_global_events()
            self._prune_retention()
            if self._supervisor.is_disk_critical:
                self._join_executors()
                return EXIT_DISK_LOW
            ticks += 1
            self._write_heartbeat_if_due()
            self._options.stop.wait(self._options.tick_interval_seconds)
        self._join_executors()
        return EXIT_OK

    def _join_executors(self) -> None:
        """等待执行线程收尾；宽限到点强制终止卡死调用的 worker，上限取模块常量。"""
        self._supervisor.join_executors(
            _EXECUTOR_JOIN_SECONDS, abort_grace_seconds=_EXECUTOR_ABORT_GRACE_SECONDS
        )

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


def run_serve(data_root: Path, options: ServeOptions | None = None) -> int:
    """serve 主流程，返回进程退出码。

    启动序：互斥量（被占写事件退 3）→ 锁文件（写不了退 1）→ 数据库与迁移 →
    通知清扫 → 启动扫描 → tick 循环。收尾永远删锁文件并释放互斥量。
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


@dataclass(slots=True)
class _CoreParts:
    """serve 核心段的在装部件：随装配逐步填充，收尾按字段逐项回收。"""

    started_at: datetime
    connection: sqlite3.Connection | None = None
    notify: NotifyService | None = None
    plugin_host: RpcPluginHost | None = None
    api_server: ApiServer | None = None
    loop: ServeLoop | None = None


def _run_core(data_root: Path, options: ServeOptions, started_at: datetime) -> int:
    """库、迁移、通知清扫、启动扫描、API 挂载与 tick 循环；打开失败与运行期错误都记日志退 1。"""
    parts = _CoreParts(started_at=started_at)
    try:
        exit_code = _assemble_core(data_root, options, parts)
        if exit_code is not None:
            return exit_code
        # 装配无早退时循环必然已就位；loop 仍空属于装配段的程序错误。
        assert parts.loop is not None
        return parts.loop.run()
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
        _recycle_core(parts, options.log)


def _assemble_core(data_root: Path, options: ServeOptions, parts: _CoreParts) -> int | None:
    """按启动序装配核心部件；返回挂载早退的退出码，None 表示循环可跑。

    部件随装随填：中途失败时收尾只回收已打开的资源（docs/08 停机与阶段边界）。
    """
    database = data_root / DATABASE_FILENAME
    parts.connection = open_database(database)
    migrate(parts.connection, backups_dir=data_root / "backups", clock=options.clock)
    parts.notify = _build_notify(parts.connection, data_root, options)
    parts.plugin_host = _build_plugin_host(data_root, options)
    supervisor = _startup_supervisor(parts.connection, data_root, options, parts.plugin_host)
    # 环境动作在请求线程自开连接：supervisor 的连接只归 tick 线程。
    parts.api_server, exit_code = _start_api(
        parts.connection,
        options,
        ApiServices(
            environment_actions=EnvironmentActionService(
                database,
                options.clock,
                parts.plugin_host.registry,
                options=ActionOptions(log=options.log),
            ),
            run_stopper=supervisor,
            drain_requester=_drain_requester(options.stop, database, options.clock),
        ),
    )
    if exit_code is not None:
        return exit_code
    parts.loop = _build_loop(parts, data_root, options, supervisor)
    return None


def _build_notify(
    connection: sqlite3.Connection, data_root: Path, options: ServeOptions
) -> NotifyService:
    """通知服务：启动清扫在先，游标定桩在后（docs/08 通知账目口径）。"""
    # 上次进程硬杀遗留的 pending 通知先落终态：账目中间态不跨进程，也不流进本次投递历史。
    sweep_pending_notifications(connection, options.clock, log=options.log)
    # 通知游标在启动扫描与启动事件落库之前定桩：崩溃收尾（运行置 unknown）与
    # 看门狗停手补报都要进通知，重启不能漏报。
    return NotifyService(
        connection,
        options.clock,
        data_root=data_root,
        options=NotifyOptions(log=options.log),
    )


def _build_plugin_host(data_root: Path, options: ServeOptions) -> RpcPluginHost:
    """插件宿主：插件根取自注入项或 data 同级（docs/08 路径布局）。"""
    plugins_root = _plugins_root(data_root, options)
    return RpcPluginHost(plugins_root, installed_plugin_ids(plugins_root), log=options.log)


def _startup_supervisor(
    connection: sqlite3.Connection,
    data_root: Path,
    options: ServeOptions,
    plugin_host: RpcPluginHost,
) -> Supervisor:
    """启动扫描与宿主启动事件：装配 Supervisor、重建状态，再落校时与宿主动作事件。"""
    _record_plugin_events(connection, plugin_host, at=options.clock.utc_now())
    supervisor = Supervisor(
        connection,
        options.clock,
        plugin_host.registry,
        data_root=data_root,
        log=options.log,
        disk_free=options.disk_free,
        shutdown=options.stop.is_set,
        abort_workers=plugin_host.abort_workers,
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
    return supervisor


def _build_loop(
    parts: _CoreParts,
    data_root: Path,
    options: ServeOptions,
    supervisor: Supervisor,
) -> ServeLoop:
    """心跳写入件与循环部件：维护侧协作对象随库连接一并生成。"""
    connection = parts.connection
    assert connection is not None
    return ServeLoop(
        ServeLoopParts(
            supervisor=supervisor,
            runs=JobRunService(connection, options.clock),
            writer=HeartbeatWriter(
                path=data_root / "run" / heartbeat.HEARTBEAT_FILENAME,
                started_at=parts.started_at,
                pid=os.getpid(),
                version=executable_version(),
            ),
            options=options,
            notify=parts.notify,
            pruner=GlobalEventsPruner(connection, options.clock, log=options.log),
            retention=RetentionRunner(
                connection,
                options.clock,
                data_root=data_root,
                options=RetentionOptions(log=options.log),
            ),
        )
    )


def _recycle_core(parts: _CoreParts, log: Logger) -> None:
    """逐项回收：任一步抛错不能跳过后面的清理，worker 子进程随核心先结束。"""
    if parts.plugin_host is not None:
        _recycle(parts.plugin_host.close, event="serve.close_plugin_host_failed", log=log)
    if parts.notify is not None:
        _recycle(parts.notify.close, event="serve.close_notify_failed", log=log)
    if parts.api_server is not None:
        _recycle(parts.api_server.shutdown, event="serve.shutdown_api_failed", log=log)
    if parts.connection is not None:
        _recycle(parts.connection.close, event="serve.close_database_failed", log=log)


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


__all__ = [
    "AUTOLOGON_CHECKED_EVENT",
    "PORT_CONFLICT_EVENT",
    "TASKS_REGISTERED_EVENT",
    "TICK_INTERVAL_SECONDS",
    "WATCHDOG_GAVE_UP_EVENT",
    "HeartbeatWriter",
    "ServeLoop",
    "ServeLoopParts",
    "ServeOptions",
    "executable_version",
    "install_stop_handlers",
    "run_serve",
]
