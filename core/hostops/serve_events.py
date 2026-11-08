"""宿主启动事件：校时结果、看门狗停手补报、自动登录自检、任务注册与插件装配结果的记录。

事件按归属分 host.event 与 core.event 两个顶层类型（docs/03）；本模块只负责产生
与落库，单项失败是否阻断启动由调用方决定（docs/08）。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from contracts import EventType
from core.hostops import tasks, timesync, watchdog
from core.hostops.doctor import WINLOGON_KEY
from core.plugins.rpc_host import RpcPluginHost
from storage.repositories.events import EventLog

if TYPE_CHECKING:
    from core.hostops.serve import ServeOptions

# 事件常量与使用它们的事件记录同处一模块：事件记录直接用常量，serve 不再反向
# 导入 serve_events 取值，两者之间没有循环导入。AUTOLOGON_CHECKED_EVENT 的字面量
# 只在这里定义；另两个取自有属主的模块，避免字面量各写一份。
AUTOLOGON_CHECKED_EVENT = "host.autologon_checked"
TASKS_REGISTERED_EVENT = tasks.TASKS_REGISTERED_EVENT
WATCHDOG_GAVE_UP_EVENT = watchdog.WATCHDOG_GAVE_UP_EVENT


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
    gave_up = watchdog.take_gave_up_event_payload(data_root / "run" / watchdog.STATE_FILENAME)
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
