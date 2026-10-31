r"""dead-man 外部心跳：看门狗检查通过后 ping 外部 Push 服务，检查失败按既有通知渠道报 fail。

看门狗主模块只用标准库并保持零数据库访问（core.hostops.watchdog 模块说明），ping
与 fail 通知由本模块实现，经 CheckInputs.deadman 注入、由 CLI 装配。settings 从数据
库现读：配置随库而生，缺库即未配置，静默休眠；库损坏才落错误日志供排障。唯一开关
是 notify.heartbeat_url：空串表示不启用，不发任何网络请求、不写任何日志（docs/08
外部报警）。URL 出现在日志里一律脱敏为来源加前 6 位（docs/11 不回显）；失败通知
正文不带 URL。
"""

from __future__ import annotations

import http.client
import json
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from contracts import Clock, ContractError, Notification, NotificationLevel, Notifier
from core.api.store import all_settings
from core.hostops import watchdog
from core.hostops.doctor import DATABASE_FILENAME
from core.notify.config import NotifyConfig
from core.notify.selftest import (
    build_kernel_delivery_context,
    deliver_test_notification,
    redacted_url,
)
from storage import open_database
from storage.defaults import default

HEARTBEAT_URL_KEY: Final[str] = "notify.heartbeat_url"

# 单次心跳的网络等待上限（docs/08 外部报警）；互斥量挡住并发检查，超时只延长当次运行。
_HEARTBEAT_TIMEOUT_SECONDS: Final[float] = 5.0

# docs/11 外部调用：默认关闭系统代理。
_NO_PROXY_OPENER: Final[urllib.request.OpenerDirector] = urllib.request.build_opener(
    urllib.request.ProxyHandler({})
)

_PING_EVENT: Final[str] = "deadman.heartbeat"
_FAIL_EVENT: Final[str] = "deadman.check_failed"
_ERROR_EVENT: Final[str] = "deadman.error"

# 失败通知正文里的动作说明；取值与 docs/08 独立看门狗的动作集合一致。
_ACTION_TEXT: Final[dict[str, str]] = {
    watchdog.ACTION_START_REQUESTED: "已请求计划任务拉起核心",
    watchdog.ACTION_GAVE_UP: "已停止拉起，待核心下次启动补报",
    watchdog.ACTION_NONE: "拉起请求未成功",
}

type Transport = Callable[[urllib.request.Request, float], object]


def _default_transport(request: urllib.request.Request, timeout_seconds: float) -> object:
    """真实传输：一次 GET，默认关代理（docs/11 外部调用）；应答即关，不读内容。"""
    with _NO_PROXY_OPENER.open(request, timeout=timeout_seconds):
        return None


@dataclass(frozen=True, slots=True)
class DeadmanInputs:
    """dead-man 的注入项：时钟、网络传输与渠道构造；生产路径全用默认值。

    transport 与 channel_factory 是测试缝：假传输断言请求次数，假渠道免真实网络。
    """

    clock: Clock
    transport: Transport = _default_transport
    channel_factory: Callable[[NotifyConfig], Notifier] | None = None


def on_check_report(
    report: watchdog.CheckReport,
    *,
    data_root: Path,
    inputs: DeadmanInputs,
) -> None:
    """一轮检查结束后的 dead-man 处理：通过发心跳，失败按去重规则报 fail。

    Args:
        report: 看门狗一轮检查的结果快照。
        data_root: 数据根目录，定位库文件、状态文件与看门狗日志。
        inputs: 时钟、网络传输与渠道构造的注入项。

    Returns:
        None。所有结果都落看门狗日志。

    Raises:
        无：本函数绝不抛异常，任何内部故障都落日志（deadman.error）后返回，
        看门狗主循环不受影响（docs/08 外部报警）。
    """
    try:
        _dispatch(report, data_root=data_root, inputs=inputs)
    except Exception as error:
        _record(data_root, inputs.clock.utc_now(), {"event": _ERROR_EVENT, "detail": str(error)})


def _dispatch(report: watchdog.CheckReport, *, data_root: Path, inputs: DeadmanInputs) -> None:
    """读 settings 并分派：未启用静默返回，通过 ping，失败报 fail。"""
    settings = _read_settings(data_root / DATABASE_FILENAME)
    if settings is None:
        return
    url = _heartbeat_url(settings)
    if url is None:
        _record(
            data_root,
            inputs.clock.utc_now(),
            {"event": _ERROR_EVENT, "detail": f"{HEARTBEAT_URL_KEY} 必须是字符串"},
        )
        return
    if not url:
        return
    if report.passed:
        _clear_fail_notified(data_root)
        _ping_round(url, data_root=data_root, inputs=inputs)
        return
    _fail_round(report, settings, data_root=data_root, inputs=inputs)


def _read_settings(database_path: Path) -> Mapping[str, Any] | None:
    """现读 settings 快照；库文件缺失返回 None。

    配置随库而生：缺库即未配置，按未启用静默处理；库存在但读不了（损坏）由
    on_check_report 兜底落 deadman.error，供排障。
    """
    if not database_path.is_file():
        return None
    connection = open_database(database_path)
    try:
        return all_settings(connection)
    finally:
        connection.close()


def _heartbeat_url(settings: Mapping[str, Any]) -> str | None:
    """读心跳地址；缺省取 defaults，类型不是字符串视为非法（返回 None）。"""
    value = settings.get(HEARTBEAT_URL_KEY, default("notify", "heartbeat_url"))
    if not isinstance(value, str):
        return None
    return value


def _ping_round(url: str, *, data_root: Path, inputs: DeadmanInputs) -> None:
    """发一次心跳并把结果落日志；网络故障不外抛（docs/08 异常不得影响主循环）。"""
    at = inputs.clock.utc_now()
    try:
        inputs.transport(urllib.request.Request(url, method="GET"), _HEARTBEAT_TIMEOUT_SECONDS)
    except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError) as error:
        _record(
            data_root,
            at,
            {
                "event": _PING_EVENT,
                "url": redacted_url(url),
                "result": "failed",
                "detail": str(error),
            },
        )
        return
    _record(data_root, at, {"event": _PING_EVENT, "url": redacted_url(url), "result": "ok"})


def _fail_round(
    report: watchdog.CheckReport,
    settings: Mapping[str, Any],
    *,
    data_root: Path,
    inputs: DeadmanInputs,
) -> None:
    """失败分支：同一次失败事件只发一条 fail（去重标记在状态文件），投递成功才置位。"""
    state_path = data_root / "run" / watchdog.STATE_FILENAME
    state = watchdog.read_state(state_path)
    if state.get("deadman_fail_notified"):
        return
    notified, detail = _deliver_fail(report, settings, data_root=data_root, inputs=inputs)
    _record(
        data_root,
        inputs.clock.utc_now(),
        {
            "event": _FAIL_EVENT,
            "result": "notified" if notified else "notify_failed",
            "detail": detail,
        },
    )
    if notified:
        state["deadman_fail_notified"] = True
        watchdog.write_state(state_path, state)


def _deliver_fail(
    report: watchdog.CheckReport,
    settings: Mapping[str, Any],
    *,
    data_root: Path,
    inputs: DeadmanInputs,
) -> tuple[bool, str]:
    """按既有通知渠道投递一条 fail；单次尝试、不重试、不落账目（与备份失败通知同款）。

    Returns:
        是否投递成功，以及渠道应答或配置错误的说明。
    """
    try:
        delivery = deliver_test_notification(
            settings,
            build_kernel_delivery_context(inputs.clock, data_root=data_root),
            channel_factory=inputs.channel_factory,
            message=_fail_notification(report),
        )
    except ContractError as error:
        return False, error.message
    return delivery.result.ok, delivery.result.detail


def _clear_fail_notified(data_root: Path) -> None:
    """检查恢复通过即结束当前失败事件：清去重标记，下一轮失败重新通知。"""
    state_path = data_root / "run" / watchdog.STATE_FILENAME
    state = watchdog.read_state(state_path)
    if not state.get("deadman_fail_notified"):
        return
    state["deadman_fail_notified"] = False
    watchdog.write_state(state_path, state)


def _fail_notification(report: watchdog.CheckReport) -> Notification:
    """失败通知文案：动作、心跳年龄与 pid；不带心跳 URL，外部渠道不回显敏感值。"""
    age = "未知" if report.heartbeat_age_s is None else f"{report.heartbeat_age_s:.0f} 秒"
    pid = "未知" if report.core_pid is None else str(report.core_pid)
    action = _ACTION_TEXT.get(report.action, report.action)
    return Notification(
        level=NotificationLevel.ERROR,
        title="看门狗：核心失联",
        body=f"检查未通过：{action}。心跳年龄 {age}，核心 pid {pid}。外部心跳本轮停发。",
    )


def _record(data_root: Path, at: datetime, fields: Mapping[str, object]) -> None:
    """追加一行 dead-man 结果到看门狗日志；写不了就放弃（同 watchdog._append_log 口径）。"""
    log_path = data_root / "logs" / watchdog.LOG_FILENAME
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"at": at.isoformat(), **fields}, ensure_ascii=False)
        with log_path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
    except OSError:
        return


__all__ = [
    "HEARTBEAT_URL_KEY",
    "DeadmanInputs",
    "Transport",
    "on_check_report",
]
