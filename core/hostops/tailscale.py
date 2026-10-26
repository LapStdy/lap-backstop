"""Tailscale 远程访问：状态探测、启动监听计划与事件的唯一口径。

远程访问只信 tailscale status --json 的 BackendState 与 TailscaleIPs；探测失败
（命令缺失、非零退出、坏 JSON）一律降级为「不可查」而不抛错——远程访问是增强
能力，不能拖垮核心启动与 doctor（docs/08 Tailscale 一节）。
"""

from __future__ import annotations

import ipaddress
import json
import sqlite3
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from contracts import EventType, Logger
from storage.defaults import default
from storage.repositories.events import EventLog

STATUS_COMMAND = ("tailscale", "status", "--json")
RUNNING_STATE = "Running"
# tailnet 地址的固定网段（docs/08）：只认这段里的 IPv4，局域网与公网地址不监听。
TAILNET_IPV4_NETWORK = ipaddress.IPv4Network("100.64.0.0/10")
# 探测必须限时（docs/11 外部调用带超时）；命令缺失或超时按 -1 处理。
PROBE_TIMEOUT_SECONDS = 10.0
_COMMAND_MISSING_EXIT = -1

# settings 键与缺省值基线（config/defaults.toml [api] 段）。
REMOTE_ENABLED_KEY = "api.remote_enabled"

# host.event 的 event 字段取值（注册表见 docs/03）；事件名属主在本模块。
TAILSCALE_DOWN_EVENT = "host.tailscale_down"
TAILSCALE_BIND_FAILED_EVENT = "host.tailscale_bind_failed"

Runner = Callable[[list[str]], subprocess.CompletedProcess[bytes]]


@dataclass(frozen=True, slots=True)
class ListenPlan:
    """一次启动的监听计划：主地址、端口与追加监听的 tailnet 地址。"""

    host: str
    port: int
    extra_hosts: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TailscaleStatus:
    """一条探测结果；state 为空串表示状态不可查。"""

    state: str
    addresses: tuple[str, ...] = ()
    note: str = ""

    @property
    def is_running(self) -> bool:
        """BackendState 是否为 Running。"""
        return self.state == RUNNING_STATE

    @property
    def down_detail(self) -> str:
        """host.tailscale_down 的 detail 取值：BackendState 或不可查原因。"""
        return self.state if self.state else f"不可查：{self.note}"


def remote_listen_plan(
    settings: Mapping[str, Any],
    runner: Runner,
    log: Logger,
) -> tuple[tuple[str, ...], str | None]:
    """remote_enabled 下的追加监听计划：tailnet IPv4 与 host.tailscale_down 的 detail。

    detail 为 None 表示 Tailscale 在 Running；监听只在启动时定，Tailscale 之后
    恢复要重启内核生效（docs/08），主回环监听不受影响。

    Raises:
        ValueError: api.remote_enabled 不是布尔值。
    """
    enabled = settings.get(REMOTE_ENABLED_KEY, default("api", "remote_enabled"))
    if not isinstance(enabled, bool):
        raise ValueError(f"{REMOTE_ENABLED_KEY} 必须是布尔值")
    if not enabled:
        return (), None
    status = probe(runner)
    if not status.is_running:
        return (), status.down_detail
    if not status.addresses:
        log.warn({"event": "serve.tailscale_no_address"})
    return status.addresses, None


def record_bind_degradation(
    connection: sqlite3.Connection,
    *,
    failures: tuple[tuple[str, str], ...],
    at: datetime,
) -> None:
    """tailnet 地址绑定失败不退出：写 host.event 说明，继续只用已成功的地址。"""
    if not failures:
        return
    detail = (
        "、".join(f"{failed_host}（{error}）" for failed_host, error in failures)
        + "绑定失败；这些地址不监听，仅保留已成功的监听地址"
    )
    EventLog(connection).append_global(
        EventType.HOST_EVENT,
        {"event": TAILSCALE_BIND_FAILED_EVENT, "detail": detail},
        at=at,
    )


def default_runner(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    """探测命令的默认执行件：捕获输出并限时；缺失或超时按退出码 -1 返回。"""
    try:
        return subprocess.run(
            command,
            capture_output=True,
            check=False,
            timeout=PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return subprocess.CompletedProcess(command, _COMMAND_MISSING_EXIT, b"", b"")


def probe(runner: Runner) -> TailscaleStatus:
    """跑一次 status 命令并解析；失败降级为不可查，理由记在 note。"""
    completed = runner(list(STATUS_COMMAND))
    if completed.returncode == _COMMAND_MISSING_EXIT:
        return TailscaleStatus(state="", addresses=(), note="命令不可用")
    if completed.returncode != 0:
        return TailscaleStatus(state="", addresses=(), note=f"命令退出码 {completed.returncode}")
    status = parse_status_json(completed.stdout.decode("utf-8", errors="replace"))
    if status.state == "":
        return TailscaleStatus(state="", addresses=(), note="输出不可解析")
    return status


def parse_status_json(text: str) -> TailscaleStatus:
    """解析 status --json 的 stdout；坏 JSON 与缺 BackendState 都按不可查处理。"""
    try:
        data = json.loads(text)
    except ValueError:
        return TailscaleStatus(state="")
    if not isinstance(data, dict):
        return TailscaleStatus(state="")
    state = data.get("BackendState")
    return TailscaleStatus(
        state=state if isinstance(state, str) else "",
        addresses=_tailnet_ipv4s(data.get("TailscaleIPs")),
    )


def _tailnet_ipv4s(raw: object) -> tuple[str, ...]:
    """TailscaleIPs 里的 tailnet IPv4；IPv6、非 tailnet 与非法条目丢弃。"""
    if not isinstance(raw, list):
        return ()
    addresses: list[str] = []
    for value in raw:
        try:
            address = ipaddress.ip_address(str(value))
        except ValueError:
            continue
        if address.version == 4 and address in TAILNET_IPV4_NETWORK:
            addresses.append(str(address))
    return tuple(addresses)


__all__ = [
    "PROBE_TIMEOUT_SECONDS",
    "REMOTE_ENABLED_KEY",
    "RUNNING_STATE",
    "STATUS_COMMAND",
    "TAILNET_IPV4_NETWORK",
    "TAILSCALE_BIND_FAILED_EVENT",
    "TAILSCALE_DOWN_EVENT",
    "ListenPlan",
    "Runner",
    "TailscaleStatus",
    "default_runner",
    "parse_status_json",
    "probe",
    "record_bind_degradation",
    "remote_listen_plan",
]
