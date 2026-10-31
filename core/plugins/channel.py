"""内核侧的 worker 通道：拉起子进程、等待调用结果、超时强杀与崩溃降级。

一次调用一条 JSON 消息（ADR 0010）；等待与强杀用真实单调时间，业务截止仍由核心按
注入的 Clock 判定。进程世代与失败收尾在 ``channel_session``，调用交换在
``channel_exchange``；本模块保留对外的 ``WorkerChannel`` 与默认拉起参数。
"""

from __future__ import annotations

import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from contracts import RunContext
from core.plugins.channel_exchange import ChannelExchange
from core.plugins.channel_session import ChannelSession
from core.plugins.worker_options import WorkerConfig

_REPO_ROOT: Final = Path(__file__).resolve().parents[2]


def _worker_launch(is_frozen: bool) -> tuple[list[str], Path]:
    """worker 默认拉起参数与工作目录。

    源码运行用 ``-m core.plugins.worker``，仓库根作 cwd 保证模块可导入；打包件里
    模块已进 exe，走 exe 自身的 worker 子命令（docs/08 计划任务的动作口径），
    工作目录取 exe 所在目录。
    """
    if is_frozen:
        return [sys.executable, "worker"], Path(sys.executable).resolve().parent
    return [sys.executable, "-u", "-m", "core.plugins.worker"], _REPO_ROOT


class WorkerChannel:
    """一个插件的 worker 子进程；同一时刻只执行一个能力调用。"""

    def __init__(self, plugin_id: str, config: WorkerConfig) -> None:
        self.plugin_id = plugin_id
        if config.command is None:
            self._command, self._cwd = _worker_launch(getattr(sys, "frozen", False))
        else:
            # 自带命令的调用方接管拉起语义，cwd 沿用源码运行口径。
            self._command = list(config.command)
            self._cwd = _REPO_ROOT
        self._session = ChannelSession(plugin_id, self._command, self._cwd, config)
        self._exchange = ChannelExchange(self._session)

    @property
    def pid(self) -> int | None:
        """当前 worker 进程号；未启动为 None。"""
        return self._session.pid

    @property
    def is_degraded(self) -> bool:
        """连续失败后处于降级期；到期后允许再试。"""
        return self._session.is_degraded

    @property
    def degradation_reason(self) -> str | None:
        """降级期内的原因描述；未降级为 None。每次读取实时判定，不缓存。"""
        return self._session.degradation_reason

    @property
    def capabilities(self) -> frozenset[str]:
        """worker 就绪消息里声明的能力名。"""
        return self._session.capabilities

    def start(self) -> frozenset[str]:
        """拉起 worker 并等到 ready，返回声明的能力集合。"""
        self._session.ensure_started()
        return self._session.capabilities

    def call(
        self,
        capability: str,
        method: str,
        args: Mapping[str, Any],
        ctx: RunContext,
    ) -> object:
        """执行一次能力调用，返回 worker 的原始结果值。

        Args:
            capability: 能力名，如 environment、script、verifier。
            method: 能力方法名。
            args: 方法参数，JSON 可序列化。
            ctx: 本次运行上下文。

        Returns:
            worker 返回的 JSON 取值；由代理按契约类型解码。

        Raises:
            PluginDegradedError: 插件处于降级期。
            WorkerTimeoutError: 调用超过超时，进程树已结束。
            WorkerCancelledError: 取消宽限期到期仍未返回。
            WorkerCrashError: 进程退出、写坏协议或启动失败。
            WorkerCallError: 插件方法抛出异常或返回失败结果。
            WorkerPayloadError: 协议载荷或插件事件不符合契约。
        """
        self._session.ensure_started()
        return self._exchange.call(capability, method, args, ctx)

    def close(self) -> None:
        """发送 shutdown 并结束进程树；已退出的通道重复关闭无副作用。"""
        self._session.close()
