"""内核侧的 worker 通道：拉起子进程、等待调用结果、超时强杀与崩溃降级。

一次调用一条 JSON 消息（ADR 0010）；等待与强杀用真实单调时间，业务截止仍由核心按
注入的 Clock 判定。进程世代与失败收尾在 ``channel_session``；本模块保留对外的
``WorkerChannel`` 与消息交换。
"""

from __future__ import annotations

import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from contracts import RunContext
from core.plugins import rpc
from core.plugins.channel_session import ChannelSession
from core.plugins.errors import (
    WorkerCallError,
    WorkerCancelledError,
    WorkerCrashError,
    WorkerPayloadError,
    WorkerTimeoutError,
)
from core.plugins.worker_options import WorkerConfig

_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
# 分派结果的「还没完」哨兵：日志通知与回调处理完继续等本调用的结果。
_PENDING: Final = object()


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
        self._call_id = 0

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
        call_id = self._next_call_id()
        self._session.send(
            {
                "kind": rpc.CALL,
                "id": call_id,
                "capability": capability,
                "method": method,
                "args": dict(args),
                "context": rpc.encode_context(ctx),
            }
        )
        return self._await_result(call_id, capability, method, ctx)

    def close(self) -> None:
        """发送 shutdown 并结束进程树；已退出的通道重复关闭无副作用。"""
        self._session.close()

    def _next_call_id(self) -> int:
        self._call_id += 1
        return self._call_id

    def _await_result(
        self,
        call_id: int,
        capability: str,
        method: str,
        ctx: RunContext,
    ) -> object:
        """等待本条调用的结果，期间处理日志与回调，超时或取消时结束进程。"""
        limits = self._session.limits
        deadline = time.monotonic() + limits.call_timeout_seconds
        cancel_deadline: float | None = None
        while True:
            now = time.monotonic()
            if cancel_deadline is None and ctx.cancel.is_cancelled():
                self._session.send({"kind": rpc.CANCEL, "id": call_id}, counts_as_failure=False)
                cancel_deadline = now + limits.cancel_grace_seconds
            if cancel_deadline is not None and now >= cancel_deadline:
                self._session.fail_process(counts_as_failure=False)
                raise WorkerCancelledError(
                    f"{capability}.{method} 在取消宽限期内未返回，worker 已结束"
                )
            if now >= deadline:
                self._session.fail_process(reason="调用超时")
                raise WorkerTimeoutError(
                    f"{capability}.{method} 超过调用超时 {limits.call_timeout_seconds:.1f} 秒，"
                    "worker 已结束"
                )
            reply = self._session.receive(deadline, now)
            if reply is None:
                continue
            outcome = self._dispatch(reply, call_id, capability, method, ctx)
            if outcome is not _PENDING:
                return outcome

    def _dispatch(
        self,
        reply: Mapping[str, Any],
        call_id: int,
        capability: str,
        method: str,
        ctx: RunContext,
    ) -> object:
        """分派一条消息：结果、日志、回调或协议错误。"""
        kind = reply.get("kind")
        if kind == rpc.RESULT:
            if reply.get("id") != call_id:
                self._session.fail_process(reason="结果 id 错配")
                raise WorkerPayloadError(
                    f"{capability}.{method} 收到错配的结果 id：{reply.get('id')}"
                )
            if reply.get("ok") is not True:
                raise WorkerCallError(_describe_failure(reply, capability, method))
            self._session.reset_failures()
            return reply.get("value")
        if kind == rpc.FAULT:
            self._session.fail_process(reason="worker 报故障")
            raise WorkerCrashError(f"worker 在调用中报故障：{reply.get('message')}")
        if kind == rpc.NOTIFY:
            self._handle_notify(reply, ctx)
            return _PENDING
        if kind == rpc.CALLBACK:
            self._handle_callback(reply, ctx)
            return _PENDING
        self._session.fail_process(reason="未知协议消息")
        raise WorkerPayloadError(f"未知协议消息：{kind!r}")

    def _handle_notify(self, message: Mapping[str, Any], ctx: RunContext) -> None:
        if message.get("op") != "log":
            return
        fields = message.get("fields")
        if not isinstance(fields, Mapping):
            return
        level = message.get("level")
        if level == "debug":
            ctx.log.debug(fields)
        elif level == "info":
            ctx.log.info(fields)
        elif level == "warn":
            ctx.log.warn(fields)
        elif level == "error":
            ctx.log.error(fields)

    def _handle_callback(self, message: Mapping[str, Any], ctx: RunContext) -> None:
        callback_id = message.get("callback_id")
        args = message.get("args")
        try:
            if not isinstance(args, Mapping):
                raise WorkerPayloadError("回调参数应为对象")
            value = self._run_callback(message.get("op"), args, ctx)
        except Exception as error:
            self._session.send(
                {
                    "kind": rpc.CALLBACK_RESULT,
                    "callback_id": callback_id,
                    "ok": False,
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
            return
        self._session.send(
            {
                "kind": rpc.CALLBACK_RESULT,
                "callback_id": callback_id,
                "ok": True,
                "value": value,
            }
        )

    def _run_callback(self, op: object, args: Mapping[str, Any], ctx: RunContext) -> object:
        if op == "emit":
            event = rpc.plugin_event(_text(args, "event"))
            payload = args.get("payload")
            if not isinstance(payload, Mapping):
                raise WorkerPayloadError("emit 的 payload 应为对象")
            rpc.check_plugin_payload(event, payload)
            ctx.emit(event.value, payload)
            return None
        if op == "record_evidence":
            return ctx.record_evidence(
                _text(args, "kind"),
                Path(_text(args, "path")),
                _text(args, "summary"),
            )
        raise WorkerPayloadError(f"未知回调：{op!r}")


def _describe_failure(reply: Mapping[str, Any], capability: str, method: str) -> str:
    error = reply.get("error")
    if isinstance(error, Mapping):
        name = error.get("type")
        message = error.get("message")
        return f"{capability}.{method} 抛出 {name}：{message}"
    return f"{capability}.{method} 返回失败"


def _text(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise WorkerPayloadError(f"{key} 应为字符串")
    return value
