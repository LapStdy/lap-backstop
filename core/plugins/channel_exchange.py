"""通道的消息交换：一次能力调用的请求/响应与订阅消息的分派。

会话（``channel_session``）管进程世代与失败收尾；这里把一次调用走完：发请求、
等到匹配的结果，途中处理 worker 的日志通知与回调。
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final, NamedTuple

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

# 分派结果的「还没完」哨兵：日志通知与回调处理完继续等本调用的结果。
_PENDING: Final = object()


class PendingCall(NamedTuple):
    """一次在途调用：id 用于结果匹配，能力与方法名用于错误描述。"""

    id: int
    capability: str
    method: str


class ChannelExchange:
    """把一次能力调用走完的交换机；同一时刻只执行一个调用。"""

    def __init__(self, session: ChannelSession) -> None:
        self._session = session
        self._call_id = 0

    def call(
        self,
        capability: str,
        method: str,
        args: Mapping[str, Any],
        ctx: RunContext,
    ) -> object:
        """执行一次能力调用，返回 worker 的原始结果值；失败形态见 ``WorkerChannel.call``。"""
        call_id = self._next_call_id()
        ticket = PendingCall(call_id, capability, method)
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
        return self._await_result(ticket, ctx)

    def _next_call_id(self) -> int:
        self._call_id += 1
        return self._call_id

    def _await_result(self, ticket: PendingCall, ctx: RunContext) -> object:
        """等待本条调用的结果，期间处理日志与回调，超时或取消时结束进程。"""
        limits = self._session.limits
        deadline = time.monotonic() + limits.call_timeout_seconds
        cancel_deadline: float | None = None
        while True:
            now = time.monotonic()
            if cancel_deadline is None and ctx.cancel.is_cancelled():
                self._session.send({"kind": rpc.CANCEL, "id": ticket.id}, counts_as_failure=False)
                cancel_deadline = now + limits.cancel_grace_seconds
            if cancel_deadline is not None and now >= cancel_deadline:
                self._session.fail_process(counts_as_failure=False)
                raise WorkerCancelledError(
                    f"{ticket.capability}.{ticket.method} 在取消宽限期内未返回，worker 已结束"
                )
            if now >= deadline:
                self._session.fail_process(reason="调用超时")
                raise WorkerTimeoutError(
                    f"{ticket.capability}.{ticket.method} 超过调用超时 "
                    f"{limits.call_timeout_seconds:.1f} 秒，worker 已结束"
                )
            reply = self._session.receive(deadline, now)
            if reply is None:
                continue
            outcome = self._dispatch(reply, ticket, ctx)
            if outcome is not _PENDING:
                return outcome

    def _dispatch(
        self,
        reply: Mapping[str, Any],
        ticket: PendingCall,
        ctx: RunContext,
    ) -> object:
        """分派一条消息：结果、日志、回调或协议错误。"""
        kind = reply.get("kind")
        if kind == rpc.RESULT:
            if reply.get("id") != ticket.id:
                self._session.fail_process(reason="结果 id 错配")
                raise WorkerPayloadError(
                    f"{ticket.capability}.{ticket.method} 收到错配的结果 id：{reply.get('id')}"
                )
            if reply.get("ok") is not True:
                raise WorkerCallError(_describe_failure(reply, ticket.capability, ticket.method))
            self._session.reset_failures()
            return reply.get("value")
        if kind == rpc.FAULT:
            self._session.fail_process(reason="worker 报故障")
            raise WorkerCrashError(f"worker 在调用中报故障：{reply.get('message')}")
        if kind == rpc.NOTIFY:
            _handle_notify(reply, ctx)
            return _PENDING
        if kind == rpc.CALLBACK:
            self._handle_callback(reply, ctx)
            return _PENDING
        self._session.fail_process(reason="未知协议消息")
        raise WorkerPayloadError(f"未知协议消息：{kind!r}")

    def _handle_callback(self, message: Mapping[str, Any], ctx: RunContext) -> None:
        callback_id = message.get("callback_id")
        args = message.get("args")
        try:
            if not isinstance(args, Mapping):
                raise WorkerPayloadError("回调参数应为对象")
            value = _run_callback(message.get("op"), args, ctx)
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


def _handle_notify(message: Mapping[str, Any], ctx: RunContext) -> None:
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


def _run_callback(op: object, args: Mapping[str, Any], ctx: RunContext) -> object:
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
