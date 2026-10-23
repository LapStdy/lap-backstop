"""内核侧的 worker 通道：拉起子进程、等待调用结果、超时强杀与崩溃降级。

一次调用一条 JSON 消息（ADR 0010）；等待与强杀用真实单调时间，业务截止仍由核心按
注入的 Clock 判定。杀进程时结束整棵进程树，避免插件拉起的脚本在后台继续跑。
"""

from __future__ import annotations

import contextlib
import queue
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from contracts import PLUGIN_EVENTS, EventType, Logger, RunContext
from core.logging import NullLogger
from core.plugins import rpc
from core.plugins.errors import (
    PluginDegradedError,
    WorkerCallError,
    WorkerCancelledError,
    WorkerCrashError,
    WorkerPayloadError,
    WorkerTimeoutError,
)
from core.plugins.worker_options import WorkerConfig
from core.plugins.worker_process import close_streams, terminate_tree

_POLL_SECONDS: Final = 0.05
_STDERR_LINES: Final = 20
_MAX_LINE_CHARS: Final = 8 * 1024 * 1024
_REPO_ROOT: Final = Path(__file__).resolve().parents[2]
_PENDING: Final = object()


class WorkerChannel:
    """一个插件的 worker 子进程；同一时刻只执行一个能力调用。"""

    def __init__(self, plugin_id: str, config: WorkerConfig) -> None:
        self.plugin_id = plugin_id
        self._config = config
        self._log: Logger = NullLogger() if config.log is None else config.log
        self._command = (
            list(config.command)
            if config.command is not None
            else [sys.executable, "-u", "-m", "core.plugins.worker"]
        )
        self._process: subprocess.Popen[str] | None = None
        self._reader: threading.Thread | None = None
        self._stderr_reader: threading.Thread | None = None
        self._messages: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._stderr: deque[str] = deque(maxlen=_STDERR_LINES)
        self._call_id = 0
        self._restart_failures = 0
        self._degraded_until: float | None = None
        self._capabilities: frozenset[str] = frozenset()

    @property
    def pid(self) -> int | None:
        """当前 worker 进程号；未启动为 None。"""
        return None if self._process is None else self._process.pid

    @property
    def is_degraded(self) -> bool:
        """连续失败后处于降级期；到期后允许再试。"""
        if self._degraded_until is None:
            return False
        return time.monotonic() < self._degraded_until

    @property
    def capabilities(self) -> frozenset[str]:
        """worker 就绪消息里声明的能力名。"""
        return self._capabilities

    def start(self) -> frozenset[str]:
        """拉起 worker 并等到 ready，返回声明的能力集合。"""
        self._ensure_started()
        return self._capabilities

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
        self._ensure_started()
        call_id = self._next_call_id()
        self._send_call(call_id, capability, method, args, ctx)
        return self._await_result(call_id, capability, method, ctx)

    def close(self) -> None:
        """发送 shutdown 并结束进程树；已退出的通道重复关闭无副作用。"""
        process, self._process = self._process, None
        if process is None:
            return
        if process.poll() is None and process.stdin is not None:
            with contextlib.suppress(OSError):
                process.stdin.write(rpc.encode_line({"kind": rpc.SHUTDOWN}) + "\n")
                process.stdin.flush()
                process.wait(timeout=2)
        terminate_tree(process)
        close_streams(process)
        self._join_threads()

    def _next_call_id(self) -> int:
        self._call_id += 1
        return self._call_id

    def _send_call(
        self,
        call_id: int,
        capability: str,
        method: str,
        args: Mapping[str, Any],
        ctx: RunContext,
    ) -> None:
        self._send(
            {
                "kind": rpc.CALL,
                "id": call_id,
                "capability": capability,
                "method": method,
                "args": dict(args),
                "context": rpc.encode_context(ctx),
            }
        )

    def _await_result(
        self,
        call_id: int,
        capability: str,
        method: str,
        ctx: RunContext,
    ) -> object:
        """等待本条调用的结果，期间处理日志与回调，超时或取消时结束进程。"""
        limits = self._config.limits
        deadline = time.monotonic() + limits.call_timeout_seconds
        cancel_deadline: float | None = None
        while True:
            now = time.monotonic()
            if cancel_deadline is None and ctx.cancel.is_cancelled():
                self._send({"kind": rpc.CANCEL, "id": call_id}, counts_as_failure=False)
                cancel_deadline = now + limits.cancel_grace_seconds
            if cancel_deadline is not None and now >= cancel_deadline:
                self._fail_process(counts_as_failure=False)
                raise WorkerCancelledError(
                    f"{capability}.{method} 在取消宽限期内未返回，worker 已结束"
                )
            if now >= deadline:
                self._fail_process()
                raise WorkerTimeoutError(
                    f"{capability}.{method} 超过调用超时 {limits.call_timeout_seconds:.1f} 秒，"
                    "worker 已结束"
                )
            reply = self._receive(deadline, now)
            if reply is None:
                continue
            outcome = self._dispatch(reply, call_id, capability, method, ctx)
            if outcome is not _PENDING:
                return outcome

    def _receive(self, deadline: float, now: float) -> Mapping[str, Any] | None:
        """取一条 worker 消息；队列空或进程退出时给出对应结论。"""
        wait = min(_POLL_SECONDS, max(deadline - now, 0.0) or _POLL_SECONDS)
        try:
            reply = self._messages.get(timeout=wait)
        except queue.Empty:
            return None
        if reply is None:
            tail = self._stderr_tail()
            self._fail_process()
            raise WorkerCrashError(f"worker 在调用中退出：{tail}")
        return reply

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
                self._fail_process()
                raise WorkerPayloadError(
                    f"{capability}.{method} 收到错配的结果 id：{reply.get('id')}"
                )
            if reply.get("ok") is not True:
                raise WorkerCallError(_describe_failure(reply, capability, method))
            self._restart_failures = 0
            self._degraded_until = None
            return reply.get("value")
        if kind == rpc.FAULT:
            self._fail_process()
            raise WorkerCrashError(f"worker 在调用中报故障：{reply.get('message')}")
        if kind == rpc.NOTIFY:
            self._handle_notify(reply, ctx)
            return _PENDING
        if kind == rpc.CALLBACK:
            self._handle_callback(reply, ctx)
            return _PENDING
        self._fail_process()
        raise WorkerPayloadError(f"未知协议消息：{kind!r}")

    def _ensure_started(self) -> None:
        if self.is_degraded:
            raise PluginDegradedError(f"插件 {self.plugin_id} 处于降级期，停止接收调用")
        if self._degraded_until is not None:
            self._degraded_until = None
            self._restart_failures = 0
            self._log.info({"event": "plugin.recovered", "plugin_id": self.plugin_id})
        if self._process is not None and self._process.poll() is None:
            return
        self._start()

    def _start(self) -> None:
        self._discard_process()
        command = [*self._command, self.plugin_id, "--plugins-root", str(self._config.plugins_root)]
        process = subprocess.Popen(
            command,
            cwd=_REPO_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        messages: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._process = process
        self._messages = messages
        self._reader = threading.Thread(
            target=self._read_stdout,
            args=(process, messages),
            name=f"worker-{self.plugin_id}",
            daemon=True,
        )
        self._reader.start()
        self._stderr_reader = threading.Thread(
            target=self._read_stderr,
            args=(process,),
            name=f"worker-{self.plugin_id}-stderr",
            daemon=True,
        )
        self._stderr_reader.start()
        self._await_ready(messages)

    def _await_ready(self, messages: queue.Queue[dict[str, Any] | None]) -> None:
        deadline = time.monotonic() + self._config.limits.start_timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._fail_process()
                raise WorkerCrashError(f"插件 {self.plugin_id} 启动超时")
            try:
                message = messages.get(timeout=min(remaining, _POLL_SECONDS))
            except queue.Empty:
                continue
            if message is None:
                tail = self._stderr_tail()
                self._fail_process()
                raise WorkerCrashError(f"插件 {self.plugin_id} 启动即退出：{tail}")
            kind = message.get("kind")
            if kind == rpc.FAULT:
                self._fail_process()
                raise WorkerCrashError(f"插件 {self.plugin_id} 加载失败：{message.get('message')}")
            if kind != rpc.READY:
                continue
            if message.get("protocol") != rpc.PROTOCOL_VERSION:
                self._fail_process()
                raise WorkerCrashError(
                    f"插件 {self.plugin_id} 的协议版本不匹配：{message.get('protocol')}"
                )
            raw = message.get("capabilities")
            self._capabilities = (
                frozenset(item for item in raw if isinstance(item, str))
                if isinstance(raw, list)
                else frozenset()
            )
            return

    def _read_stdout(
        self,
        process: subprocess.Popen[str],
        messages: queue.Queue[dict[str, Any] | None],
    ) -> None:
        """只把本世代进程的消息写进本世代队列，旧线程不会污染新会话。"""
        stream = process.stdout
        if stream is None:
            messages.put(None)
            return
        try:
            for line in stream:
                text = line.strip()
                if not text:
                    continue
                if len(text) > _MAX_LINE_CHARS:
                    messages.put(None)
                    return
                try:
                    messages.put(rpc.decode_line(text))
                except WorkerPayloadError:
                    messages.put(None)
                    return
        except (OSError, ValueError):
            pass
        finally:
            messages.put(None)

    def _read_stderr(self, process: subprocess.Popen[str]) -> None:
        stream = process.stderr
        if stream is None:
            return
        try:
            for line in stream:
                self._stderr.append(line.rstrip())
        except (OSError, ValueError):
            pass

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
            self._send(
                {
                    "kind": rpc.CALLBACK_RESULT,
                    "callback_id": callback_id,
                    "ok": False,
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
            return
        self._send(
            {
                "kind": rpc.CALLBACK_RESULT,
                "callback_id": callback_id,
                "ok": True,
                "value": value,
            }
        )

    def _run_callback(self, op: object, args: Mapping[str, Any], ctx: RunContext) -> object:
        if op == "emit":
            event = _plugin_event(_text(args, "event"))
            payload = args.get("payload")
            if not isinstance(payload, Mapping):
                raise WorkerPayloadError("emit 的 payload 应为对象")
            _check_plugin_payload(event, payload)
            ctx.emit(event.value, payload)
            return None
        if op == "record_evidence":
            return ctx.record_evidence(
                _text(args, "kind"),
                Path(_text(args, "path")),
                _text(args, "summary"),
            )
        raise WorkerPayloadError(f"未知回调：{op!r}")

    def _send(self, message: Mapping[str, Any], *, counts_as_failure: bool = True) -> None:
        process = self._process
        if process is None or process.stdin is None:
            raise WorkerCrashError(f"插件 {self.plugin_id} 的 worker 未启动")
        try:
            process.stdin.write(rpc.encode_line(message) + "\n")
            process.stdin.flush()
        except OSError as error:
            self._fail_process(counts_as_failure=counts_as_failure)
            raise WorkerCrashError(f"写入 worker 失败：{error}") from error

    def _fail_process(self, *, counts_as_failure: bool = True) -> None:
        """结束整棵进程树并计数；用户取消不是重启失败，不计入降级预算。"""
        self._discard_process()
        if not counts_as_failure:
            return
        self._restart_failures += 1
        if self._restart_failures < self._config.limits.max_restarts:
            return
        self._degraded_until = time.monotonic() + self._config.limits.degraded_retry_seconds
        self._log.error(
            {
                "event": "plugin.degraded",
                "plugin_id": self.plugin_id,
                "failures": self._restart_failures,
                "retry_after_seconds": self._config.limits.degraded_retry_seconds,
            }
        )

    def _discard_process(self) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        terminate_tree(process)
        close_streams(process)
        self._join_threads()

    def _join_threads(self) -> None:
        for thread in (self._reader, self._stderr_reader):
            if thread is None or thread is threading.current_thread():
                continue
            if not thread.is_alive():
                continue
            thread.join(timeout=2)
            if thread.is_alive():
                self._log.warn(
                    {
                        "event": "worker.reader_leak",
                        "plugin_id": self.plugin_id,
                        "thread": thread.name,
                    }
                )

    def _stderr_tail(self) -> str:
        return " | ".join(line for line in self._stderr if line)[-800:]


def _plugin_event(name: str) -> EventType:
    """插件只能写 PLUGIN_EVENTS 里的事件；其余事件由内核写。"""
    try:
        event = EventType(name)
    except ValueError as error:
        raise WorkerPayloadError(f"未知事件类型：{name}") from error
    if event not in PLUGIN_EVENTS:
        raise WorkerPayloadError(f"插件不能写内核事件：{name}")
    return event


def _check_plugin_payload(event: EventType, payload: Mapping[str, Any]) -> None:
    if event is not EventType.RUN_SIGNAL:
        return
    kind = payload.get("kind")
    if not isinstance(kind, str) or not kind:
        raise WorkerPayloadError("run.signal 的 kind 必须是非空字符串")
    signal_payload = payload.get("payload", {})
    if not isinstance(signal_payload, Mapping):
        raise WorkerPayloadError("run.signal 的 payload 必须是对象")


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
