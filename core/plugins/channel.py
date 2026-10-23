"""内核侧的 worker 通道：拉起子进程、等待调用结果、超时强杀与崩溃降级。

一次调用一条 JSON 消息（ADR 0010）；等待与强杀用真实单调时间，业务截止仍由核心按
注入的 Clock 判定。插件崩溃、超时或连续失败只影响当前调用与后续调用，不接触内核状态。
"""

from __future__ import annotations

import contextlib
import queue
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from contracts import Logger, RunContext
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
from storage.defaults import default

_POLL_SECONDS: Final = 0.05
_STDERR_LINES: Final = 20
_REPO_ROOT: Final = Path(__file__).resolve().parents[2]


class WorkerChannel:
    """一个插件的 worker 子进程；同一时刻只执行一个能力调用。"""

    def __init__(
        self,
        plugin_id: str,
        *,
        plugins_root: Path,
        log: Logger | None = None,
        command: Sequence[str] | None = None,
        call_timeout_seconds: float | None = None,
        start_timeout_seconds: float | None = None,
        cancel_grace_seconds: float | None = None,
        max_restarts: int | None = None,
    ) -> None:
        self.plugin_id = plugin_id
        self._plugins_root = plugins_root
        self._log: Logger = NullLogger() if log is None else log
        self._command = (
            list(command)
            if command is not None
            else [sys.executable, "-u", "-m", "core.plugins.worker"]
        )
        self._call_timeout = _seconds("call_timeout_seconds", call_timeout_seconds)
        self._start_timeout = _seconds("start_timeout_seconds", start_timeout_seconds)
        self._cancel_grace = _seconds("cancel_grace_seconds", cancel_grace_seconds)
        self._max_restarts = _count("max_restarts", max_restarts)
        self._process: subprocess.Popen[str] | None = None
        self._reader: threading.Thread | None = None
        self._stderr_reader: threading.Thread | None = None
        self._messages: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._stderr: deque[str] = deque(maxlen=_STDERR_LINES)
        self._call_id = 0
        self._restart_failures = 0
        self._degraded = False
        self._capabilities: frozenset[str] = frozenset()

    @property
    def pid(self) -> int | None:
        """当前 worker 进程号；未启动为 None。"""
        return None if self._process is None else self._process.pid

    @property
    def degraded(self) -> bool:
        """连续重启失败后为 True，后续调用直接失败。"""
        return self._degraded

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
        *,
        timeout_seconds: float | None = None,
    ) -> Any:
        """执行一次能力调用；插件异常、超时、崩溃与取消都转成对应错误。"""
        limit = self._call_timeout if timeout_seconds is None else timeout_seconds
        self._ensure_started()
        self._call_id += 1
        call_id = self._call_id
        message = {
            "kind": rpc.CALL,
            "id": call_id,
            "capability": capability,
            "method": method,
            "args": dict(args),
            "context": rpc.encode_context(ctx),
        }
        self._write(message)
        deadline = time.monotonic() + limit
        cancel_deadline: float | None = None
        while True:
            now = time.monotonic()
            if cancel_deadline is None and ctx.cancel.is_cancelled():
                self._write({"kind": rpc.CANCEL, "id": call_id})
                cancel_deadline = now + self._cancel_grace
            if cancel_deadline is not None and now >= cancel_deadline:
                self._fail_process(counts_as_failure=False)
                raise WorkerCancelledError(
                    f"{capability}.{method} 在取消宽限期内未返回，worker 已结束"
                )
            if now >= deadline:
                self._fail_process()
                raise WorkerTimeoutError(
                    f"{capability}.{method} 超过调用超时 {limit:.1f} 秒，worker 已结束"
                )
            wait = min(_POLL_SECONDS, max(deadline - now, 0.0) or _POLL_SECONDS)
            try:
                reply = self._messages.get(timeout=wait)
            except queue.Empty:
                continue
            if reply is None:
                tail = self._stderr_tail()
                self._fail_process()
                raise WorkerCrashError(f"worker 在 {capability}.{method} 中退出：{tail}")
            kind = reply.get("kind")
            if kind == rpc.RESULT:
                if reply.get("id") != call_id:
                    continue
                if reply.get("ok") is True:
                    self._restart_failures = 0
                    return reply.get("value")
                raise WorkerCallError(_describe_failure(reply, capability, method))
            if kind == rpc.NOTIFY:
                self._handle_notify(reply, ctx)
            elif kind == rpc.CALLBACK:
                self._handle_callback(reply, ctx)

    def close(self) -> None:
        """发送 shutdown 并结束进程；已退出的通道重复关闭无副作用。"""
        process, self._process = self._process, None
        if process is None:
            return
        try:
            if process.poll() is None and process.stdin is not None:
                process.stdin.write(rpc.encode_line({"kind": rpc.SHUTDOWN}) + "\n")
                process.stdin.flush()
                process.wait(timeout=2)
        except (OSError, subprocess.TimeoutExpired):
            pass
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        self._join_threads()

    def _ensure_started(self) -> None:
        if self._degraded:
            raise PluginDegradedError(f"插件 {self.plugin_id} 已降级，停止接收调用")
        if self._process is not None and self._process.poll() is None:
            return
        self._start()

    def _start(self) -> None:
        self._discard_process()
        command = [*self._command, self.plugin_id, "--plugins-root", str(self._plugins_root)]
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
        self._process = process
        self._messages = queue.Queue()
        self._reader = threading.Thread(
            target=self._read_stdout,
            args=(process,),
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
        self._await_ready()

    def _await_ready(self) -> None:
        deadline = time.monotonic() + self._start_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._fail_process()
                raise WorkerCrashError(f"插件 {self.plugin_id} 启动超时")
            try:
                message = self._messages.get(timeout=min(remaining, _POLL_SECONDS))
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
            capabilities = message.get("capabilities")
            self._capabilities = (
                frozenset(item for item in capabilities if isinstance(item, str))
                if isinstance(capabilities, list)
                else frozenset()
            )
            return

    def _read_stdout(self, process: subprocess.Popen[str]) -> None:
        stream = process.stdout
        if stream is None:
            self._messages.put(None)
            return
        try:
            for line in stream:
                text = line.strip()
                if not text:
                    continue
                try:
                    message = rpc.decode_line(text)
                except WorkerPayloadError:
                    self._messages.put(None)
                    return
                self._messages.put(message)
        finally:
            self._messages.put(None)

    def _read_stderr(self, process: subprocess.Popen[str]) -> None:
        stream = process.stderr
        if stream is None:
            return
        for line in stream:
            self._stderr.append(line.rstrip())

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
            self._write(
                {
                    "kind": rpc.CALLBACK_RESULT,
                    "callback_id": callback_id,
                    "ok": False,
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
            return
        self._write(
            {
                "kind": rpc.CALLBACK_RESULT,
                "callback_id": callback_id,
                "ok": True,
                "value": value,
            }
        )

    def _run_callback(self, op: object, args: Mapping[str, Any], ctx: RunContext) -> object:
        if op == "emit":
            event = _text(args, "event")
            payload = args.get("payload")
            if not isinstance(payload, Mapping):
                raise WorkerPayloadError("emit 的 payload 应为对象")
            ctx.emit(event, payload)
            return None
        if op == "record_evidence":
            return ctx.record_evidence(
                _text(args, "kind"),
                Path(_text(args, "path")),
                _text(args, "summary"),
            )
        raise WorkerPayloadError(f"未知回调：{op!r}")

    def _write(self, message: Mapping[str, Any]) -> None:
        process = self._process
        if process is None or process.stdin is None:
            raise WorkerCrashError(f"插件 {self.plugin_id} 的 worker 未启动")
        try:
            process.stdin.write(rpc.encode_line(message) + "\n")
            process.stdin.flush()
        except OSError as error:
            self._fail_process()
            raise WorkerCrashError(f"写入 worker 失败：{error}") from error

    def _fail_process(self, *, counts_as_failure: bool = True) -> None:
        """结束进程并计数；用户取消不是重启失败，不计入降级预算。"""
        self._discard_process()
        if not counts_as_failure:
            return
        self._restart_failures += 1
        if self._restart_failures >= self._max_restarts:
            self._degraded = True
            self._log.error(
                {
                    "event": "plugin.degraded",
                    "plugin_id": self.plugin_id,
                    "failures": self._restart_failures,
                }
            )

    def _discard_process(self) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        if process.poll() is None:
            process.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=5)
        self._messages.put(None)
        self._join_threads()

    def _join_threads(self) -> None:
        for thread in (self._reader, self._stderr_reader):
            if (
                thread is not None
                and thread.is_alive()
                and thread is not threading.current_thread()
            ):
                thread.join(timeout=2)

    def _stderr_tail(self) -> str:
        return " | ".join(line for line in self._stderr if line)[-800:]


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


def _seconds(key: str, value: float | None) -> float:
    return float(default("worker", key)) if value is None else value


def _count(key: str, value: int | None) -> int:
    return int(default("worker", key)) if value is None else value
