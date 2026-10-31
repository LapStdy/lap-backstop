"""通道的 worker 子进程会话：拉起进程世代、泵送消息流、失败收尾与降级记账。

会话持有内核与一个插件 worker 之间的全部进程侧状态：当前世代、读线程、消息队列、
stderr 尾巴与降级预算。失败时结束整棵进程树，避免插件拉起的脚本在后台继续跑。
"""

from __future__ import annotations

import contextlib
import queue
import subprocess
import threading
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from contracts import Logger
from core.logging import NullLogger
from core.plugins import rpc
from core.plugins.errors import PluginDegradedError, WorkerCrashError, WorkerPayloadError
from core.plugins.worker_options import WorkerConfig, WorkerLimits
from core.plugins.worker_process import close_streams, terminate_tree

_POLL_SECONDS: Final = 0.05
_STDERR_LINES: Final = 20
_MAX_LINE_CHARS: Final = 8 * 1024 * 1024
# 降级原因的兜底描述：失败路径都应传入具体形态，取不到时才有它。
_DEFAULT_DEGRADED_REASON: Final = "worker 故障"


class ChannelSession:
    """内核与一个插件 worker 子进程的会话；进程世代与降级预算都在这里。"""

    def __init__(
        self,
        plugin_id: str,
        command: list[str],
        cwd: Path,
        config: WorkerConfig,
    ) -> None:
        self.plugin_id = plugin_id
        self._command = command
        self._cwd = cwd
        self._config = config
        self._log: Logger = NullLogger() if config.log is None else config.log
        self._process: subprocess.Popen[str] | None = None
        self._reader: threading.Thread | None = None
        self._stderr_reader: threading.Thread | None = None
        self._messages: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._stderr: deque[str] = deque(maxlen=_STDERR_LINES)
        self._restart_failures = 0
        self._degraded_until: float | None = None
        self._degraded_reason: str = _DEFAULT_DEGRADED_REASON
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
    def degradation_reason(self) -> str | None:
        """降级期内的原因描述；未降级为 None。每次读取实时判定，不缓存。"""
        if not self.is_degraded:
            return None
        return f"连续 {self._restart_failures} 次失败（{self._degraded_reason}）"

    @property
    def capabilities(self) -> frozenset[str]:
        """worker 就绪消息里声明的能力名。"""
        return self._capabilities

    @property
    def limits(self) -> WorkerLimits:
        """通道的调用与取消时限，供消息交换读取。"""
        return self._config.limits

    def ensure_started(self) -> None:
        """降级期直接拒绝调用；降级窗口刚过去或进程已死时重新拉起。

        Raises:
            PluginDegradedError: 插件处于降级期。
            WorkerCrashError: 启动超时、启动即退出、加载失败或协议版本不匹配。
            OSError: 拉起子进程本身失败（可执行文件消失、权限、句柄耗尽）。
        """
        if self.is_degraded:
            raise PluginDegradedError(f"插件 {self.plugin_id} 处于降级期，停止接收调用")
        if self._degraded_until is not None:
            self.reset_failures()
            self._log.info({"event": "plugin.recovered", "plugin_id": self.plugin_id})
        if self._process is not None and self._process.poll() is None:
            return
        self._start()

    def send(self, message: Mapping[str, Any], *, counts_as_failure: bool = True) -> None:
        """把一条协议消息写进 worker 的 stdin；写失败按进程故障收尾。

        Args:
            message: 协议消息，行分隔 JSON 编码。
            counts_as_failure: 写入失败是否计入降级预算；用户取消引发的写失败为 False。

        Raises:
            WorkerCrashError: worker 未启动，或写入失败（进程树已随之结束）。
        """
        process = self._process
        if process is None or process.stdin is None:
            raise WorkerCrashError(f"插件 {self.plugin_id} 的 worker 未启动")
        try:
            process.stdin.write(rpc.encode_line(message) + "\n")
            process.stdin.flush()
        except OSError as error:
            self.fail_process(counts_as_failure=counts_as_failure, reason="写入失败")
            raise WorkerCrashError(f"写入 worker 失败：{error}") from error

    def receive(self, deadline: float, now: float) -> Mapping[str, Any] | None:
        """取一条 worker 消息；队列空或进程退出时给出对应结论。

        Args:
            deadline: 本次调用的绝对单调时刻，超过即不再等待。
            now: 当前单调时刻，用于压缩空转等待。

        Returns:
            一条协议消息；队列空返回 None，调用方继续轮询。

        Raises:
            WorkerCrashError: worker 在调用中退出（进程树已随之结束）。
        """
        wait = min(_POLL_SECONDS, max(deadline - now, 0.0) or _POLL_SECONDS)
        try:
            reply = self._messages.get(timeout=wait)
        except queue.Empty:
            return None
        if reply is None:
            tail = self._stderr_tail()
            self.fail_process(reason="调用中退出")
            raise WorkerCrashError(f"worker 在调用中退出：{tail}")
        return reply

    def fail_process(
        self,
        *,
        counts_as_failure: bool = True,
        reason: str = _DEFAULT_DEGRADED_REASON,
    ) -> None:
        """结束整棵进程树并计数；用户取消不是重启失败，不计入降级预算。

        达到 ``max_restarts`` 时进入降级期并记 ERROR 日志。可重复调用：进程已不在时
        只补记账。

        Args:
            counts_as_failure: 是否计入降级预算；用户取消传 False。
            reason: 触发本次失败的具体形态，降级后随 ``degradation_reason`` 暴露给路由。
        """
        self._discard_process()
        if not counts_as_failure:
            return
        self._restart_failures += 1
        if self._restart_failures < self._config.limits.max_restarts:
            return
        self._degraded_until = time.monotonic() + self._config.limits.degraded_retry_seconds
        self._degraded_reason = reason
        self._log.error(
            {
                "event": "plugin.degraded",
                "plugin_id": self.plugin_id,
                "failures": self._restart_failures,
                "retry_after_seconds": self._config.limits.degraded_retry_seconds,
                "reason": reason,
            }
        )

    def reset_failures(self) -> None:
        """调用成功后清零失败计数与降级标记。"""
        self._restart_failures = 0
        self._degraded_until = None

    def close(self) -> None:
        """发送 shutdown 并结束进程树；已退出的会话重复关闭无副作用。"""
        process, self._process = self._process, None
        if process is None:
            return
        if process.poll() is None and process.stdin is not None:
            with contextlib.suppress(OSError):
                process.stdin.write(rpc.encode_line({"kind": rpc.SHUTDOWN}) + "\n")
                process.stdin.flush()
                process.wait(timeout=2)
        self._release(process)

    def _start(self) -> None:
        self._discard_process()
        command = [*self._command, self.plugin_id, "--plugins-root", str(self._config.plugins_root)]
        process = subprocess.Popen(
            command,
            cwd=self._cwd,
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
                self.fail_process(reason="启动超时")
                raise WorkerCrashError(f"插件 {self.plugin_id} 启动超时")
            try:
                message = messages.get(timeout=min(remaining, _POLL_SECONDS))
            except queue.Empty:
                continue
            if message is None:
                tail = self._stderr_tail()
                self.fail_process(reason="启动即退出")
                raise WorkerCrashError(f"插件 {self.plugin_id} 启动即退出：{tail}")
            kind = message.get("kind")
            if kind == rpc.FAULT:
                self.fail_process(reason="加载失败")
                raise WorkerCrashError(f"插件 {self.plugin_id} 加载失败：{message.get('message')}")
            if kind != rpc.READY:
                continue
            if message.get("protocol") != rpc.PROTOCOL_VERSION:
                self.fail_process(reason="协议版本不匹配")
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

    def _discard_process(self) -> None:
        process, self._process = self._process, None
        if process is None:
            return
        self._release(process)

    def _release(self, process: subprocess.Popen[str]) -> None:
        """结束进程树、回收三根管道，并等仍在读的线程退出。"""
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
