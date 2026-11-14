"""worker 通道单元用例的共享脚手架：真实子进程夹具、上下文构造与进程存活探测。"""

from __future__ import annotations

import ctypes
import os
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import RunContext
from core.clock import VirtualClock
from core.plugins.channel import WorkerChannel
from core.plugins.worker_options import WorkerConfig, WorkerLimits

NOW = datetime(2026, 10, 24, 2, 0, tzinfo=UTC)
STUB = Path(__file__).resolve().parents[1] / "fixtures" / "worker_stub.py"
PLUGIN_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "plugins"
CALL_TIMEOUT_SECONDS = 0.2
START_TIMEOUT_SECONDS = 0.5
CANCEL_GRACE_SECONDS = 0.2
MAX_RESTARTS = 3
DEGRADED_RETRY_SECONDS = 300.0
CANCEL_DELAY_SECONDS = 0.05
OK_VALUE: dict[str, Any] = {"state": "ready", "detail": "stub", "observed_at": None}

_SYNCHRONIZE = 0x00100000
_WAIT_TIMEOUT = 0x00000102

RecordEvidence = Callable[[str, Path, str], str]
Emit = Callable[[str, Mapping[str, Any]], None]


def _process_alive(pid: int) -> bool:
    """进程是否仍在运行；Windows 用内核句柄查询，其它平台用信号 0。"""
    if sys.platform == "win32":
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(_SYNCHRONIZE, False, pid)
        if not handle:
            return False
        try:
            return kernel32.WaitForSingleObject(handle, 0) == _WAIT_TIMEOUT
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _process_ended(pid: int, *, timeout: float = 2.0) -> bool:
    """进程是否已经结束；Windows 的进程对象在退出后还会短暂可打开，所以轮询到期限。"""
    deadline = time.monotonic() + timeout
    while _process_alive(pid):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
    return True


class _Cancel:
    """可手动置位的取消令牌，模拟内核在等待期间置位。"""

    def __init__(self) -> None:
        self._cancelled = threading.Event()

    def cancel(self) -> None:
        self._cancelled.set()

    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()


class _RecordingLog:
    """记录结构化日志调用；通道自身的降级日志与 ctx.log 共用这份实现。"""

    def __init__(self) -> None:
        self.entries: list[tuple[str, Mapping[str, Any]]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("debug", fields))

    def info(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("info", fields))

    def warn(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("warn", fields))

    def error(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("error", fields))


def _noop_emit(event: str, payload: Mapping[str, Any]) -> None:
    return None


def _default_record_evidence(kind: str, path: Path, summary: str) -> str:
    return "ev-1"


@dataclass(frozen=True, slots=True)
class _ContextDeps:
    """_context 的可注入协作者：取消令牌、日志、证据、emit 与插件配置。"""

    cancel: _Cancel | None = None
    log: _RecordingLog | None = None
    record_evidence: RecordEvidence | None = None
    emit: Emit | None = None
    config: Mapping[str, Any] | None = None


_DEFAULT_CONTEXT_DEPS = _ContextDeps()


def _context(tmp_path: Path, deps: _ContextDeps = _DEFAULT_CONTEXT_DEPS) -> RunContext:
    """构造一次能力调用的上下文；取消令牌、日志、证据与 emit 由用例注入。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="stub",
        environment_id="env-1",
        config={"scenario": "stub"} if deps.config is None else dict(deps.config),
        work_dir=tmp_path / "work",
        evidence_dir=tmp_path / "evidence",
        deadline_at=NOW + timedelta(minutes=30),
        cancel=_Cancel() if deps.cancel is None else deps.cancel,
        clock=VirtualClock(NOW),
        secrets={},
        log=_RecordingLog() if deps.log is None else deps.log,
        emit=_noop_emit if deps.emit is None else deps.emit,
        record_evidence=(
            _default_record_evidence if deps.record_evidence is None else deps.record_evidence
        ),
    )


def _limits(
    *,
    call_timeout_seconds: float = 5.0,
    max_restarts: int = MAX_RESTARTS,
    degraded_retry_seconds: float = DEGRADED_RETRY_SECONDS,
) -> WorkerLimits:
    """真实插件用例的通道参数：超时放宽，避免启动开销撞上调用上限。"""
    return WorkerLimits(
        call_timeout_seconds=call_timeout_seconds,
        start_timeout_seconds=5.0,
        cancel_grace_seconds=CANCEL_GRACE_SECONDS,
        max_restarts=max_restarts,
        degraded_retry_seconds=degraded_retry_seconds,
    )


def _env_args(plugin_id: str) -> dict[str, Any]:
    return {"env": {"id": "env-1", "provider_id": plugin_id, "config": {}}}


@dataclass(frozen=True, slots=True)
class _ChannelOptions:
    """channels() 的可调参数：超时与重启字段带测试口径的默认值。"""

    control: Path | None = None
    call_timeout_seconds: float = CALL_TIMEOUT_SECONDS
    start_timeout_seconds: float = START_TIMEOUT_SECONDS
    cancel_grace_seconds: float = CANCEL_GRACE_SECONDS
    max_restarts: int = MAX_RESTARTS
    degraded_retry_seconds: float = DEGRADED_RETRY_SECONDS
    log: _RecordingLog | None = None

    def limits(self) -> WorkerLimits:
        """把超时、取消与重启字段映射成通道参数。"""
        return WorkerLimits(
            call_timeout_seconds=self.call_timeout_seconds,
            start_timeout_seconds=self.start_timeout_seconds,
            cancel_grace_seconds=self.cancel_grace_seconds,
            max_restarts=self.max_restarts,
            degraded_retry_seconds=self.degraded_retry_seconds,
        )


_DEFAULT_CHANNEL_OPTIONS = _ChannelOptions()


class _ChannelFactory:
    """为单个用例造通道，并负责用例结束时关闭所有子进程。"""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self._channels: list[WorkerChannel] = []

    def __call__(
        self, mode: str, options: _ChannelOptions = _DEFAULT_CHANNEL_OPTIONS
    ) -> WorkerChannel:
        command = [sys.executable, "-u", str(STUB), mode]
        if options.control is not None:
            command.append(str(options.control))
        channel = WorkerChannel(
            "stub",
            WorkerConfig(
                plugins_root=self.tmp_path / "plugins",
                command=tuple(command),
                limits=options.limits(),
                log=_RecordingLog() if options.log is None else options.log,
            ),
        )
        self._channels.append(channel)
        return channel

    def close(self) -> None:
        for channel in self._channels:
            channel.close()


def _control(channels: _ChannelFactory, *tokens: str) -> Path:
    """写一份逐次读取的行为控制文件，供 sequence 模式消费。"""
    path = channels.tmp_path / "behaviour.txt"
    path.write_text("".join(f"{token}\n" for token in tokens), encoding="utf-8")
    return path
