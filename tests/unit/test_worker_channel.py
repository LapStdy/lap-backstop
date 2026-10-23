"""worker 通道的故障注入：调用超时强杀、崩溃重启、连续失败降级与取消宽限。

全部用例都用真实子进程与真实单调时间，每条只盯一条隔离约束：错误类型、进程号是否
变 None、降级标记，以及进程是否真的被结束。短超时保证整套远低于十秒。
"""

from __future__ import annotations

import ctypes
import os
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import RunContext
from core.clock import VirtualClock
from core.plugins.channel import WorkerChannel
from core.plugins.errors import (
    PluginDegradedError,
    WorkerCancelledError,
    WorkerCrashError,
    WorkerTimeoutError,
)

NOW = datetime(2026, 10, 24, 2, 0, tzinfo=UTC)
STUB = Path(__file__).resolve().parents[1] / "fixtures" / "worker_stub.py"
CALL_TIMEOUT_SECONDS = 0.2
START_TIMEOUT_SECONDS = 0.5
CANCEL_GRACE_SECONDS = 0.2
MAX_RESTARTS = 3
CANCEL_DELAY_SECONDS = 0.05
OK_VALUE: dict[str, Any] = {"state": "ready", "detail": "stub", "observed_at": None}

_SYNCHRONIZE = 0x00100000
_WAIT_TIMEOUT = 0x00000102

RecordEvidence = Callable[[str, Path, str], str]


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


def _context(
    tmp_path: Path,
    *,
    cancel: _Cancel | None = None,
    log: _RecordingLog | None = None,
    record_evidence: RecordEvidence | None = None,
) -> RunContext:
    """构造一次能力调用的上下文；取消令牌、日志与证据回调由用例注入。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="stub",
        environment_id="env-1",
        config={"scenario": "stub"},
        work_dir=tmp_path / "work",
        evidence_dir=tmp_path / "evidence",
        deadline_at=NOW + timedelta(minutes=30),
        cancel=_Cancel() if cancel is None else cancel,
        clock=VirtualClock(NOW),
        secrets={},
        log=_RecordingLog() if log is None else log,
        emit=_noop_emit,
        record_evidence=_default_record_evidence if record_evidence is None else record_evidence,
    )


class _ChannelFactory:
    """为单个用例造通道，并负责用例结束时关闭所有子进程。"""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self._channels: list[WorkerChannel] = []

    def __call__(
        self,
        mode: str,
        *,
        control: Path | None = None,
        call_timeout_seconds: float = CALL_TIMEOUT_SECONDS,
        start_timeout_seconds: float = START_TIMEOUT_SECONDS,
        cancel_grace_seconds: float = CANCEL_GRACE_SECONDS,
        max_restarts: int = MAX_RESTARTS,
        log: _RecordingLog | None = None,
    ) -> WorkerChannel:
        command = [sys.executable, "-u", str(STUB), mode]
        if control is not None:
            command.append(str(control))
        channel = WorkerChannel(
            "stub",
            plugins_root=self.tmp_path / "plugins",
            command=command,
            call_timeout_seconds=call_timeout_seconds,
            start_timeout_seconds=start_timeout_seconds,
            cancel_grace_seconds=cancel_grace_seconds,
            max_restarts=max_restarts,
            log=_RecordingLog() if log is None else log,
        )
        self._channels.append(channel)
        return channel

    def close(self) -> None:
        for channel in self._channels:
            channel.close()


@pytest.fixture
def channels(tmp_path: Path) -> Iterator[_ChannelFactory]:
    factory = _ChannelFactory(tmp_path)
    try:
        yield factory
    finally:
        factory.close()


def _control(channels: _ChannelFactory, *tokens: str) -> Path:
    """写一份逐次读取的行为控制文件，供 sequence 模式消费。"""
    path = channels.tmp_path / "behaviour.txt"
    path.write_text("".join(f"{token}\n" for token in tokens), encoding="utf-8")
    return path


def test_start_declares_capabilities(channels: _ChannelFactory) -> None:
    channel = channels("ok")
    capabilities = channel.start()
    assert capabilities == frozenset({"environment"})
    assert channel.pid is not None
    assert channel.pid != os.getpid()
    assert channel.degraded is False


def test_call_returns_worker_value(channels: _ChannelFactory) -> None:
    channel = channels("ok")
    value = channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert value == OK_VALUE
    assert channel.pid is not None
    assert channel.degraded is False


def test_call_timeout_kills_worker(channels: _ChannelFactory) -> None:
    channel = channels("silent")
    channel.start()
    pid = channel.pid
    assert pid is not None
    started = time.monotonic()
    with pytest.raises(WorkerTimeoutError, match="超过调用超时"):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert time.monotonic() - started >= CALL_TIMEOUT_SECONDS
    assert channel.pid is None
    assert channel.degraded is False
    assert _process_ended(pid) is True


def test_crash_then_next_call_restarts_worker(channels: _ChannelFactory) -> None:
    control = _control(channels, "crash", "ok")
    channel = channels("sequence", control=control)
    channel.start()
    first_pid = channel.pid
    assert first_pid is not None
    with pytest.raises(WorkerCrashError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.pid is None
    assert _process_ended(first_pid) is True
    assert channel.degraded is False
    value = channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert value == OK_VALUE
    assert channel.pid is not None
    assert channel.pid != first_pid
    assert channel.degraded is False


def test_repeated_failures_degrade_channel(channels: _ChannelFactory) -> None:
    log = _RecordingLog()
    control = _control(channels, "crash", "crash")
    channel = channels("sequence", control=control, max_restarts=2, log=log)
    for _ in range(2):
        with pytest.raises(WorkerCrashError):
            channel.call("environment", "probe", {}, _context(channels.tmp_path))
        assert channel.pid is None
    assert channel.degraded is True
    degraded = [fields for level, fields in log.entries if fields.get("event") == "plugin.degraded"]
    assert degraded == [{"event": "plugin.degraded", "plugin_id": "stub", "failures": 2}]
    with pytest.raises(PluginDegradedError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.pid is None
    assert channel.degraded is True


def test_success_resets_failure_counter(channels: _ChannelFactory) -> None:
    control = _control(channels, "crash", "ok", "crash", "crash")
    channel = channels("sequence", control=control)
    with pytest.raises(WorkerCrashError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.degraded is False
    assert channel.call("environment", "probe", {}, _context(channels.tmp_path)) == OK_VALUE
    for _ in range(2):
        with pytest.raises(WorkerCrashError):
            channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.degraded is False


def test_cancel_grace_kills_worker(channels: _ChannelFactory) -> None:
    cancel = _Cancel()
    # 调用超时留得比「发现取消 + 宽限」更长，否则先撞上超时分支。
    channel = channels("silent", call_timeout_seconds=0.5)
    channel.start()
    pid = channel.pid
    assert pid is not None
    timer = threading.Timer(CANCEL_DELAY_SECONDS, cancel.cancel)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(WorkerCancelledError, match="取消宽限期"):
            channel.call(
                "environment",
                "probe",
                {},
                _context(channels.tmp_path, cancel=cancel),
            )
    finally:
        timer.cancel()
    assert time.monotonic() - started >= CANCEL_GRACE_SECONDS
    assert channel.pid is None
    assert channel.degraded is False
    assert _process_ended(pid) is True


def test_cancel_does_not_count_as_restart_failure(channels: _ChannelFactory) -> None:
    """用户取消不是重启失败：max_restarts=1 时取消一次也不应降级。"""
    cancel = _Cancel()
    channel = channels("silent", call_timeout_seconds=0.5, max_restarts=1)
    channel.start()
    timer = threading.Timer(CANCEL_DELAY_SECONDS, cancel.cancel)
    timer.start()
    try:
        with pytest.raises(WorkerCancelledError):
            channel.call(
                "environment",
                "probe",
                {},
                _context(channels.tmp_path, cancel=cancel),
            )
    finally:
        timer.cancel()
    assert channel.degraded is False


@pytest.mark.parametrize(
    ("mode", "reason"),
    [("crash", "启动即退出"), ("fault", "加载失败")],
)
def test_start_failure_raises_crash(
    channels: _ChannelFactory,
    mode: str,
    reason: str,
) -> None:
    channel = channels(mode)
    with pytest.raises(WorkerCrashError, match=reason):
        channel.start()
    assert channel.pid is None
    assert channel.degraded is False


def test_callback_result_is_applied_by_context(channels: _ChannelFactory) -> None:
    recorded: list[tuple[str, Path, str]] = []

    def record_evidence(kind: str, path: Path, summary: str) -> str:
        recorded.append((kind, path, summary))
        return "ev-1"

    channel = channels("callback")
    value = channel.call(
        "verifier",
        "verify",
        {},
        _context(channels.tmp_path, record_evidence=record_evidence),
    )
    assert value == {"evidence": "ev-1"}
    assert recorded == [("log", Path("stub.log"), "stub")]


def test_notify_reaches_context_log(channels: _ChannelFactory) -> None:
    log = _RecordingLog()
    channel = channels("notify")
    value = channel.call("environment", "probe", {}, _context(channels.tmp_path, log=log))
    assert value == OK_VALUE
    assert ("info", {"message": "stub notify"}) in log.entries


def test_bad_json_counts_as_crash(channels: _ChannelFactory) -> None:
    channel = channels("badjson")
    channel.start()
    pid = channel.pid
    assert pid is not None
    with pytest.raises(WorkerCrashError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.pid is None
    assert channel.degraded is False
    assert _process_ended(pid) is True


def test_close_ends_worker(channels: _ChannelFactory) -> None:
    channel = channels("ok")
    channel.start()
    pid = channel.pid
    assert pid is not None
    channel.close()
    assert channel.pid is None
    assert _process_ended(pid) is True
