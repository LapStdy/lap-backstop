"""worker 通道的单元用例：调用往返、故障注入与降级，以及 emit 契约与门面行数钉界。

全部用例都用真实子进程与真实单调时间，每条只盯一条隔离约束：错误类型、进程号是否
变 None、降级标记，以及进程是否真的被结束。短超时保证整套远低于十秒。
"""

from __future__ import annotations

import os
import sys
import threading
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

from core.plugins import channel as channel_module
from core.plugins.channel import WorkerChannel, _worker_launch
from core.plugins.errors import (
    PluginDegradedError,
    WorkerCallError,
    WorkerCancelledError,
    WorkerCrashError,
    WorkerTimeoutError,
)
from core.plugins.worker_options import WorkerConfig
from tests.unit.worker_channel_support import (
    CALL_TIMEOUT_SECONDS,
    CANCEL_DELAY_SECONDS,
    CANCEL_GRACE_SECONDS,
    DEGRADED_RETRY_SECONDS,
    OK_VALUE,
    PLUGIN_FIXTURES,
    _Cancel,
    _ChannelFactory,
    _ChannelOptions,
    _context,
    _ContextDeps,
    _control,
    _env_args,
    _limits,
    _process_ended,
    _RecordingLog,
)


@pytest.fixture
def channels(tmp_path: Path) -> Iterator[_ChannelFactory]:
    factory = _ChannelFactory(tmp_path)
    try:
        yield factory
    finally:
        factory.close()


def test_start_declares_capabilities(channels: _ChannelFactory) -> None:
    channel = channels("ok")
    capabilities = channel.start()
    assert capabilities == frozenset({"environment"})
    assert channel.pid is not None
    assert channel.pid != os.getpid()
    assert channel.is_degraded is False


def test_call_returns_worker_value(channels: _ChannelFactory) -> None:
    channel = channels("ok")
    value = channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert value == OK_VALUE
    assert channel.pid is not None
    assert channel.is_degraded is False


def test_exchange_call_roundtrip_returns_worker_value(channels: _ChannelFactory) -> None:
    """消息往返落在交换模块：发请求、等到本调用 id 的结果。"""
    channel = channels("ok")
    channel.start()
    value = channel._exchange.call("environment", "probe", {}, _context(channels.tmp_path))
    assert value == OK_VALUE


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
    assert channel.is_degraded is False
    assert _process_ended(pid) is True


def test_crash_then_next_call_restarts_worker(channels: _ChannelFactory) -> None:
    control = _control(channels, "crash", "ok")
    channel = channels("sequence", _ChannelOptions(control=control))
    channel.start()
    first_pid = channel.pid
    assert first_pid is not None
    with pytest.raises(WorkerCrashError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.pid is None
    assert _process_ended(first_pid) is True
    assert channel.is_degraded is False
    value = channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert value == OK_VALUE
    assert channel.pid is not None
    assert channel.pid != first_pid
    assert channel.is_degraded is False


def test_repeated_failures_degrade_channel(channels: _ChannelFactory) -> None:
    log = _RecordingLog()
    control = _control(channels, "crash", "crash")
    channel = channels("sequence", _ChannelOptions(control=control, max_restarts=2, log=log))
    for _ in range(2):
        with pytest.raises(WorkerCrashError):
            channel.call("environment", "probe", {}, _context(channels.tmp_path))
        assert channel.pid is None
    assert channel.is_degraded is True
    degraded = [fields for level, fields in log.entries if fields.get("event") == "plugin.degraded"]
    assert degraded[0]["failures"] == 2
    assert degraded[0]["retry_after_seconds"] == DEGRADED_RETRY_SECONDS
    with pytest.raises(PluginDegradedError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.pid is None
    assert channel.is_degraded is True


def test_degradation_reason_reflects_last_failure(channels: _ChannelFactory) -> None:
    """降级原因实时反映触发降级的失败形态；未降级时为 None。"""
    control = _control(channels, "crash", "crash")
    channel = channels("sequence", _ChannelOptions(control=control, max_restarts=2))
    assert channel.degradation_reason is None
    for _ in range(2):
        with pytest.raises(WorkerCrashError):
            channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.degradation_reason == "连续 2 次失败（调用中退出）"
    with pytest.raises(PluginDegradedError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.degradation_reason == "连续 2 次失败（调用中退出）"


def test_success_resets_failure_counter(channels: _ChannelFactory) -> None:
    control = _control(channels, "crash", "ok", "crash", "crash")
    channel = channels("sequence", _ChannelOptions(control=control))
    with pytest.raises(WorkerCrashError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.is_degraded is False
    assert channel.call("environment", "probe", {}, _context(channels.tmp_path)) == OK_VALUE
    for _ in range(2):
        with pytest.raises(WorkerCrashError):
            channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.is_degraded is False


def test_session_fail_discards_process_and_counts_degradation(
    channels: _ChannelFactory,
) -> None:
    """失败收尾落在会话模块：整树结束进程，连续失败计入降级预算并记因。"""
    log = _RecordingLog()
    channel = channels("ok", _ChannelOptions(max_restarts=2, log=log))
    channel.start()
    session = channel._session
    pid = channel.pid
    assert pid is not None
    session.fail_process(reason="测试收尾")
    assert session.pid is None
    assert _process_ended(pid) is True
    session.fail_process(reason="测试收尾")
    assert session.is_degraded is True
    assert session.degradation_reason == "连续 2 次失败（测试收尾）"
    degraded = [fields for level, fields in log.entries if fields.get("event") == "plugin.degraded"]
    assert degraded[-1]["reason"] == "测试收尾"


def test_cancel_grace_kills_worker(channels: _ChannelFactory) -> None:
    cancel = _Cancel()
    # 调用超时留得比「发现取消 + 宽限」更长，否则先撞上超时分支。
    channel = channels("silent", _ChannelOptions(call_timeout_seconds=0.5))
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
                _context(channels.tmp_path, _ContextDeps(cancel=cancel)),
            )
    finally:
        timer.cancel()
    assert time.monotonic() - started >= CANCEL_GRACE_SECONDS
    assert channel.pid is None
    assert channel.is_degraded is False
    assert _process_ended(pid) is True


def test_cancel_does_not_count_as_restart_failure(channels: _ChannelFactory) -> None:
    """用户取消不是重启失败：max_restarts=1 时取消一次也不应降级。"""
    cancel = _Cancel()
    channel = channels("silent", _ChannelOptions(call_timeout_seconds=0.5, max_restarts=1))
    channel.start()
    timer = threading.Timer(CANCEL_DELAY_SECONDS, cancel.cancel)
    timer.start()
    try:
        with pytest.raises(WorkerCancelledError):
            channel.call(
                "environment",
                "probe",
                {},
                _context(channels.tmp_path, _ContextDeps(cancel=cancel)),
            )
    finally:
        timer.cancel()
    assert channel.is_degraded is False


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
    assert channel.is_degraded is False


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
        _context(channels.tmp_path, _ContextDeps(record_evidence=record_evidence)),
    )
    assert value == {"evidence": "ev-1"}
    assert recorded == [("log", Path("stub.log"), "stub")]


def test_notify_reaches_context_log(channels: _ChannelFactory) -> None:
    log = _RecordingLog()
    channel = channels("notify")
    value = channel.call(
        "environment", "probe", {}, _context(channels.tmp_path, _ContextDeps(log=log))
    )
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
    assert channel.is_degraded is False
    assert _process_ended(pid) is True


def test_close_ends_worker(channels: _ChannelFactory) -> None:
    channel = channels("ok")
    channel.start()
    pid = channel.pid
    assert pid is not None
    channel.close()
    assert channel.pid is None
    assert _process_ended(pid) is True


def test_timeout_kills_process_tree(tmp_path: Path) -> None:
    """超时强杀要连插件拉起的子进程一起结束，不能留下孤儿。"""
    channel = WorkerChannel(
        "child",
        WorkerConfig(plugins_root=PLUGIN_FIXTURES, limits=_limits(call_timeout_seconds=0.8)),
    )
    try:
        with pytest.raises(WorkerTimeoutError):
            channel.call(
                "environment",
                "probe",
                {"env": {"id": "env-1", "provider_id": "child", "config": {}}},
                _context(tmp_path),
            )
        child_pid = int((tmp_path / "work" / "child.pid").read_text(encoding="utf-8"))
        assert _process_ended(child_pid) is True
    finally:
        channel.close()


def test_degraded_recovers_after_retry_window(channels: _ChannelFactory) -> None:
    """降级不是终态：重试窗口过后通道重新拉起 worker 并清零失败计数。"""
    log = _RecordingLog()
    control = _control(channels, "crash", "ok")
    channel = channels(
        "sequence",
        _ChannelOptions(control=control, max_restarts=1, degraded_retry_seconds=0.2, log=log),
    )
    with pytest.raises(WorkerCrashError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    assert channel.is_degraded is True
    with pytest.raises(PluginDegradedError):
        channel.call("environment", "probe", {}, _context(channels.tmp_path))
    time.sleep(0.3)
    assert channel.is_degraded is False
    assert channel.call("environment", "probe", {}, _context(channels.tmp_path)) == OK_VALUE
    recovered = [
        fields for level, fields in log.entries if fields.get("event") == "plugin.recovered"
    ]
    assert recovered


def test_emit_allows_run_signal(tmp_path: Path) -> None:
    recorded: list[tuple[str, Mapping[str, Any]]] = []

    def emit(event: str, payload: Mapping[str, Any]) -> None:
        recorded.append((event, payload))

    channel = WorkerChannel("emitter", WorkerConfig(plugins_root=PLUGIN_FIXTURES, limits=_limits()))
    try:
        value = channel.call(
            "environment",
            "probe",
            _env_args("emitter"),
            _context(
                tmp_path,
                _ContextDeps(
                    emit=emit,
                    config={
                        "event": "run.signal",
                        "payload": {"kind": "progress", "payload": {"p": 1}},
                    },
                ),
            ),
        )
    finally:
        channel.close()
    assert isinstance(value, dict)
    assert value["detail"] == "emitted"
    assert recorded == [("run.signal", {"kind": "progress", "payload": {"p": 1}})]


@pytest.mark.parametrize(
    ("event", "payload", "reason"),
    [
        ("run.finished", {}, "内核事件"),
        ("not.an.event", {}, "未知事件类型"),
        ("run.signal", {"payload": {}}, "kind"),
    ],
)
def test_emit_rejects_events_outside_contract(
    tmp_path: Path,
    event: str,
    payload: dict[str, Any],
    reason: str,
) -> None:
    channel = WorkerChannel("emitter", WorkerConfig(plugins_root=PLUGIN_FIXTURES, limits=_limits()))
    try:
        with pytest.raises(WorkerCallError, match=reason):
            channel.call(
                "environment",
                "probe",
                _env_args("emitter"),
                _context(
                    tmp_path,
                    _ContextDeps(config={"event": event, "payload": payload}),
                ),
            )
    finally:
        channel.close()


def test_worker_launch_source_uses_module_entry() -> None:
    """源码运行的默认命令是 -m 模块入口，工作目录是仓库根。"""
    command, cwd = _worker_launch(False)
    assert command == [sys.executable, "-u", "-m", "core.plugins.worker"]
    assert cwd == Path(__file__).resolve().parents[2]


def test_worker_launch_frozen_uses_exe_subcommand() -> None:
    """打包件的默认命令是 exe 自身的 worker 子命令，工作目录是 exe 所在目录。"""
    command, cwd = _worker_launch(True)
    assert command == [sys.executable, "worker"]
    assert cwd == Path(sys.executable).resolve().parent


def test_channel_picks_launch_branch_by_frozen_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """默认命令按 sys.frozen 取分支；注入假冻结环境后应走 exe 回调形态。"""
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    channel = WorkerChannel("fake", WorkerConfig(plugins_root=tmp_path))
    assert channel._command == [sys.executable, "worker"]
    assert channel._cwd == Path(sys.executable).resolve().parent


def test_channel_module_stays_within_line_limit() -> None:
    """门面模块行数钉在 docs/11 的 500 行上限内。"""
    lines = Path(channel_module.__file__).read_text(encoding="utf-8").splitlines()
    assert len(lines) <= 500, f"channel.py 现有 {len(lines)} 行，超过上限 500"
