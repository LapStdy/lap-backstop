"""MaaScript 启动与停止的单元测试（docs/plugins/maa.md 脚本节）。

进程端口与等待全部用假件：不启动真实 MAA、不读系统时间、不真等。
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import RunContext, ScriptRef, ScriptState, StopMode
from plugins.maa.capabilities.script import MAA_EXE_NAME, MaaScript
from plugins.maa.process_host import clean_environment

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
SCRIPT = ScriptRef(id="script-1", plugin_id="maa")


class _Clock:
    """虚拟时钟：monotonic 可推进，utc_now 恒定。"""

    def __init__(self) -> None:
        self.monotonic_seconds = 0.0

    def monotonic(self) -> float:
        return self.monotonic_seconds

    def utc_now(self) -> datetime:
        return NOW


class _CancelFlag:
    def is_cancelled(self) -> bool:
        return False


class _SilentLogger:
    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


class _FakeProcessHost:
    """假进程表：记录调用，按预设的存活轮次决定何时退出。"""

    def __init__(
        self,
        existing: tuple[int, ...] = (),
        alive_checks: int = 0,
        stop_requested: bool = True,
        terminate_works: bool = True,
    ) -> None:
        self.existing = existing
        self.alive_checks = alive_checks
        self.stop_requested = stop_requested
        self.terminate_works = terminate_works
        self.started: list[tuple[Path, Path]] = []
        self.stop_calls: list[int] = []
        self.terminate_calls: list[int] = []
        self._alive = False

    def start(self, executable: Path, working_dir: Path) -> int:
        self.started.append((executable, working_dir))
        self._alive = True
        self._remaining = self.alive_checks
        return 4321

    def is_running(self, pid: int) -> bool:
        if not self._alive:
            return False
        if self._remaining <= 0:
            self._alive = False
            return False
        self._remaining -= 1
        return True

    def running_pids(self, image_name: str) -> tuple[int, ...]:
        return self.existing

    def request_stop(self, pid: int) -> bool:
        self.stop_calls.append(pid)
        return self.stop_requested

    def terminate(self, pid: int) -> None:
        self.terminate_calls.append(pid)
        if self.terminate_works:
            self._alive = False


def make_context(maa_dir: Path, clock: _Clock | None = None) -> RunContext:
    """构造指向临时 MAA 目录的上下文。"""
    active_clock = _Clock() if clock is None else clock
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={"maa_dir": str(maa_dir)},
        work_dir=Path("."),
        evidence_dir=Path("."),
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=active_clock,
        secrets={},
        log=_SilentLogger(),
        emit=lambda kind, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


def _script(host: _FakeProcessHost, clock: _Clock | None = None) -> tuple[MaaScript, _Clock]:
    """装配脚本能力与推进时钟的假等待。"""
    active_clock = _Clock() if clock is None else clock

    def wait(seconds: float) -> None:
        active_clock.monotonic_seconds += seconds

    return MaaScript(host=host, wait=wait), active_clock


def test_launch_starts_maa_and_records_offsets(tmp_path: Path) -> None:
    """launch 启动 MAA.exe 并把 pid 与日志起点写进句柄。"""
    debug = tmp_path / "debug"
    debug.mkdir()
    (debug / "gui.log").write_bytes(b"x" * 11)
    (debug / "asst.log").write_bytes(b"y" * 7)
    host = _FakeProcessHost()
    script, _clock = _script(host)
    handle = script.launch(SCRIPT, make_context(tmp_path))
    assert host.started == [(tmp_path / MAA_EXE_NAME, tmp_path)]
    assert handle.pid == 4321
    assert handle.meta["launched"] is True
    assert handle.meta["gui_log_offset"] == 11
    assert handle.meta["asst_log_offset"] == 7


def test_launch_refuses_when_maA_already_running(tmp_path: Path) -> None:
    """已有同类进程时不接管，直接失败，避免两个驱动者抢同一环境。"""
    host = _FakeProcessHost(existing=(999,))
    script, _clock = _script(host)
    with pytest.raises(RuntimeError, match="已在运行"):
        script.launch(SCRIPT, make_context(tmp_path))
    assert host.started == []


def test_stop_graceful_waits_for_self_exit(tmp_path: Path) -> None:
    """graceful 先发 WM_CLOSE；MAA 自行退出就算成功，不再结束进程树。"""
    host = _FakeProcessHost(alive_checks=1)
    script, _clock = _script(host)
    ctx = make_context(tmp_path)
    handle = script.launch(SCRIPT, ctx)
    result = script.stop(handle, ctx, StopMode.GRACEFUL)
    assert result.ok is True
    assert host.stop_calls == [4321]
    assert host.terminate_calls == []


def test_stop_graceful_falls_back_to_terminate(tmp_path: Path) -> None:
    """宽限期内没退出时结束整棵进程树，并确认退出。"""
    host = _FakeProcessHost(alive_checks=10_000, terminate_works=True)
    script, clock = _script(host)
    ctx = make_context(tmp_path, clock)
    handle = script.launch(SCRIPT, ctx)
    result = script.stop(handle, ctx, StopMode.GRACEFUL)
    assert result.ok is True
    assert host.terminate_calls == [4321]
    # 宽限期 10 秒按 0.2 秒轮询，注入时钟应推进这段预算。
    assert clock.monotonic_seconds >= 10.0


def test_stop_force_terminates_without_window_request(tmp_path: Path) -> None:
    """force 不发窗口消息，直接结束进程树。"""
    host = _FakeProcessHost(alive_checks=10_000)
    script, _clock = _script(host)
    ctx = make_context(tmp_path)
    handle = script.launch(SCRIPT, ctx)
    result = script.stop(handle, ctx, StopMode.FORCE)
    assert result.ok is True
    assert host.stop_calls == []
    assert host.terminate_calls == [4321]


def test_stop_reports_failure_when_process_survives_kill(tmp_path: Path) -> None:
    """结束进程树后仍存活：返回失败并带 E_SCRIPT，不谎报成功。"""
    host = _FakeProcessHost(alive_checks=10_000, terminate_works=False)
    script, clock = _script(host)
    ctx = make_context(tmp_path, clock)
    handle = script.launch(SCRIPT, ctx)
    result = script.stop(handle, ctx, StopMode.FORCE)
    assert result.ok is False
    assert result.error_code == "E_SCRIPT"


def test_stop_is_idempotent_when_process_already_gone(tmp_path: Path) -> None:
    """进程已退出时重复 stop 仍成功，不重复发结束命令。"""
    host = _FakeProcessHost(alive_checks=0)
    script, _clock = _script(host)
    ctx = make_context(tmp_path)
    handle = script.launch(SCRIPT, ctx)
    first = script.stop(handle, ctx, StopMode.FORCE)
    second = script.stop(handle, ctx, StopMode.FORCE)
    assert first.ok is True
    assert second.ok is True
    assert host.terminate_calls == []


def test_stop_marks_run_exited_for_observe(tmp_path: Path) -> None:
    """stop 之后 observe 报 EXITED：停止也算一次终态。"""
    host = _FakeProcessHost(alive_checks=0)
    script, _clock = _script(host)
    ctx = make_context(tmp_path)
    handle = script.launch(SCRIPT, ctx)
    script.stop(handle, ctx, StopMode.FORCE)
    assert script.observe(handle, ctx).state is ScriptState.EXITED


def test_clean_environment_drops_proxy_variables() -> None:
    """启动环境剔除代理变量，其余变量原样保留。"""
    environment = clean_environment(
        {
            "HTTP_PROXY": "http://127.0.0.1:10809",
            "HTTPS_PROXY": "http://127.0.0.1:10809",
            "ALL_PROXY": "socks5://127.0.0.1:10808",
            "http_proxy": "x",
            "PATH": "C:/Windows",
        }
    )
    assert environment == {"PATH": "C:/Windows"}
