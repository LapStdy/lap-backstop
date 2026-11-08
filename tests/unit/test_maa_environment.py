"""MaaEnvironment 探测的单元测试（docs/plugins/maa.md 环境节）。

全部用假真机端口：不连 adb、不枚举真实窗口、不读系统时间。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import EnvironmentRef, EnvironmentState, RunContext
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.windows_host import ScreenSize

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
ENV = EnvironmentRef(id="env-1", provider_id="maa")


class _Clock:
    """虚拟时钟：时间恒定。"""

    def monotonic(self) -> float:
        return 0.0

    def utc_now(self) -> datetime:
        return NOW


class _CancelFlag:
    """取消令牌假件：永不取消。"""

    def is_cancelled(self) -> bool:
        return False


class _SilentLogger:
    """结构化日志假件：丢弃所有输出。"""

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


class _FakeHost:
    """可调的真机读数假件：四个方法都只返回预置值。"""

    def __init__(
        self,
        titles: Sequence[str] = ("MuMuPlayer-12.0",),
        adb_state: str = "device",
        size: ScreenSize | None = None,
        package: str | None = "com.hypergryph.arknights",
        screencap_ok: bool = True,
    ) -> None:
        self._titles = tuple(titles)
        self._adb_state = adb_state
        self._size = ScreenSize(width=1280, height=720) if size is None else size
        self._package = package
        self._screencap_ok = screencap_ok

    def window_titles(self) -> Sequence[str]:
        return self._titles

    def adb_device_state(self) -> str:
        return self._adb_state

    def screencap_size(self) -> ScreenSize | None:
        return self._size if self._screencap_ok else None

    def foreground_package(self) -> str | None:
        return self._package


def make_context(config: Mapping[str, Any] | None = None) -> RunContext:
    """构造最小上下文；插件配置可注入。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={"mumu_dir": "H:/MuMuPlayer"} if config is None else dict(config),
        work_dir=Path("."),
        evidence_dir=Path("."),
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=_SilentLogger(),
        emit=lambda kind, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


def test_probe_reports_off_without_emulator_window() -> None:
    """窗口不存在时环境未启动，其余读数不参与判定。"""
    provider = MaaEnvironment(host=_FakeHost(titles=("记事本",), adb_state="absent"))
    probe = provider.probe(ENV, make_context())
    assert probe.state is EnvironmentState.OFF
    assert probe.observed_at == NOW
    assert "窗口" in probe.detail


def test_probe_reports_booting_when_window_exists_but_adb_absent() -> None:
    """窗口在、adb 未连接：模拟器正在启动。"""
    provider = MaaEnvironment(host=_FakeHost(adb_state="absent"))
    probe = provider.probe(ENV, make_context())
    assert probe.state is EnvironmentState.BOOTING
    assert "127.0.0.1:16384" in probe.detail


def test_probe_reports_degraded_when_adb_offline() -> None:
    """adb 报告 offline 时环境不可执行。"""
    provider = MaaEnvironment(host=_FakeHost(adb_state="offline"))
    probe = provider.probe(ENV, make_context())
    assert probe.state is EnvironmentState.DEGRADED
    assert "offline" in probe.detail


def test_probe_reports_degraded_when_screencap_missing() -> None:
    """adb 在线但截不到帧，按证据缺失降级，不猜。"""
    host = _FakeHost(screencap_ok=False)
    probe = MaaEnvironment(host=host).probe(ENV, make_context())
    assert probe.state is EnvironmentState.DEGRADED
    assert "截图" in probe.detail


def test_probe_reports_degraded_when_game_not_foreground() -> None:
    """前台是桌面时不放行，detail 写明当前包名。"""
    provider = MaaEnvironment(host=_FakeHost(package="app.lawnchair"))
    probe = provider.probe(ENV, make_context())
    assert probe.state is EnvironmentState.DEGRADED
    assert "app.lawnchair" in probe.detail


def test_probe_reports_failed_on_resolution_mismatch() -> None:
    """游戏在前台但分辨率不符：停手并给人看，不自动改设置。"""
    provider = MaaEnvironment(host=_FakeHost(size=ScreenSize(width=720, height=1280)))
    probe = provider.probe(ENV, make_context())
    assert probe.state is EnvironmentState.FAILED
    assert "720x1280" in probe.detail


def test_probe_reports_ready_when_all_four_checks_pass() -> None:
    """四项都满足才算就绪；detail 带上地址、包名与分辨率。"""
    provider = MaaEnvironment(host=_FakeHost())
    probe = provider.probe(ENV, make_context())
    assert probe.state is EnvironmentState.READY
    assert "127.0.0.1:16384" in probe.detail
    assert "com.hypergryph.arknights" in probe.detail
    assert "1280x720" in probe.detail


def test_probe_reads_config_overrides() -> None:
    """窗口标题、包名与分辨率按插件配置读取，不写死。"""
    config = {
        "mumu_dir": "H:/MuMuPlayer",
        "emulator_window_title": "MuMuNx",
        "game_package": "com.example.game",
        "screen_size": "1920x1080",
    }
    host = _FakeHost(
        titles=("MuMuNxMain",), package="com.example.game", size=ScreenSize(1920, 1080)
    )
    probe = MaaEnvironment(host=host).probe(ENV, make_context(config))
    assert probe.state is EnvironmentState.READY


def test_probe_falls_back_on_broken_screen_size_config() -> None:
    """分辨率写法非法时退回默认值，探测本身不失败。"""
    host = _FakeHost()
    probe = MaaEnvironment(host=host).probe(ENV, make_context({"screen_size": "坏值"}))
    assert probe.state is EnvironmentState.READY


def test_probe_is_repeatable_without_side_effects() -> None:
    """同一输入重复探测结果一致：只读端口不应有状态。"""
    provider = MaaEnvironment(host=_FakeHost())
    ctx = make_context()
    assert provider.probe(ENV, ctx) == provider.probe(ENV, ctx)
