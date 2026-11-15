"""MaaEnvironment 复位/清理/保温的单元测试（docs/plugins/maa.md 环境节）。

模拟器动作与真机读数全部用假件：不关游戏、不关实例、不真等。
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import EnvironmentRef, RunContext
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.mumu_instance import MumuInstance
from plugins.maa.windows_host import ScreenSize

REQS = ("M-108",)

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
ENV = EnvironmentRef(id="env-1", provider_id="maa")
GAME = "com.hypergryph.arknights"
LAUNCHER = "app.lawnchair"


class _Clock:
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


class _FakeHost:
    """可调读数：adb 状态固定，前台包名按序列给出。"""

    def __init__(
        self,
        adb_state: str = "device",
        packages: tuple[str | None, ...] = (GAME,),
    ) -> None:
        self._adb_state = adb_state
        self._packages = list(packages)
        self._seen = 0

    def window_titles(self) -> tuple[str, ...]:
        return ("MuMuNxMain",)

    def instance_state(self, index: int) -> MumuInstance | None:
        return None

    def adb_device_state(self) -> str:
        return self._adb_state

    def screencap_size(self) -> ScreenSize | None:
        return ScreenSize(1280, 720)

    def foreground_package(self) -> str | None:
        index = min(self._seen, len(self._packages) - 1)
        self._seen += 1
        return self._packages[index]


class _FakeActions:
    """记录动作调用；可让某个动作抛 OSError。"""

    def __init__(self, package_running: bool = True) -> None:
        self.package_running = package_running
        self.calls: list[str] = []
        self.failure: OSError | None = None

    def start_instance(self, instance_index: int) -> None:
        self.calls.append(f"start:{instance_index}")

    def connect_adb(self) -> None:
        self.calls.append("connect")

    def disconnect_adb(self) -> None:
        self.calls.append("disconnect")

    def is_package_running(self, package: str) -> bool:
        return self.package_running

    def launch_package(self, package: str) -> None:
        self.calls.append(f"launch:{package}")

    def stop_package(self, package: str) -> None:
        self.calls.append(f"stop:{package}")

    def shutdown_instance(self, instance_index: int) -> None:
        self.calls.append(f"shutdown:{instance_index}")
        if self.failure is not None:
            raise self.failure


def make_context(config: Mapping[str, Any] | None = None) -> tuple[RunContext, _Clock]:
    clock = _Clock()
    ctx = RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={} if config is None else dict(config),
        work_dir=Path("."),
        evidence_dir=Path("."),
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=clock,
        secrets={},
        log=_SilentLogger(),
        emit=lambda kind, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )
    return ctx, clock


def make_provider(host: _FakeHost, actions: _FakeActions, ctx: RunContext) -> MaaEnvironment:
    def wait(seconds: float) -> None:
        ctx.clock.monotonic_seconds += seconds  # type: ignore[attr-defined]

    return MaaEnvironment(host=host, actions=actions, wait=wait)


def test_reset_disconnects_reconnects_and_restarts_game() -> None:
    """reset 断开再重连 adb，结束游戏后重新拉起并等前台。"""
    actions = _FakeActions(package_running=False)
    ctx, _clock = make_context()
    provider = make_provider(_FakeHost(packages=(LAUNCHER, GAME)), actions, ctx)
    result = provider.reset(ENV, ctx)
    assert result.ok is True
    assert actions.calls == [
        "disconnect",
        "connect",
        f"stop:{GAME}",
        f"launch:{GAME}",
    ]


def test_reset_fails_when_adb_never_online() -> None:
    """复位后 adb 连不上：失败并给 E_ENV_DISCONNECTED。"""
    actions = _FakeActions()
    ctx, _clock = make_context({"mumu_boot_timeout_s": 3})
    provider = make_provider(_FakeHost(adb_state="offline"), actions, ctx)
    result = provider.reset(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_DISCONNECTED"


def test_reset_fails_when_game_never_foreground() -> None:
    """复位后游戏没回到前台：失败并给 E_ENV_NOT_READY。"""
    actions = _FakeActions(package_running=False)
    ctx, _clock = make_context({"game_launch_timeout_s": 3})
    provider = make_provider(_FakeHost(packages=(LAUNCHER,)), actions, ctx)
    result = provider.reset(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"


def test_cleanup_keep_warm_leaves_game_and_instance_running() -> None:
    """keep_warm=True 时不动游戏与实例。"""
    actions = _FakeActions()
    ctx, _clock = make_context()
    result = make_provider(_FakeHost(), actions, ctx).cleanup(ENV, ctx, keep_warm=True)
    assert result.ok is True
    assert actions.calls == []


def test_cleanup_cold_stops_game_and_shuts_instance() -> None:
    """keep_warm=False 时先结束游戏再关实例。"""
    actions = _FakeActions()
    ctx, _clock = make_context()
    result = make_provider(_FakeHost(), actions, ctx).cleanup(ENV, ctx, keep_warm=False)
    assert result.ok is True
    assert actions.calls == [f"stop:{GAME}", "shutdown:0"]


def test_cleanup_reports_shutdown_failure() -> None:
    """关闭实例失败：返回 E_ENV_UNREACHABLE，不谎报已清理。"""
    actions = _FakeActions()
    actions.failure = OSError("MuMuManager shutdown 失败")
    ctx, _clock = make_context()
    result = make_provider(_FakeHost(), actions, ctx).cleanup(ENV, ctx, keep_warm=False)
    assert result.ok is False
    assert result.error_code == "E_ENV_UNREACHABLE"


def test_warm_leaves_foreground_game_alone() -> None:
    """游戏已在前台：保温不发任何动作。"""
    actions = _FakeActions()
    ctx, _clock = make_context()
    result = make_provider(_FakeHost(), actions, ctx).warm(ENV, ctx, minutes=30)
    assert result.ok is True
    assert actions.calls == []


def test_warm_relaunches_game_when_not_foreground() -> None:
    """游戏在后台且进程不在时重新拉起并等前台。"""
    actions = _FakeActions(package_running=False)
    ctx, _clock = make_context()
    provider = make_provider(_FakeHost(packages=(LAUNCHER, GAME)), actions, ctx)
    result = provider.warm(ENV, ctx, minutes=30)
    assert result.ok is True
    assert actions.calls == [f"launch:{GAME}"]


def test_warm_fails_when_game_never_foreground() -> None:
    """保温时限内游戏没到前台：失败并给 E_ENV_NOT_READY。"""
    actions = _FakeActions(package_running=False)
    ctx, _clock = make_context({"game_launch_timeout_s": 3})
    provider = make_provider(_FakeHost(packages=(LAUNCHER,)), actions, ctx)
    result = provider.warm(ENV, ctx, minutes=30)
    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
