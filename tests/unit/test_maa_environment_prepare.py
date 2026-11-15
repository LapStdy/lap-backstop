"""MaaEnvironment.prepare 的单元测试（docs/plugins/maa.md 环境节）。

模拟器动作、真机读数与帧源全部用假件：不启动实例、不连 adb、不拉起游戏、
不截真帧；主界面判定输入用冻结样本重建的满帧。
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import EnvironmentRef, RunContext
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.mumu_instance import MumuInstance
from plugins.maa.vision.frames import Frame, FrameSource
from plugins.maa.windows_host import ScreenSize
from tests.maa_vision_support import fixture_frame, load_manifest

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
ENV = EnvironmentRef(id="env-1", provider_id="maa")
GAME = "com.hypergryph.arknights"


class _Clock:
    """虚拟时钟：monotonic 由假等待推进，utc_now 恒定。"""

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
    """按轮次给读数的假真机端口：adb 状态与前台包名各给一个序列。"""

    def __init__(
        self,
        titles: tuple[str, ...] = ("MuMuNxMain",),
        adb_states: tuple[str, ...] = ("device",),
        sizes: tuple[ScreenSize | None, ...] = (ScreenSize(1280, 720),),
        packages: tuple[str | None, ...] = (GAME,),
    ) -> None:
        self._titles = titles
        self._adb = list(adb_states)
        self._sizes = list(sizes)
        self._packages = list(packages)
        self._adb_seen = 0
        self._size_seen = 0
        self._package_seen = 0
        self.instance: MumuInstance | None = None

    def window_titles(self) -> tuple[str, ...]:
        return self._titles

    def instance_state(self, index: int) -> MumuInstance | None:
        return self.instance

    def adb_device_state(self) -> str:
        return self._next(self._adb, "_adb_seen")

    def screencap_size(self) -> ScreenSize | None:
        return self._next(self._sizes, "_size_seen")

    def foreground_package(self) -> str | None:
        return self._next(self._packages, "_package_seen")

    def _next(self, values: list[Any], counter: str) -> Any:
        index = getattr(self, counter)
        setattr(self, counter, index + 1)
        return values[min(index, len(values) - 1)]


class _FakeActions:
    """记录动作调用的假模拟器端口。"""

    def __init__(self, package_running: bool = True) -> None:
        self.package_running = package_running
        self.calls: list[str] = []
        self.failure: OSError | None = None

    def start_instance(self, instance_index: int) -> None:
        self.calls.append(f"start:{instance_index}")
        if self.failure is not None:
            raise self.failure

    def connect_adb(self) -> None:
        self.calls.append("connect")

    def is_package_running(self, package: str) -> bool:
        return self.package_running

    def launch_package(self, package: str) -> None:
        self.calls.append(f"launch:{package}")

    def stop_package(self, package: str) -> None:
        self.calls.append(f"stop:{package}")

    def disconnect_adb(self) -> None:
        self.calls.append("disconnect")

    def shutdown_instance(self, instance_index: int) -> None:
        self.calls.append(f"shutdown:{instance_index}")


def make_context(
    config: Mapping[str, Any] | None = None, evidence_dir: Path | None = None
) -> RunContext:
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={} if config is None else dict(config),
        work_dir=Path(".") if evidence_dir is None else evidence_dir,
        evidence_dir=Path(".") if evidence_dir is None else evidence_dir,
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=_SilentLogger(),
        emit=lambda kind, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


class _RepeatFrames:
    """每次截帧返回同一满帧的假帧源：门控两帧确认用同一帧。"""

    def __init__(self, frame: Frame) -> None:
        self._frame = frame
        self.captures = 0

    def capture(self) -> Frame | None:
        self.captures += 1
        return self._frame


def _home_frames() -> FrameSource:
    """冻结样本第一帧主界面重建的满帧源，prepare 成功用例的门控输入。"""
    homes = [entry for entry in load_manifest()["frames"] if entry["expected"] == "home"]
    return _RepeatFrames(fixture_frame(homes[0]))


def make_provider(
    host: _FakeHost,
    actions: _FakeActions,
    ctx: RunContext,
    frames: FrameSource | None = None,
) -> MaaEnvironment:
    """装配环境能力；等待只推进该上下文注入的假时钟，测试不真等。"""

    def wait(seconds: float) -> None:
        ctx.clock.monotonic_seconds += seconds  # type: ignore[attr-defined]

    return MaaEnvironment(
        host=host,
        actions=actions,
        wait=wait,
        frames=frames if frames is not None else _home_frames(),
    )


def test_prepare_reuses_running_instance_without_actions(tmp_path: Path) -> None:
    """实例已在跑、adb 在线、游戏在前台：prepare 不发任何动作。"""
    actions = _FakeActions()
    ctx = make_context(evidence_dir=tmp_path)
    result = make_provider(_FakeHost(), actions, ctx).prepare(ENV, ctx)
    assert result.ok is True
    assert actions.calls == []
    assert result.started_at == NOW


def test_prepare_starts_instance_when_window_missing(tmp_path: Path) -> None:
    """窗口不存在时先拉起实例，再等 adb 上线。"""
    actions = _FakeActions()
    ctx = make_context(evidence_dir=tmp_path)
    result = make_provider(_FakeHost(titles=()), actions, ctx).prepare(ENV, ctx)
    assert result.ok is True
    assert actions.calls[0] == "start:0"


def test_prepare_starts_instance_when_launcher_window_open(tmp_path: Path) -> None:
    """控制窗口在、实例没起来：仍要拉起实例，不白等 adb 超时。"""
    actions = _FakeActions()
    ctx = make_context(evidence_dir=tmp_path)
    host = _FakeHost()
    host.instance = MumuInstance(0, "明日方舟", "15.0", False, False)
    result = make_provider(host, actions, ctx).prepare(ENV, ctx)
    assert result.ok is True
    assert actions.calls[0] == "start:0"


def test_prepare_skips_start_when_instance_already_running(tmp_path: Path) -> None:
    """info 说实例在跑时不重复拉起，即便窗口标题没匹配上。"""
    actions = _FakeActions()
    ctx = make_context(evidence_dir=tmp_path)
    host = _FakeHost(titles=("记事本",))
    host.instance = MumuInstance(0, "明日方舟", "15.0", True, True)
    result = make_provider(host, actions, ctx).prepare(ENV, ctx)
    assert result.ok is True
    assert actions.calls == []


def test_prepare_reconnects_adb_when_offline(tmp_path: Path) -> None:
    """adb 不在线时先重连；连上后继续准备。"""
    actions = _FakeActions()
    ctx = make_context(evidence_dir=tmp_path)
    host = _FakeHost(adb_states=("offline", "device"))
    result = make_provider(host, actions, ctx).prepare(ENV, ctx)
    assert result.ok is True
    assert "connect" in actions.calls


def test_prepare_fails_when_adb_never_online() -> None:
    """adb 在配置时限内一直连不上：失败并给 E_ENV_UNREACHABLE。"""
    actions = _FakeActions()
    ctx = make_context({"mumu_boot_timeout_s": 5})
    host = _FakeHost(adb_states=("absent",))
    result = make_provider(host, actions, ctx).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_UNREACHABLE"


def test_prepare_fails_when_screencap_missing() -> None:
    """adb 在线但截不到帧：按环境不可用失败，不继续拉游戏。"""
    actions = _FakeActions()
    ctx = make_context()
    host = _FakeHost(sizes=(None,))
    result = make_provider(host, actions, ctx).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_UNREACHABLE"
    assert actions.calls == []


def test_prepare_launches_game_when_not_running(tmp_path: Path) -> None:
    """游戏进程不在时拉起，等它到前台。"""
    actions = _FakeActions(package_running=False)
    ctx = make_context(evidence_dir=tmp_path)
    host = _FakeHost(packages=("app.lawnchair", GAME))
    result = make_provider(host, actions, ctx).prepare(ENV, ctx)
    assert result.ok is True
    assert actions.calls == [f"launch:{GAME}"]


def test_prepare_fails_when_game_never_foreground() -> None:
    """游戏在时限内没到前台：失败并给 E_ENV_NOT_READY。"""
    actions = _FakeActions(package_running=False)
    ctx = make_context({"game_launch_timeout_s": 3})
    host = _FakeHost(packages=("app.lawnchair",))
    result = make_provider(host, actions, ctx).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"


def test_prepare_reports_action_failure_as_unreachable() -> None:
    """动作端口抛 OSError（如 MuMuManager 起不来）：折成 E_ENV_UNREACHABLE。"""
    actions = _FakeActions()
    actions.failure = OSError("MuMuManager 启动失败")
    ctx = make_context()
    result = make_provider(_FakeHost(titles=()), actions, ctx).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_UNREACHABLE"
    assert "MuMuManager" in result.detail
