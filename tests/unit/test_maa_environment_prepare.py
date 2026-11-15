"""MaaEnvironment.prepare 的单元测试（docs/plugins/maa.md 环境节）。

模拟器动作、真机读数与帧源全部用假件：不启动实例、不连 adb、不拉起游戏、
不截真帧；主界面判定输入用冻结样本重建的满帧；登录后导航用例用冻结样本帧
驱动，覆盖层停手用黑画布贴签到锚点模板的合成帧，返回键经假动作端口计数。
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from contracts import EnvironmentRef, RunContext
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.mumu_instance import MumuInstance
from plugins.maa.vision.frames import Frame, FrameOrigin, FrameSource
from plugins.maa.vision.judge import DEFAULT_TEMPLATE_DIR, vision_modules
from plugins.maa.vision.login_vision import DAILY_SIGNIN_TEMPLATE
from plugins.maa.windows_host import ScreenSize
from tests.maa_vision_support import fixture_frame, frame_entry, load_manifest

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
    """记录动作调用的假模拟器端口；返回键与实例动作可分别注入失败。"""

    def __init__(self, package_running: bool = True) -> None:
        self.package_running = package_running
        self.calls: list[str] = []
        self.failure: OSError | None = None
        self.back_failure: OSError | None = None

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

    def send_back(self) -> None:
        self.calls.append("back")
        if self.back_failure is not None:
            raise self.back_failure

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


class _ScriptedFrames:
    """按脚本逐帧返回冻结样本帧（None 表示截帧失败），记录截帧次数。"""

    def __init__(self, frames: Sequence[Frame | None]) -> None:
        self._frames = list(frames)
        self.calls = 0

    def capture(self) -> Frame | None:
        assert self.calls < len(self._frames), "截帧次数超出脚本长度"
        frame = self._frames[self.calls]
        self.calls += 1
        return frame


def _named_frame(name: str) -> Frame:
    """按 manifest 帧名重建冻结样本帧，导航用例的判定输入。"""
    return fixture_frame(frame_entry(load_manifest(), name))


def _overlay_frame() -> Frame:
    """黑画布贴签到锚点模板的合成帧：登录判读认出的每日签到覆盖层。"""
    cv2, _ = vision_modules()
    rng = np.random.default_rng(7)
    canvas = rng.integers(0, 60, (720, 1280, 3), dtype=np.uint8)
    template = cv2.imread(str(DEFAULT_TEMPLATE_DIR / DAILY_SIGNIN_TEMPLATE), cv2.IMREAD_COLOR)
    height, width = template.shape[:2]
    canvas[82 : 82 + height, 98 : 98 + width] = template
    ok, encoded = cv2.imencode(".png", canvas)
    assert ok, "合成覆盖层帧编码 PNG 失败"
    data = encoded.tobytes()
    return Frame(
        image=canvas,
        width=1280,
        height=720,
        captured_at=NOW,
        sha256=hashlib.sha256(data).hexdigest(),
        source=FrameOrigin.ADB,
        png=data,
    )


def _navigation_context(tmp_path: Path) -> RunContext:
    """登录流程关闭、导航开启的上下文：导航用例只驱动导航环节。"""
    return make_context({"navigation": True, "login_flow": False}, evidence_dir=tmp_path)


def test_navigation_disabled_keeps_prepare_unchanged(tmp_path: Path) -> None:
    """navigation 关闭（缺省与显式 false）时：不发返回键、不多采样，结果与现状一致。"""
    frame = _named_frame("主界面")
    for config in ({}, {"navigation": False}):
        frames = _RepeatFrames(frame)
        actions = _FakeActions()
        ctx = make_context(config, evidence_dir=tmp_path)
        result = make_provider(_FakeHost(), actions, ctx, frames).prepare(ENV, ctx)
        assert result.ok is True
        assert "back" not in actions.calls
        # 截帧数＝现状：登录流程采样 1 次＋门控连续确认 2 次；导航若被误开会多采样一次。
        assert frames.captures == 3


def test_navigation_enabled_reaches_home_then_gate_passes(tmp_path: Path) -> None:
    """navigation 开启：非主界面按一次返回，其后连续 3 帧达导航档到家，门控随后放行。"""
    frames = _ScriptedFrames(
        [
            _named_frame("MuMu-20261108-210433-099"),
            _named_frame("主界面"),
            _named_frame("主界面"),
            _named_frame("主界面"),
            _named_frame("主界面"),
            _named_frame("主界面"),
        ]
    )
    actions = _FakeActions()
    ctx = _navigation_context(tmp_path)
    result = make_provider(_FakeHost(), actions, ctx, frames).prepare(ENV, ctx)
    assert result.ok is True
    assert "门控放行" in result.detail
    assert actions.calls.count("back") == 1
    assert frames.calls == 6


def test_navigation_budget_exhausted_fails_before_gate(tmp_path: Path) -> None:
    """navigation 开启：连续非主界面帧用满 3 次返回后停手，门控未运行。"""
    frame = _named_frame("MuMu-20261108-210433-099")
    frames = _ScriptedFrames([frame] * 7)
    actions = _FakeActions()
    ctx = _navigation_context(tmp_path)
    result = make_provider(_FakeHost(), actions, ctx, frames).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    assert "budget_exhausted" in result.detail
    assert "导航步数 7" in result.detail
    assert actions.calls.count("back") == 3
    assert frames.calls == 7


def test_navigation_overlay_stops_without_pressing_key(tmp_path: Path) -> None:
    """navigation 开启：判到覆盖层立即停手失败，一次返回键都不发。"""
    frames = _ScriptedFrames([_overlay_frame()])
    actions = _FakeActions()
    ctx = _navigation_context(tmp_path)
    result = make_provider(_FakeHost(), actions, ctx, frames).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    assert "overlay" in result.detail
    assert "导航步数 1" in result.detail
    assert actions.calls.count("back") == 0
    assert frames.calls == 1


def test_navigation_no_frame_fails(tmp_path: Path) -> None:
    """navigation 开启：截帧返回 None 立即停手，不按键、不重试。"""
    frames = _ScriptedFrames([None])
    actions = _FakeActions()
    ctx = _navigation_context(tmp_path)
    result = make_provider(_FakeHost(), actions, ctx, frames).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    assert "no_frame" in result.detail
    assert "导航步数 0" in result.detail
    assert actions.calls.count("back") == 0
    assert frames.calls == 1


def test_navigation_back_key_failure_fails_prepare(tmp_path: Path) -> None:
    """navigation 开启：返回键命令失败（OSError）按 E_ENV_NOT_READY 收口，循环不继续。"""
    frame = _named_frame("MuMu-20261108-210433-099")
    frames = _ScriptedFrames([frame, frame])
    actions = _FakeActions()
    actions.back_failure = OSError("adb 返回键失败：退出码 1")
    ctx = _navigation_context(tmp_path)
    result = make_provider(_FakeHost(), actions, ctx, frames).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_NOT_READY"
    assert "返回键" in result.detail
    assert actions.calls.count("back") == 1
    assert frames.calls == 1
