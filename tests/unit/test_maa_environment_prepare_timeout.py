"""MaaEnvironment.prepare 生产构造帧端口的超时写入口绑定（docs/plugins/maa.md 环境节）。

原生通道构造时写入口必须是本次运行的 ``ctx.emit``：探针替换 ``NemuIpc`` 记录构造
参数，验证绑定身份、超时事件恰一条落当次运行的事件流、注入帧源时不偷建原生通道。
模拟器动作与真机读数用假件：不启动实例、不连 adb、不拉起游戏、不截真帧。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import plugins.maa.capabilities.environment as maa_environment
from contracts import EnvironmentRef, RunContext
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.mumu_instance import MumuInstance
from plugins.maa.vision.frames import Frame, FrameSource
from plugins.maa.vision.mumu_frames import NemuIpcOptions, RgbaFrame
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


class _RepeatFrames:
    """每次截帧返回同一满帧的假帧源：注入路径用例的门控输入。"""

    def __init__(self, frame: Frame) -> None:
        self._frame = frame
        self.captures = 0

    def capture(self) -> Frame | None:
        self.captures += 1
        return self._frame


def make_context(
    config: Mapping[str, Any] | None = None,
    evidence_dir: Path | None = None,
    emit: Callable[[str, Mapping[str, Any]], None] | None = None,
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
        emit=emit if emit is not None else (lambda kind, payload: None),
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


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


class _ProbeIpc:
    """NemuIpc 探针：记录构造参数；capture 按 ``emits_timeout`` 走超时写事件路径。"""

    def __init__(
        self,
        mumu_dir: Path,
        index: int,
        package: str,
        options: NemuIpcOptions | None = None,
    ) -> None:
        self.mumu_dir = mumu_dir
        self.index = index
        self.package = package
        self.options = options
        self.captures = 0
        self.emits_timeout = False

    def capture(self) -> RgbaFrame | None:
        self.captures += 1
        if self.emits_timeout and self.options is not None and self.options.logger is not None:
            self.options.logger("maa.frame.timeout", {"channel": "native", "elapsed_ms": 12})
        return None

    def close(self) -> None:
        return None


def _install_probe(monkeypatch: pytest.MonkeyPatch, emits_timeout: bool) -> list[_ProbeIpc]:
    """把 environment 的 NemuIpc 换成探针，返回按构造顺序收集的实例列表。"""
    constructed: list[_ProbeIpc] = []

    def factory(
        mumu_dir: Path, index: int, package: str, options: NemuIpcOptions | None = None
    ) -> _ProbeIpc:
        probe = _ProbeIpc(mumu_dir, index, package, options)
        probe.emits_timeout = emits_timeout
        constructed.append(probe)
        return probe

    monkeypatch.setattr(maa_environment, "NemuIpc", factory)
    return constructed


def _production_provider(ctx: RunContext) -> MaaEnvironment:
    """不注入帧源的环境能力：帧端口走生产构造路径，等待推进注入的假时钟。"""

    def wait(seconds: float) -> None:
        ctx.clock.monotonic_seconds += seconds  # type: ignore[attr-defined]

    return MaaEnvironment(host=_FakeHost(), actions=_FakeActions(), wait=wait, frames=None)


def test_prepare_wires_native_timeout_sink(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """生产构造原生帧端口时写入口绑本次运行：探针看到 ctx.emit，而不是 None。"""
    probes = _install_probe(monkeypatch, emits_timeout=False)
    ctx = make_context({"login_flow": False}, evidence_dir=tmp_path)
    result = _production_provider(ctx).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_UNREACHABLE"
    assert len(probes) == 1
    assert probes[0].options is not None
    assert probes[0].options.logger is ctx.emit
    assert probes[0].captures == 1


def test_timeout_event_reaches_run_context(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """探针按超时路径写事件：恰一条 maa.frame.timeout 落当次运行，通道 native、耗时正整数。"""
    probes = _install_probe(monkeypatch, emits_timeout=True)
    events: list[tuple[str, Mapping[str, Any]]] = []
    ctx = make_context(
        {"login_flow": False},
        evidence_dir=tmp_path,
        emit=lambda kind, payload: events.append((kind, dict(payload))),
    )
    result = _production_provider(ctx).prepare(ENV, ctx)
    assert result.ok is False
    assert result.error_code == "E_ENV_UNREACHABLE"
    assert len(events) == 1
    kind, payload = events[0]
    assert kind == "maa.frame.timeout"
    assert payload["channel"] == "native"
    elapsed = payload["elapsed_ms"]
    assert isinstance(elapsed, int)
    assert elapsed > 0
    assert probes[0].captures == 1


def test_injected_frames_keep_no_sink(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """注入帧源时探针不构造、事件不写：注入路径不偷建原生通道。"""
    probes = _install_probe(monkeypatch, emits_timeout=True)
    events: list[tuple[str, Mapping[str, Any]]] = []
    ctx = make_context(
        {},
        evidence_dir=tmp_path,
        emit=lambda kind, payload: events.append((kind, dict(payload))),
    )
    result = make_provider(_FakeHost(), _FakeActions(), ctx, _home_frames()).prepare(ENV, ctx)
    assert result.ok is True
    assert probes == []
    assert events == []
