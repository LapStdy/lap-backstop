"""主界面门控的单元测试（docs/arknights/03 交接档、docs/arknights/04 证据与错误映射）。

帧源、上下文与宿主全部用假件：不连 adb、不启停模拟器与游戏；判定输入按
冻结样本 manifest 把裁片重建为 1280×720 满帧。
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import cv2
import numpy as np

from contracts import EnvironmentRef, ErrorCode, EventType, RunContext
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.capabilities.page_gate import GATE_SIGNAL_KIND, PageGate
from plugins.maa.vision.frames import Frame, FrameOrigin
from plugins.maa.windows_host import ScreenSize
from tests.maa_vision_support import fixture_frame, frame_entry, load_manifest

NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
ENV = EnvironmentRef(id="env-1", provider_id="maa")
GAME = "com.hypergryph.arknights"


class _Recorder:
    """收集门控副作用的假件：run.signal 载荷与证据登记逐条记录。"""

    def __init__(self) -> None:
        self.signals: list[tuple[str, Mapping[str, Any]]] = []
        self.evidence: list[tuple[str, Path, str]] = []

    def emit(self, event: str, payload: Mapping[str, Any]) -> None:
        self.signals.append((event, payload))

    def record(self, kind: str, path: Path, summary: str) -> str:
        self.evidence.append((kind, path, summary))
        return "evidence-1"


class _Clock:
    """UTC 时刻恒定；monotonic 用真实时钟测判定耗时。"""

    def monotonic(self) -> float:
        return time.monotonic()

    def utc_now(self) -> datetime:
        return NOW


class _NeverCancelled:
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


class _FakeFrames:
    """按序列返回预置帧的假帧源；耗尽后返回 None 并记录截帧次数。"""

    def __init__(self, frames: list[Frame | None]) -> None:
        self._frames = list(frames)
        self.captures = 0

    def capture(self) -> Frame | None:
        self.captures += 1
        return self._frames.pop(0) if self._frames else None


class _StubHost:
    """就绪宿主读数：窗口在、adb 在线、可截图、游戏在前台。"""

    def window_titles(self) -> tuple[str, ...]:
        return ("MuMuNxMain",)

    def adb_device_state(self) -> str:
        return "device"

    def screencap_size(self) -> ScreenSize | None:
        return ScreenSize(1280, 720)

    def foreground_package(self) -> str | None:
        return GAME


class _StubActions:
    """记录动作调用的假模拟器端口。"""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def start_instance(self, instance_index: int) -> None:
        self.calls.append(f"start:{instance_index}")

    def connect_adb(self) -> None:
        self.calls.append("connect")

    def is_package_running(self, package: str) -> bool:
        return True

    def launch_package(self, package: str) -> None:
        self.calls.append(f"launch:{package}")

    def stop_package(self, package: str) -> None:
        self.calls.append(f"stop:{package}")

    def disconnect_adb(self) -> None:
        self.calls.append("disconnect")

    def shutdown_instance(self, instance_index: int) -> None:
        self.calls.append(f"shutdown:{instance_index}")


def _no_wait(seconds: float) -> None:
    """零等待：两帧之间不睡，性能与放行判定都不受时钟影响。"""
    return None


def make_context(
    tmp_path: Path, recorder: _Recorder, config: Mapping[str, Any] | None = None
) -> RunContext:
    """门控测试上下文：证据目录指向临时目录，副作用进 recorder。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={} if config is None else dict(config),
        work_dir=tmp_path,
        evidence_dir=tmp_path,
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_NeverCancelled(),
        clock=_Clock(),
        secrets={},
        log=_SilentLogger(),
        emit=recorder.emit,
        record_evidence=recorder.record,
    )


def _home_entries() -> list[dict]:
    """冻结样本里的 21 帧主界面。"""
    homes = [entry for entry in load_manifest()["frames"] if entry["expected"] == "home"]
    assert len(homes) == 21
    return homes


def _sized_frame(width: int, height: int) -> Frame:
    """纯黑合成帧：分辨率核对用，内容不参与判定。"""
    image = np.zeros((height, width, 3), np.uint8)
    ok, encoded = cv2.imencode(".png", image)
    assert ok, "合成画布编码 PNG 失败"
    data = encoded.tobytes()
    return Frame(
        image=image,
        width=width,
        height=height,
        captured_at=NOW,
        sha256=hashlib.sha256(data).hexdigest(),
        source=FrameOrigin.ADB,
        png=data,
    )


def test_home_frames_pass_gate(tmp_path: Path) -> None:
    """21 帧主界面样本逐帧喂入：连续两帧达交接档，全部放行。"""
    for entry in _home_entries():
        frame = fixture_frame(entry)
        recorder = _Recorder()
        outcome = PageGate(frames=_FakeFrames([frame, frame]), wait=_no_wait).run(
            make_context(tmp_path, recorder)
        )
        assert outcome.passed is True, entry["name"]
        assert outcome.error_code is None
        assert "放行" in outcome.detail


def test_non_home_frames_block_gate(tmp_path: Path) -> None:
    """17 帧非主界面样本逐帧喂入：全部拦下，错误码与 run.signal 载荷齐全。"""
    others = [entry for entry in load_manifest()["frames"] if entry["expected"] == "other"]
    assert len(others) == 17
    for entry in others:
        frame = fixture_frame(entry)
        recorder = _Recorder()
        outcome = PageGate(frames=_FakeFrames([frame, frame]), wait=_no_wait).run(
            make_context(tmp_path, recorder)
        )
        assert outcome.passed is False, entry["name"]
        assert outcome.error_code is ErrorCode.ENV_NOT_READY
        signals = [
            payload for event, payload in recorder.signals if event == EventType.RUN_SIGNAL.value
        ]
        assert len(signals) == 1, entry["name"]
        assert signals[0]["kind"] == GATE_SIGNAL_KIND
        body = signals[0]["payload"]
        assert body["frame_sha256"] == frame.sha256
        assert isinstance(body["total_score"], int)
        assert isinstance(body["appearance_hits"], int)


def test_capture_failure_maps_unreachable(tmp_path: Path) -> None:
    """帧源返回 None 按环境不可达；1920×1080 按分辨率不符且不缩放。"""
    recorder = _Recorder()
    none_frames = _FakeFrames([None])
    outcome = PageGate(frames=none_frames, wait=_no_wait).run(make_context(tmp_path, recorder))
    assert outcome.passed is False
    assert outcome.error_code is ErrorCode.ENV_UNREACHABLE
    assert none_frames.captures == 1
    assert recorder.evidence == []

    large = _sized_frame(1920, 1080)
    recorder = _Recorder()
    large_frames = _FakeFrames([large])
    outcome = PageGate(frames=large_frames, wait=_no_wait).run(make_context(tmp_path, recorder))
    assert outcome.passed is False
    assert outcome.error_code is ErrorCode.ENV_RESOLUTION
    assert "1920x1080" in outcome.detail
    assert large_frames.captures == 1
    screenshots = [path for kind, path, _ in recorder.evidence if kind == "screenshot"]
    assert len(screenshots) == 1
    assert (tmp_path / screenshots[0]).read_bytes() == large.png


def test_gate_writes_two_evidence_rows(tmp_path: Path) -> None:
    """每次判定写两条证据：路径相对证据根目录，截图字节与登记哈希同帧一致。"""
    frame = fixture_frame(frame_entry(load_manifest(), _home_entries()[0]["name"]))
    recorder = _Recorder()
    outcome = PageGate(frames=_FakeFrames([frame, frame]), wait=_no_wait).run(
        make_context(tmp_path, recorder)
    )
    assert outcome.passed is True
    assert [kind for kind, _path, _summary in recorder.evidence] == [
        "screenshot",
        "probe",
        "screenshot",
        "probe",
    ]
    stem = f"page-gate-{frame.sha256[:12]}"
    for kind, path, _summary in recorder.evidence:
        assert not path.is_absolute()
        assert path == Path(f"{stem}.{'png' if kind == 'screenshot' else 'json'}")
    assert (tmp_path / f"{stem}.png").read_bytes() == frame.png
    probe = json.loads((tmp_path / f"{stem}.json").read_text(encoding="utf-8"))
    assert probe["frame_sha256"] == frame.sha256
    assert probe["total_score"] >= 8
    assert probe["handover_hit"] is True


def test_gate_within_budget(tmp_path: Path) -> None:
    """注入零等待：门控内部判定耗时（两帧合计）不超过 20 毫秒。

    两帧用不同主界面样本，判定明细各占一份文件，合计才是两次判定的真实耗时。
    """
    frames = [fixture_frame(entry) for entry in _home_entries()[:2]]
    recorder = _Recorder()
    outcome = PageGate(frames=_FakeFrames(frames), wait=_no_wait).run(
        make_context(tmp_path, recorder)
    )
    assert outcome.passed is True
    judge_ms = 0.0
    for kind, path, _summary in recorder.evidence:
        if kind == "probe":
            probe = json.loads((tmp_path / path).read_text(encoding="utf-8"))
            judge_ms += float(probe["judge_ms"])
    assert judge_ms <= 20.0, f"两帧判定合计 {judge_ms} 毫秒"


def test_page_gate_disabled_keeps_old_behaviour(tmp_path: Path) -> None:
    """page_gate=false：不截帧、不写证据、不写信号，prepare 结果与改造前一致。"""
    frames = _FakeFrames([None])
    recorder = _Recorder()
    provider = MaaEnvironment(host=_StubHost(), actions=_StubActions(), frames=frames)
    result = provider.prepare(ENV, make_context(tmp_path, recorder, config={"page_gate": False}))
    assert result.ok is True
    assert result.error_code is None
    assert result.detail == "环境就绪：127.0.0.1:16384 在线，com.hypergryph.arknights 在前台"
    assert frames.captures == 0
    assert recorder.evidence == []
    assert recorder.signals == []
