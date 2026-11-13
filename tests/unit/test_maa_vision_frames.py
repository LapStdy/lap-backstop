"""帧端口的行为：合法 PNG 成帧、失败一律 None、命令与超时参数、缺依赖提示。"""

from __future__ import annotations

import hashlib
import subprocess
import sys
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

import cv2
import numpy as np
import pytest

from plugins.maa.vision.frames import AdbFrameSource, FrameOrigin
from plugins.maa.vision.judge import INSTALL_HINT
from plugins.maa.windows_host import MachineHost, WindowsMachineHost


def _ok(data: bytes) -> subprocess.CompletedProcess[bytes]:
    """退出码 0、携带给定输出的假进程结果。"""
    return subprocess.CompletedProcess([], 0, stdout=data)


class _ScriptedRunner:
    """注入的假执行器：按预设返回结果或抛错，记录命令与超时，不跑真 adb。"""

    def __init__(
        self,
        completed: subprocess.CompletedProcess[bytes] | None = None,
        raises: Exception | None = None,
    ) -> None:
        self.command: list[str] | None = None
        self.timeout: float | None = None
        self._completed = completed
        self._raises = raises

    def __call__(
        self, args: Sequence[str], timeout: float
    ) -> subprocess.CompletedProcess[bytes] | None:
        self.command = list(args)
        self.timeout = timeout
        if self._raises is not None:
            raise self._raises
        return self._completed


def _png_bytes(width: int = 160, height: int = 90) -> bytes:
    """合成一张指定宽高的真实 PNG，供解码正例与哈希断言使用。"""
    image = np.full((height, width, 3), (30, 60, 90), np.uint8)
    ok, encoded = cv2.imencode(".png", image)
    assert ok, "cv2.imencode 失败"
    return encoded.tobytes()


def _raising_run(error: Exception):
    """假 subprocess.run：一律抛给定异常。"""

    def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise error

    return run


def test_adb_source_returns_frame_on_valid_png() -> None:
    """正例：合法 PNG → Frame 宽高正确、来源 adb、摘要等于字节摘要、时刻带 UTC。"""
    data = _png_bytes()
    frame = AdbFrameSource(runner=_ScriptedRunner(completed=_ok(data))).capture()
    assert frame is not None
    assert (frame.width, frame.height) == (160, 90)
    assert frame.image.shape == (90, 160, 3)
    assert frame.source == FrameOrigin.ADB and frame.source.value == "adb"
    assert frame.sha256 == hashlib.sha256(data).hexdigest()
    assert frame.captured_at.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    "runner",
    [
        _ScriptedRunner(raises=subprocess.TimeoutExpired(cmd="adb", timeout=30.0)),
        _ScriptedRunner(completed=subprocess.CompletedProcess([], 1, stdout=b"error")),
        _ScriptedRunner(completed=_ok(b"plain text, not png")),
        _ScriptedRunner(completed=None),
    ],
    ids=["timeout", "nonzero-exit", "not-png", "no-output"],
)
def test_adb_source_returns_none_on_failure(runner: _ScriptedRunner) -> None:
    """反例：超时、非零退出、非 PNG、无输出都返回 None 且不抛。"""
    assert AdbFrameSource(runner=runner).capture() is None


def test_adb_source_passes_device_and_timeout() -> None:
    """命令与超时：-s 127.0.0.1:16384 exec-out screencap -p，单次超时 30 秒。"""
    runner = _ScriptedRunner(completed=_ok(_png_bytes()))
    AdbFrameSource(runner=runner).capture()
    assert runner.command == ["adb", "-s", "127.0.0.1:16384", "exec-out", "screencap", "-p"]
    assert runner.timeout == 30.0


def test_non_720p_frame_is_not_scaled() -> None:
    """帧端口只如实报宽高：1920×1080 原样成帧，不缩放不裁剪。"""
    data = _png_bytes(width=1920, height=1080)
    frame = AdbFrameSource(runner=_ScriptedRunner(completed=_ok(data))).capture()
    assert frame is not None
    assert (frame.width, frame.height) == (1920, 1080)
    assert frame.image.shape == (1080, 1920, 3)
    assert frame.sha256 == hashlib.sha256(data).hexdigest()


def test_missing_dependency_hint_for_frames(monkeypatch: pytest.MonkeyPatch) -> None:
    """缺 cv2/numpy 时解码抛 RuntimeError 并带安装命令，不静默降级。"""
    data = _png_bytes()
    monkeypatch.setitem(sys.modules, "cv2", None)
    monkeypatch.setitem(sys.modules, "numpy", None)
    source = AdbFrameSource(runner=_ScriptedRunner(completed=_ok(data)))
    with pytest.raises(RuntimeError) as excinfo:
        source.capture()
    assert INSTALL_HINT in str(excinfo.value)


def test_windows_host_capture_returns_bytes_or_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """宿主截帧：PNG → 整帧字节；超时、非零退出、非 PNG → None；协议成员集合不变。"""
    host = WindowsMachineHost(adb_path=Path("adb"), address="127.0.0.1:16384")
    data = _png_bytes(8, 8)
    monkeypatch.setattr(
        "plugins.maa.windows_host.subprocess.run", lambda *args, **kwargs: _ok(data)
    )
    assert host.capture() == data
    monkeypatch.setattr(
        "plugins.maa.windows_host.subprocess.run",
        _raising_run(subprocess.TimeoutExpired(cmd="adb", timeout=30.0)),
    )
    assert host.capture() is None
    monkeypatch.setattr(
        "plugins.maa.windows_host.subprocess.run",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 1, stdout=b""),
    )
    assert host.capture() is None
    monkeypatch.setattr(
        "plugins.maa.windows_host.subprocess.run", lambda *args, **kwargs: _ok(b"not png")
    )
    assert host.capture() is None
    # Protocol 的运行时成员内省属性实测存在（Python 3.14），typeshed 未收录，故定向 ignore。
    members = sorted(MachineHost.__protocol_attrs__)  # type: ignore[attr-defined]
    assert members == [
        "adb_device_state",
        "foreground_package",
        "screencap_size",
        "window_titles",
    ]
