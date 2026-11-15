"""原生帧端口：RGBA 自下而上 → BGR 自上而下；通道失败一律 None，不抛异常。

DLL 卡死路径用可控放行事件验证：取帧按时限收口、断开恰一次、只有超时写事件。
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pytest

import plugins.maa.vision.mumu_frames as mumu_frames
from plugins.maa.capabilities.environment import _build_frames
from plugins.maa.vision.frames import AdbFrameSource, FrameOrigin
from plugins.maa.vision.mumu_frames import MumuFrameSource, NemuIpc, NemuIpcOptions, RgbaFrame

_RED = bytes((255, 0, 0, 255))
_GREEN = bytes((0, 255, 0, 255))
_BLUE = bytes((0, 0, 255, 255))
_WHITE = bytes((255, 255, 255, 255))
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_TIMEOUT_MS = 400
_TIMEOUT_BOUND_SECONDS = 0.6
_BLOCK_LIMIT_SECONDS = 5.0
_FRAME_TIMEOUT_EVENT = "maa.frame.timeout"


class _FakeRenderer:
    """固定返回一帧的渲染通道假件；记录 close 是否被调用。"""

    def __init__(self, frame: RgbaFrame | None) -> None:
        self._frame = frame
        self.closed = 0

    def capture(self) -> RgbaFrame | None:
        return self._frame

    def close(self) -> None:
        self.closed += 1


class _FakeLib:
    """按 MAA 约定返回尺寸与像素的假 DLL；display 查询只认 default。"""

    def __init__(self, width: int = 4, height: int = 2) -> None:
        self._width = width
        self._height = height
        self.calls: list[str] = []
        self.disconnected = 0

    def nemu_connect(self, *args: Any) -> int:
        self.calls.append("connect")
        return 7

    def nemu_disconnect(self, *args: Any) -> None:
        self.disconnected += 1

    def nemu_get_display_id(self, *args: Any) -> int:
        package = args[1]
        self.calls.append(f"display:{package.decode()}")
        return -1 if package != b"default" else 3

    def nemu_capture_display(self, *args: Any) -> int:
        # args: handle, display, size, byref(width), byref(height), buffer
        _handle, _display, size, width_out, height_out, buffer = args
        width_out._obj.value = self._width
        height_out._obj.value = self._height
        if buffer is not None:
            buffer.raw = bytes(size)
        return 0


class _BlockingLib(_FakeLib):
    """带缓冲的取帧调用阻塞到放行的假 DLL：模拟 DLL 卡死，放行或到时上限后返回。"""

    def __init__(self) -> None:
        super().__init__(width=2, height=1)
        self.release = threading.Event()

    def nemu_capture_display(self, *args: Any) -> int:
        _handle, _display, size, width_out, height_out, buffer = args
        width_out._obj.value = self._width
        height_out._obj.value = self._height
        if buffer is not None:
            self.release.wait(_BLOCK_LIMIT_SECONDS)
            buffer.raw = bytes(size)
        return 0


class _RefusingLib:
    """连接必失败的假 DLL：nemu_connect 返回 0 即失败。"""

    def __init__(self) -> None:
        self.disconnected = 0

    def nemu_connect(self, *args: Any) -> int:
        return 0

    def nemu_disconnect(self, *args: Any) -> None:
        self.disconnected += 1


class _EventSink:
    """记录写入口收到的事件：条数、事件名与载荷。"""

    def __init__(self) -> None:
        self.received: list[tuple[str, dict[str, object]]] = []

    def __call__(self, event: str, payload: Mapping[str, object]) -> None:
        self.received.append((event, dict(payload)))


def test_capture_flips_rows_and_converts_to_bgr() -> None:
    """自下而上的两行翻转后第一行是屏幕顶行，RGBA 转 BGR 通道序正确。"""
    bottom = _RED + _GREEN
    top = _BLUE + _WHITE
    frame = MumuFrameSource(_FakeRenderer(RgbaFrame(2, 2, bottom + top))).capture()
    assert frame is not None
    assert (frame.width, frame.height) == (2, 2)
    assert frame.source is FrameOrigin.NATIVE
    assert frame.image[0, 0].tolist() == [255, 0, 0]
    assert frame.image[0, 1].tolist() == [255, 255, 255]
    assert frame.image[1, 0].tolist() == [0, 0, 255]
    assert frame.png.startswith(_PNG_SIGNATURE)
    assert frame.sha256 == hashlib.sha256(frame.png).hexdigest()


def test_capture_returns_none_when_renderer_fails() -> None:
    """通道取不到帧时返回 None，不抛异常。"""
    assert MumuFrameSource(_FakeRenderer(None)).capture() is None


def test_capture_returns_none_on_byte_count_mismatch() -> None:
    """宽高与字节数不符（驱动返回半帧）按失败处理，不猜尺寸。"""
    assert MumuFrameSource(_FakeRenderer(RgbaFrame(2, 2, bytes(15)))).capture() is None


def test_nemu_ipc_returns_none_without_library(tmp_path: Path) -> None:
    """目录里没有渲染通道 DLL 时返回 None。"""
    assert NemuIpc(tmp_path, 0, "com.hypergryph.arknights").capture() is None


def test_nemu_ipc_falls_back_to_default_display_and_reads_size(tmp_path: Path) -> None:
    """包名取不到 display 时退回 default，并按查到的尺寸取整帧字节。"""
    sdk = tmp_path / "nx_device" / "15.0" / "shell" / "sdk"
    sdk.mkdir(parents=True)
    (sdk / "external_renderer_ipc.dll").write_bytes(b"")
    library = _FakeLib(width=4, height=2)
    ipc = NemuIpc(
        tmp_path, 1, "com.hypergryph.arknights", options=NemuIpcOptions(bind=lambda path: library)
    )
    try:
        frame = ipc.capture()
    finally:
        ipc.close()
    assert frame == RgbaFrame(width=4, height=2, rgba=bytes(32))
    assert "display:com.hypergryph.arknights" in library.calls
    assert "display:default" in library.calls
    assert library.disconnected == 1


def test_each_capture_connects_and_disconnects(tmp_path: Path) -> None:
    """每次取帧都成对连接与断开：门控一个运行两帧，句柄不复用也不泄漏。"""
    sdk = tmp_path / "nx_device" / "15.0" / "shell" / "sdk"
    sdk.mkdir(parents=True)
    (sdk / "external_renderer_ipc.dll").write_bytes(b"")
    library = _FakeLib(width=2, height=1)
    ipc = NemuIpc(
        tmp_path, 0, "com.hypergryph.arknights", options=NemuIpcOptions(bind=lambda path: library)
    )
    assert ipc.capture() is not None
    assert ipc.capture() is not None
    assert library.calls.count("connect") == 2
    assert library.disconnected == 2


def test_build_frames_picks_channel_by_config() -> None:
    """缺省与非法写法都走原生通道；显式 `adb` 才走截帧命令。"""
    sink = _EventSink()
    assert isinstance(_build_frames({}, sink), MumuFrameSource)
    assert isinstance(_build_frames({"frame_source": "mumu"}, sink), MumuFrameSource)
    assert isinstance(_build_frames({"frame_source": "坏值"}, sink), MumuFrameSource)
    assert isinstance(_build_frames({"frame_source": "adb"}, sink), AdbFrameSource)


def test_capture_keeps_image_shape_consistent_with_size() -> None:
    """数组形状与上报宽高一致，且是 uint8 三通道 BGR。"""
    frame = MumuFrameSource(_FakeRenderer(RgbaFrame(3, 2, bytes(3 * 2 * 4)))).capture()
    assert frame is not None
    assert frame.image.shape == (2, 3, 3)
    assert frame.image.dtype == np.uint8


def _sdk_with_dll(tmp_path: Path) -> Path:
    """造出带渲染通道 DLL 的目录，让绑定走注入的假件。"""
    sdk = tmp_path / "nx_device" / "15.0" / "shell" / "sdk"
    sdk.mkdir(parents=True, exist_ok=True)
    (sdk / "external_renderer_ipc.dll").write_bytes(b"")
    return sdk


def _ipc_with(tmp_path: Path, library: Any, sink: _EventSink | None = None) -> NemuIpc:
    """按给定假绑定与写入口拼一个带超时配置的原生通道。"""
    options = NemuIpcOptions(bind=lambda path: library, timeout_ms=_TIMEOUT_MS, logger=sink)
    _sdk_with_dll(tmp_path)
    return NemuIpc(tmp_path, 0, "com.hypergryph.arknights", options=options)


def test_capture_returns_none_within_timeout_when_dll_blocks(tmp_path: Path) -> None:
    """DLL 卡死时取帧按时限收口：上限加 200 毫秒内返回 None，不返回帧也不抛异常。"""
    library = _BlockingLib()
    ipc = _ipc_with(tmp_path, library)
    try:
        started = time.monotonic()
        frame = ipc.capture()
        elapsed = time.monotonic() - started
    finally:
        library.release.set()
    assert frame is None
    assert elapsed < _TIMEOUT_BOUND_SECONDS


def test_capture_recovers_after_timeout(tmp_path: Path) -> None:
    """一次卡死不毁掉通道：超时断开恰一次，假绑定恢复后下一次照常取到帧。"""
    library = _BlockingLib()
    ipc = _ipc_with(tmp_path, library)
    try:
        assert ipc.capture() is None
        assert library.disconnected == 1
        library.release.set()
        frame = ipc.capture()
    finally:
        library.release.set()
    assert frame is not None
    assert library.disconnected == 2


def test_timeout_writes_single_event_with_elapsed(tmp_path: Path) -> None:
    """超时收口经写入口记恰好一条 maa.frame.timeout：载荷带通道与整数耗时。"""
    library = _BlockingLib()
    sink = _EventSink()
    ipc = _ipc_with(tmp_path, library, sink)
    try:
        assert ipc.capture() is None
    finally:
        library.release.set()
    assert len(sink.received) == 1
    event, payload = sink.received[0]
    assert event == _FRAME_TIMEOUT_EVENT
    assert payload["channel"] == "native"
    assert isinstance(payload["elapsed_ms"], int)
    refusing = _ipc_with(tmp_path, _RefusingLib(), sink)
    assert refusing.capture() is None
    assert len(sink.received) == 1


def test_plain_failures_stay_silent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """连接失败、尺寸非正、字节数不符、编码失败都返回 None：不写事件、不抛异常。"""
    sink = _EventSink()
    assert _ipc_with(tmp_path, _RefusingLib(), sink).capture() is None
    assert _ipc_with(tmp_path, _FakeLib(width=0, height=0), sink).capture() is None
    assert MumuFrameSource(_FakeRenderer(RgbaFrame(2, 2, bytes(15)))).capture() is None
    monkeypatch.setattr(mumu_frames, "_encode_png", lambda image: None)
    assert MumuFrameSource(_FakeRenderer(RgbaFrame(2, 2, bytes(16)))).capture() is None
    assert sink.received == []


def test_timeout_does_not_block_process_exit(tmp_path: Path) -> None:
    """超时后放弃卡死的调用线程：线程是 daemon，调用方不被占住，放行后自行结束。"""
    library = _BlockingLib()
    before = set(threading.enumerate())
    ipc = _ipc_with(tmp_path, library)
    abandoned: list[threading.Thread] = []
    try:
        assert ipc.capture() is None
        assert not library.release.is_set()
        abandoned = [t for t in threading.enumerate() if t not in before]
        assert abandoned
        assert all(t.daemon for t in abandoned)
    finally:
        library.release.set()
    for thread in abandoned:
        thread.join(_BLOCK_LIMIT_SECONDS)
        assert not thread.is_alive()
