"""原生帧端口：RGBA 自下而上 → BGR 自上而下；通道失败一律 None，不抛异常。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import numpy as np

from plugins.maa.capabilities.environment import _build_frames
from plugins.maa.vision.frames import AdbFrameSource, FrameOrigin
from plugins.maa.vision.mumu_frames import MumuFrameSource, NemuIpc, RgbaFrame

_RED = bytes((255, 0, 0, 255))
_GREEN = bytes((0, 255, 0, 255))
_BLUE = bytes((0, 0, 255, 255))
_WHITE = bytes((255, 255, 255, 255))
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


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
    ipc = NemuIpc(tmp_path, 1, "com.hypergryph.arknights", bind=lambda path: library)
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
    ipc = NemuIpc(tmp_path, 0, "com.hypergryph.arknights", bind=lambda path: library)
    assert ipc.capture() is not None
    assert ipc.capture() is not None
    assert library.calls.count("connect") == 2
    assert library.disconnected == 2


def test_build_frames_picks_channel_by_config() -> None:
    """`frame_source=mumu` 选原生通道，其余（含缺省）走 adb。"""
    assert isinstance(_build_frames({"frame_source": "mumu"}), MumuFrameSource)
    assert isinstance(_build_frames({"frame_source": "adb"}), AdbFrameSource)
    assert isinstance(_build_frames({}), AdbFrameSource)


def test_capture_keeps_image_shape_consistent_with_size() -> None:
    """数组形状与上报宽高一致，且是 uint8 三通道 BGR。"""
    frame = MumuFrameSource(_FakeRenderer(RgbaFrame(3, 2, bytes(3 * 2 * 4)))).capture()
    assert frame is not None
    assert frame.image.shape == (2, 3, 3)
    assert frame.image.dtype == np.uint8
