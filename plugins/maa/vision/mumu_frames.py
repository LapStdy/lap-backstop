"""MuMu 原生通道的帧端口：调 external_renderer_ipc.dll 取 RGBA 帧再转 BGR。

调用约定取自 MAA dev-v2 的 MumuExtras：``nemu_connect(宽字符目录, 实例序号)`` 返回 0
为失败；``nemu_get_display_id(handle, 包名或 "default", 0)`` 负数为失败；
``nemu_capture_display(handle, display, buf_size, &width, &height, buf)`` 返回 0 才成功，
先用 buf_size=0 与空缓冲查尺寸。帧是 RGBA8888、行序自下而上，这里翻成自上而下并转
BGR。任何失败都返回 ``None``，不抛异常；原生通道没有原始 PNG，摘要取现编码的字节。
"""

from __future__ import annotations

import ctypes
import hashlib
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from plugins.maa.vision.frames import Frame, FrameOrigin
from plugins.maa.vision.judge import vision_modules

if TYPE_CHECKING:
    import numpy as np

LIBRARY_NAME = "external_renderer_ipc.dll"
CANDIDATE_SUBDIRS = ("nx_device/15.0/shell/sdk", "nx_device/12.0/shell/sdk", "shell/sdk")
DEFAULT_DISPLAY_PACKAGE = b"default"
_CHANNELS = 4


@dataclass(frozen=True, slots=True)
class RgbaFrame:
    """原生通道取到的一帧：宽高与自下而上的 RGBA 字节。"""

    width: int
    height: int
    rgba: bytes


class Renderer(Protocol):
    """渲染通道端口：取一帧 RGBA，失败返回 None；``close`` 可重复调用。"""

    def capture(self) -> RgbaFrame | None:
        """取当前一帧；失败返回 None，不抛异常。"""
        ...

    def close(self) -> None:
        """断开连接并释放句柄。"""
        ...


class NemuIpc:
    """外部渲染通道的 ctypes 绑定：连一次、取 display、查尺寸、取帧。"""

    def __init__(
        self,
        mumu_dir: Path,
        index: int,
        package: str,
        bind: Any = None,
    ) -> None:
        self._dir = mumu_dir
        self._index = index
        self._package = package.encode("utf-8")
        self._bind = bind if bind is not None else _bind
        self._lib: Any = None
        self._handle = 0

    def capture(self) -> RgbaFrame | None:
        """取一帧；没库、连不上、查不到尺寸或取帧失败都返回 None。"""
        handle = self._connect()
        if handle == 0:
            return None
        display = self._display_id(handle)
        size = self._frame_size(handle, display)
        if size is None:
            return None
        width, height = size
        buffer = ctypes.create_string_buffer(width * height * _CHANNELS)
        if self._call(handle, display, len(buffer), buffer)[0] != 0:
            return None
        return RgbaFrame(width=width, height=height, rgba=buffer.raw)

    def close(self) -> None:
        """断开连接；重复调用安全。"""
        if self._handle != 0 and self._lib is not None:
            self._lib.nemu_disconnect(self._handle)
        self._handle = 0

    def _connect(self) -> int:
        if self._handle != 0:
            return self._handle
        lib = self._lib if self._lib is not None else self._load()
        if lib is None:
            return 0
        self._handle = int(lib.nemu_connect(str(self._dir), self._index))
        return self._handle

    def _load(self) -> Any:
        if sys.platform != "win32":
            return None
        for sub in CANDIDATE_SUBDIRS:
            path = self._dir / sub / LIBRARY_NAME
            if not path.is_file():
                continue
            try:
                self._lib = self._bind(path)
            except OSError:
                continue
            return self._lib
        return None

    def _display_id(self, handle: int) -> int:
        """优先按游戏包名取 display，失败退回 default，再失败退回 0（主 display）。"""
        found = int(self._lib.nemu_get_display_id(handle, self._package, 0))
        if found >= 0:
            return found
        fallback = int(self._lib.nemu_get_display_id(handle, DEFAULT_DISPLAY_PACKAGE, 0))
        return fallback if fallback >= 0 else 0

    def _frame_size(self, handle: int, display: int) -> tuple[int, int] | None:
        """空缓冲查尺寸：0 为成功，尺寸非正按失败处理。"""
        ret, width, height = self._call(handle, display, 0, None)
        if ret != 0 or width <= 0 or height <= 0:
            return None
        return width, height

    def _call(
        self,
        handle: int,
        display: int,
        size: int,
        buffer: Any,
    ) -> tuple[int, int, int]:
        """一次 capture_display 调用，返回（返回值、宽、高）；buffer 为 None 即查尺寸。"""
        width = ctypes.c_int(0)
        height = ctypes.c_int(0)
        ret = int(
            self._lib.nemu_capture_display(
                handle, display, size, ctypes.byref(width), ctypes.byref(height), buffer
            )
        )
        return ret, width.value, height.value


class MumuFrameSource:
    """``FrameSource`` 的原生实现：RGBA 自下而上 → BGR 自上而下，PNG 现编码。"""

    def __init__(self, renderer: Renderer) -> None:
        self._renderer = renderer

    def capture(self) -> Frame | None:
        """取一帧；通道失败、字节数不符或编码失败都返回 None，不抛异常。"""
        raw = self._renderer.capture()
        if raw is None:
            return None
        image = _to_bgr(raw)
        if image is None:
            return None
        png = _encode_png(image)
        if png is None:
            return None
        height, width = image.shape[:2]
        return Frame(
            image=image,
            width=width,
            height=height,
            captured_at=datetime.now(UTC),
            sha256=hashlib.sha256(png).hexdigest(),
            source=FrameOrigin.NATIVE,
            png=png,
        )


def _bind(path: Path) -> Any:
    """按 MAA 的调用约定绑定四个符号；缺符号时属性访问会抛 AttributeError。"""
    lib = ctypes.WinDLL(str(path))
    lib.nemu_connect.restype = ctypes.c_int
    lib.nemu_connect.argtypes = (ctypes.c_wchar_p, ctypes.c_int)
    lib.nemu_disconnect.restype = None
    lib.nemu_disconnect.argtypes = (ctypes.c_int,)
    lib.nemu_get_display_id.restype = ctypes.c_int
    lib.nemu_get_display_id.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int)
    lib.nemu_capture_display.restype = ctypes.c_int
    lib.nemu_capture_display.argtypes = (
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int),
        ctypes.c_void_p,
    )
    return lib


def _to_bgr(raw: RgbaFrame) -> np.ndarray | None:
    """RGBA（行序自下而上）→ BGR（自上而下）；字节数与尺寸不符返回 None。"""
    cv2, numpy = vision_modules()
    if raw.width <= 0 or raw.height <= 0:
        return None
    if len(raw.rgba) != raw.width * raw.height * _CHANNELS:
        return None
    pixels = numpy.frombuffer(raw.rgba, dtype=numpy.uint8).reshape(
        (raw.height, raw.width, _CHANNELS)
    )
    return cv2.flip(cv2.cvtColor(pixels, cv2.COLOR_RGBA2BGR), 0)


def _encode_png(image: np.ndarray) -> bytes | None:
    """BGR 数组编码成 PNG；编码失败返回 None。"""
    cv2, _ = vision_modules()
    ok, buffer = cv2.imencode(".png", image)
    return buffer.tobytes() if ok else None
