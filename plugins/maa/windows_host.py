"""真机宿主的只读端口：窗口枚举、adb 设备状态、截图分辨率、前台包名。

environment 能力只经这个端口读真机，单元测试注入假件；模块本身不启动程序、
不点击、不写文件。窗口标题按子串匹配：MuMu 的进程与窗口名随版本改（MuMuPlayer-12.0
变成 MuMuNx 家族），标题前缀由插件配置给出。截图分辨率取自 PNG 的 IHDR，只用标准库。
"""

from __future__ import annotations

import ctypes
import re
import subprocess
from collections.abc import Sequence
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

_ADB_TIMEOUT_SECONDS = 20.0
_SCREENCAP_TIMEOUT_SECONDS = 30.0
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_PNG_HEADER_BYTES = 24
_CREATE_NO_WINDOW = 0x08000000
_IHDR_OFFSET = 12
_IHDR_SIZE_OFFSET = 16

_DEVICE_LINE = re.compile(r"^(\S+)\s+(\S+)$")
_FOCUS_LINE = re.compile(r"mCurrentFocus=.*?\s([A-Za-z][\w.]*)/[\w.$]+")

_user32 = ctypes.WinDLL("user32", use_last_error=True)
_enum_windows_proc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
_user32.GetWindowTextLengthW.restype = ctypes.c_int
_user32.GetWindowTextLengthW.argtypes = (wintypes.HWND,)
_user32.GetWindowTextW.restype = ctypes.c_int
_user32.GetWindowTextW.argtypes = (wintypes.HWND, wintypes.LPWSTR, ctypes.c_int)
_user32.EnumWindows.restype = wintypes.BOOL
_user32.EnumWindows.argtypes = (_enum_windows_proc, wintypes.LPARAM)


@dataclass(frozen=True, slots=True)
class ScreenSize:
    """截图分辨率，单位像素。"""

    width: int
    height: int


class MachineHost(Protocol):
    """读真机的最小端口；四个方法都只读，不改变任何外部状态。"""

    def window_titles(self) -> Sequence[str]:
        """返回当前所有带标题的顶层窗口标题。"""
        ...

    def adb_device_state(self) -> str:
        """返回目标地址在 adb devices 里的状态：device / offline / absent / unavailable。"""
        ...

    def screencap_size(self) -> ScreenSize | None:
        """截一帧并返回分辨率；截不到或不是 PNG 时返回 None。"""
        ...

    def foreground_package(self) -> str | None:
        """返回当前前台窗口所属包名；读不到时返回 None。"""
        ...


class WindowsMachineHost:
    """Windows 实现：ctypes 枚举窗口，adb 子进程查询设备与截图。"""

    def __init__(self, adb_path: Path, address: str) -> None:
        self._adb_path = adb_path
        self._address = address

    def window_titles(self) -> Sequence[str]:
        """枚举带标题的顶层窗口；标题按窗口文本原样返回。"""
        titles: list[str] = []

        def collect(hwnd: int, _param: int) -> bool:
            length = _user32.GetWindowTextLengthW(hwnd)
            if length <= 0:
                return True
            buffer = ctypes.create_unicode_buffer(length + 1)
            _user32.GetWindowTextW(hwnd, buffer, length + 1)
            titles.append(buffer.value)
            return True

        _user32.EnumWindows(_enum_windows_proc(collect), 0)
        return tuple(titles)

    def adb_device_state(self) -> str:
        """不发起 connect，只读 adb devices 的现有状态。"""
        completed = self._run(("devices",), _ADB_TIMEOUT_SECONDS)
        if completed is None:
            return "unavailable"
        text = completed.stdout.decode("utf-8", errors="replace")
        for line in text.splitlines():
            match = _DEVICE_LINE.match(line.strip())
            if match is not None and match.group(1) == self._address:
                return match.group(2)
        return "absent"

    def screencap_size(self) -> ScreenSize | None:
        """整帧截图只用来读分辨率；解码失败按证据缺失处理。"""
        completed = self._run(
            ("-s", self._address, "exec-out", "screencap", "-p"),
            _SCREENCAP_TIMEOUT_SECONDS,
        )
        if completed is None or completed.returncode != 0:
            return None
        return _png_size(completed.stdout)

    def capture(self) -> bytes | None:
        """整帧 PNG 字节给视觉帧端口；只做 screencap 与进程调用，失败返回 None 不抛。"""
        completed = self._run(
            ("-s", self._address, "exec-out", "screencap", "-p"),
            _SCREENCAP_TIMEOUT_SECONDS,
        )
        if completed is None or completed.returncode != 0:
            return None
        if _png_size(completed.stdout) is None:
            return None
        return completed.stdout

    def foreground_package(self) -> str | None:
        """前台包名从 dumpsys window 的 mCurrentFocus 取；桌面也是合法结果。"""
        completed = self._run(
            ("-s", self._address, "shell", "dumpsys", "window"),
            _ADB_TIMEOUT_SECONDS,
        )
        if completed is None or completed.returncode != 0:
            return None
        return _foreground_package(completed.stdout.decode("utf-8", errors="replace"))

    def _run(
        self,
        args: tuple[str, ...],
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes] | None:
        """执行一次 adb 调用；超时与非零退出交给调用方按缺失证据处理。"""
        try:
            return subprocess.run(
                [str(self._adb_path), *args],
                capture_output=True,
                timeout=timeout,
                check=False,
                creationflags=_CREATE_NO_WINDOW,
            )
        except (OSError, subprocess.SubprocessError):
            return None


def _png_size(data: bytes) -> ScreenSize | None:
    """从 PNG 头读宽高；只认 IHDR 首块，坏字节按截不到处理。"""
    if len(data) < _PNG_HEADER_BYTES or not data.startswith(_PNG_SIGNATURE):
        return None
    if data[_IHDR_OFFSET : _IHDR_OFFSET + 4] != b"IHDR":
        return None
    width = int.from_bytes(data[_IHDR_SIZE_OFFSET : _IHDR_SIZE_OFFSET + 4], "big")
    height = int.from_bytes(data[_IHDR_SIZE_OFFSET + 4 : _IHDR_SIZE_OFFSET + 8], "big")
    if width <= 0 or height <= 0:
        return None
    return ScreenSize(width=width, height=height)


def _foreground_package(text: str) -> str | None:
    """提取 mCurrentFocus 的包名；null 或格式变化返回 None，不猜。"""
    match = _FOCUS_LINE.search(text)
    return match.group(1) if match is not None else None
