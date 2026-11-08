"""MAA 进程端口的 Windows 实现：拉起、探活、请求停止与整树结束。

script 能力只经这个端口碰进程，单元测试注入假件。拉起时剔除三个代理环境变量：
MAA 的上报与更新请求在死代理上会卡住启动（docs/arknights/02 启动节）。停止先发
``WM_CLOSE`` 让 MAA 自己收尾，宽限期后才结束整棵进程树；没有窗口时直接结束进程树。
"""

from __future__ import annotations

import csv
import ctypes
import io
import os
import subprocess
from collections.abc import Mapping
from ctypes import wintypes
from pathlib import Path
from typing import Protocol

_CREATE_NO_WINDOW = 0x08000000
_WM_CLOSE = 0x0010
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_SYNCHRONIZE = 0x00100000
_STILL_ACTIVE = 259
_WAIT_TIMEOUT = 0x102
_TASKLIST_SECONDS = 10.0
_TASKKILL_SECONDS = 20.0
_PROXY_VARIABLES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)

_user32 = ctypes.WinDLL("user32", use_last_error=True)
_kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
_enum_windows_proc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
_user32.EnumWindows.restype = wintypes.BOOL
_user32.EnumWindows.argtypes = (_enum_windows_proc, wintypes.LPARAM)
_user32.GetWindowThreadProcessId.restype = wintypes.DWORD
_user32.GetWindowThreadProcessId.argtypes = (wintypes.HWND, ctypes.POINTER(wintypes.DWORD))
_user32.PostMessageW.restype = wintypes.BOOL
_user32.PostMessageW.argtypes = (wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)
_kernel32.OpenProcess.restype = wintypes.HANDLE
_kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
_kernel32.GetExitCodeProcess.restype = wintypes.BOOL
_kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
_kernel32.CloseHandle.restype = wintypes.BOOL
_kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)


class ProcessHost(Protocol):
    """脚本进程的最小端口；五个方法都只操作平台自身的进程表。"""

    def start(self, executable: Path, working_dir: Path) -> int:
        """启动程序并返回 pid；启动失败抛 OSError。"""
        ...

    def is_running(self, pid: int) -> bool:
        """pid 是否仍是一个运行中的进程。"""
        ...

    def running_pids(self, image_name: str) -> tuple[int, ...]:
        """按映像名列出运行中的 pid；用于发现已存在的同类进程。"""
        ...

    def request_stop(self, pid: int) -> bool:
        """向该进程的顶层窗口发 WM_CLOSE；没有窗口时返回 False。"""
        ...

    def terminate(self, pid: int) -> None:
        """结束整个进程树；进程已消失时静默返回。"""
        ...


class WindowsProcessHost:
    """Windows 进程实现：Popen 拉起、kernel32 探活、taskkill 收尾。"""

    def start(self, executable: Path, working_dir: Path) -> int:
        """以 ``working_dir`` 为工作目录启动程序，不给它控制台窗口。"""
        process = subprocess.Popen(
            [str(executable)],
            cwd=str(working_dir),
            env=clean_environment(),
            creationflags=_CREATE_NO_WINDOW,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return process.pid

    def is_running(self, pid: int) -> bool:
        """用退出码判断存活：句柄拿不到即视为已退出。"""
        handle = _kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            code = wintypes.DWORD()
            if not _kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return code.value == _STILL_ACTIVE
        finally:
            _kernel32.CloseHandle(handle)

    def running_pids(self, image_name: str) -> tuple[int, ...]:
        """按映像名取 pid；tasklist 失败按没有进程处理，交给启动失败路径。"""
        try:
            completed = subprocess.run(
                ["tasklist", "/FI", f"IMAGENAME eq {image_name}", "/FO", "CSV", "/NH"],
                capture_output=True,
                timeout=_TASKLIST_SECONDS,
                check=False,
                creationflags=_CREATE_NO_WINDOW,
            )
        except (OSError, subprocess.SubprocessError):
            return ()
        text = completed.stdout.decode("utf-8", errors="replace")
        pids: list[int] = []
        for row in csv.reader(io.StringIO(text)):
            if len(row) < 2:
                continue
            try:
                pids.append(int(row[1]))
            except ValueError:
                continue
        return tuple(pids)

    def request_stop(self, pid: int) -> bool:
        """给该 pid 的每个顶层窗口发 WM_CLOSE；返回是否至少发出一条。"""
        delivered = False

        def collect(hwnd: int, _param: int) -> bool:
            nonlocal delivered
            owner = wintypes.DWORD()
            _user32.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
            if owner.value == pid:
                _user32.PostMessageW(hwnd, _WM_CLOSE, 0, 0)
                delivered = True
            return True

        _user32.EnumWindows(_enum_windows_proc(collect), 0)
        return delivered

    def terminate(self, pid: int) -> None:
        """taskkill /T /F 结束整棵树；进程已不存在时忽略错误。"""
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                timeout=_TASKKILL_SECONDS,
                check=False,
                creationflags=_CREATE_NO_WINDOW,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return None


def clean_environment(base: Mapping[str, str] | None = None) -> dict[str, str]:
    """复制环境变量并剔除代理项：死代理会让 MAA 的启动请求卡住。"""
    environment = dict(os.environ if base is None else base)
    for name in _PROXY_VARIABLES:
        environment.pop(name, None)
    return environment
