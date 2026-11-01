"""宿主进程与互斥量核对：pid 存活判定、进程创建时间核对与看门狗单实例互斥量。

kernel32 绑定集中在此：看门狗只按 PROCESS_QUERY_LIMITED_INFORMATION 打开
进程，打不开或已退出都算死。创建时间与锁文件 started_at 的比对窗口对应
两套时钟来源的偏差：started_at 来自进程内时钟、创建时间来自内核 FILETIME。
"""

from __future__ import annotations

import ctypes
from ctypes import wintypes
from datetime import UTC, datetime, timedelta

ERROR_ALREADY_EXISTS = 183
STILL_ACTIVE = 259
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_EPOCH_1601 = datetime(1601, 1, 1, tzinfo=UTC)
# started_at 来自进程内时钟、创建时间来自内核 FILETIME，允许秒级偏差。
_CREATION_TOLERANCE = timedelta(seconds=5)

_kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
_kernel32.CreateMutexW.restype = wintypes.HANDLE
_kernel32.CreateMutexW.argtypes = (ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR)
_kernel32.CloseHandle.restype = wintypes.BOOL
_kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
_kernel32.OpenProcess.restype = wintypes.HANDLE
_kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
_kernel32.GetExitCodeProcess.restype = wintypes.BOOL
_kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, wintypes.LPDWORD)
_kernel32.GetProcessTimes.restype = wintypes.BOOL
_kernel32.GetProcessTimes.argtypes = (
    wintypes.HANDLE,
    wintypes.LPFILETIME,
    wintypes.LPFILETIME,
    wintypes.LPFILETIME,
    wintypes.LPFILETIME,
)


def is_pid_alive(pid: int) -> bool:
    """pid 对应进程是否仍在运行；打不开或已退出都算死。"""
    handle = _kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return False
    try:
        exit_code = wintypes.DWORD()
        if not _kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        # STILL_ACTIVE 表示未退出；真退出码恰好是 259 的进程会被误判为活，接受该歧义。
        return exit_code.value == STILL_ACTIVE
    finally:
        _kernel32.CloseHandle(handle)


def _acquire_watchdog_mutex(name: str) -> int | None:
    """取看门狗互斥量；被占返回 None（重复触发按 docs/08 单实例一节退 0）。"""
    handle = _kernel32.CreateMutexW(None, False, name)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    if ctypes.get_last_error() == ERROR_ALREADY_EXISTS:
        _kernel32.CloseHandle(handle)
        return None
    return handle


def _process_creation_time(pid: int) -> datetime | None:
    handle = _kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return None
    try:
        creation = wintypes.FILETIME()
        exit_time = wintypes.FILETIME()
        kernel = wintypes.FILETIME()
        user = wintypes.FILETIME()
        if not _kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            return None
        stamp = (creation.dwHighDateTime << 32) | creation.dwLowDateTime
        return _EPOCH_1601 + timedelta(microseconds=stamp / 10)
    finally:
        _kernel32.CloseHandle(handle)
