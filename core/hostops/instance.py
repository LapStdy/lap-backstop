"""单实例三件套：命名互斥量（权威）、锁文件（排障）、pid 存活与复用判定。

互斥量挡住并发核心；锁文件只服务排障与看门狗拉起前的核对（docs/08 单实例一节）。
全部走 ctypes 与标准库，不引第三方依赖。
"""

from __future__ import annotations

import ctypes
import json
import sqlite3
import winreg
from ctypes import wintypes
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypedDict

from contracts import EventType
from storage.repositories.events import EventLog

MUTEX_NAME = "Global\\lap-backstop-core"
LOCK_FILENAME = "core.lock"

ERROR_ALREADY_EXISTS = 183
STILL_ACTIVE = 259
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000

_MACHINEGUID_KEY = "SOFTWARE\\Microsoft\\Cryptography"
_EPOCH_1601 = datetime(1601, 1, 1, tzinfo=UTC)
# started_at 来自进程内时钟、创建时间来自内核 FILETIME，允许秒级偏差。
_CREATION_TOLERANCE = timedelta(seconds=5)

_kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
_kernel32.CreateMutexW.restype = wintypes.HANDLE
# wintypes 不定义 SECURITY_ATTRIBUTES；本模块只用默认安全属性（传 NULL）。
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


class LockContent(TypedDict):
    """锁文件 schema：pid、启动时刻、可执行体路径、机器标识（docs/08 单实例一节）。"""

    pid: int
    started_at: str
    exe_path: str
    boot_id: str


def acquire_named_mutex(name: str) -> int | None:
    """取命名互斥量；名字已被占返回 None（第二实例），系统级失败抛 OSError。

    Returns:
        互斥量句柄，调用方负责用 :func:`release_named_mutex` 释放。
    """
    handle = _kernel32.CreateMutexW(None, False, name)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    if ctypes.get_last_error() == ERROR_ALREADY_EXISTS:
        _kernel32.CloseHandle(handle)
        return None
    return handle


def release_named_mutex(handle: int) -> None:
    """释放互斥量句柄；进程退出时系统也会回收，这里负责正常路径的显式释放。"""
    _kernel32.CloseHandle(handle)


def read_boot_id() -> str:
    """读注册表 MachineGuid 当 boot_id；读不到降级为空串，由调用方记 warn。

    Windows 重装或系统复位会换 Guid，足以区分「同一台机器的两次开机」。
    """
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _MACHINEGUID_KEY) as key:
            value, _kind = winreg.QueryValueEx(key, "MachineGuid")
    except OSError:
        return ""
    return value if isinstance(value, str) else ""


def write_lock_file(path: Path, content: LockContent) -> None:
    """写锁文件；覆盖式写，崩溃残留即被顶掉（并发已被互斥量挡住）。

    Raises:
        OSError: 目录不可写等。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def read_lock_file(path: Path) -> LockContent | None:
    """读锁文件；缺失、损坏、字段不齐一律 None，调用方按残留或无锁处置。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    pid = data.get("pid")
    started_at = data.get("started_at")
    exe_path = data.get("exe_path")
    boot_id = data.get("boot_id")
    if not isinstance(pid, int) or isinstance(pid, bool):
        return None
    if not (isinstance(started_at, str) and isinstance(exe_path, str) and isinstance(boot_id, str)):
        return None
    return {
        "pid": pid,
        "started_at": started_at,
        "exe_path": exe_path,
        "boot_id": boot_id,
    }


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


def process_creation_time(pid: int) -> datetime | None:
    """进程创建时刻（UTC）；进程不可查时返回 None。"""
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


def process_matches_lock(lock: LockContent, *, boot_id: str) -> bool:
    """锁文件描述的进程现在是否仍活着且未被 pid 复用顶替。

    看门狗拉起前用这份判定豁免误报：pid 死、跨重启（boot_id 变）、创建时间
    对不上 started_at（pid 复用）都判不匹配；创建时间取不到或 started_at
    解析失败时无法证伪，只信 pid 存活，按匹配处理。
    """
    if not is_pid_alive(lock["pid"]):
        return False
    if lock["boot_id"] and boot_id and lock["boot_id"] != boot_id:
        return False
    creation = process_creation_time(lock["pid"])
    if creation is None:
        return True
    try:
        started = datetime.fromisoformat(lock["started_at"])
    except ValueError:
        return True
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return abs(creation - started) <= _CREATION_TOLERANCE


def report_second_instance(database: Path, *, at: datetime) -> None:
    """尽力把 core.second_instance 事件落库；失败不抛，第二实例要的是尽快退出。

    用 1 秒忙等上限的独立连接，不与第一实例的长连接纠缠。
    """
    try:
        connection = sqlite3.connect(database, timeout=1.0, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 1000")
            EventLog(connection).append_global(
                EventType.CORE_EVENT,
                {"event": "core.second_instance", "detail": "互斥量被占，拒绝第二个实例"},
                at=at,
            )
        finally:
            connection.close()
    except (OSError, ValueError, sqlite3.Error):
        return


__all__ = [
    "LOCK_FILENAME",
    "MUTEX_NAME",
    "LockContent",
    "acquire_named_mutex",
    "is_pid_alive",
    "process_creation_time",
    "process_matches_lock",
    "read_boot_id",
    "read_lock_file",
    "release_named_mutex",
    "report_second_instance",
    "write_lock_file",
]
