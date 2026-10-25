r"""独立看门狗：判死、豁免误报、经计划任务拉起、跨次验收与停手。

看门狗是短命进程：由计划任务每 5 分钟唤醒，3 秒内结束。它不依赖核心的任何
代码路径（docs/08 独立看门狗一节）——核心包损坏也要照常醒来，因此本模块只
用标准库，锁文件解析与进程核对在此独立实现，不与 core.hostops 其余模块共享。

跨次状态存 ``data\run\watchdog.state.json``：拉起后记 pending_start，下次检查
核对是否出现过新心跳；30 分钟滑动窗口内 3 次验收失败即停手并留下待上报标记，
由核心下次启动补写 ``host.watchdog_gave_up``（看门狗自身零数据库访问）。
"""

from __future__ import annotations

import ctypes
import json
import os
import subprocess
from collections.abc import Callable
from ctypes import wintypes
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypedDict

WATCHDOG_MUTEX_NAME = "Global\\lap-backstop-watchdog"
STATE_FILENAME = "watchdog.state.json"
LOG_FILENAME = "watchdog.jsonl"
HEARTBEAT_FILENAME = "core.heartbeat"
LOCK_FILENAME = "core.lock"
CORE_TASK_NAME = "lap-backstop-core"

ACTION_NONE = "none"
ACTION_START_REQUESTED = "start_requested"
ACTION_GAVE_UP = "gave_up"

# 单次检查 3 秒内必须结束（docs/08 自约束）：给 schtasks 留的窗口比它更小。
_SCHEDULING_TIMEOUT_SECONDS = 2.0
ERROR_ALREADY_EXISTS = 183
STILL_ACTIVE = 259
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_EPOCH_1601 = datetime(1601, 1, 1, tzinfo=UTC)
# started_at 来自进程内时钟、创建时间来自内核 FILETIME，允许秒级偏差。
_CREATION_TOLERANCE = timedelta(seconds=5)

Runner = Callable[..., subprocess.CompletedProcess[bytes]]

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


@dataclass(frozen=True, slots=True)
class CheckPolicy:
    """判死与停手的数值；唯一真源是 [host] 默认值，由入口构造后传入。"""

    stale_seconds: float
    acceptance_seconds: float
    give_up_window: timedelta
    give_up_max_failures: int


@dataclass(frozen=True, slots=True)
class CheckInputs:
    """一次检查的注入项：时刻、数值、schtasks 执行器与 pid 判定。"""

    now: datetime
    policy: CheckPolicy
    runner: Runner
    pid_alive: Callable[[int], bool]


class WatchdogState(TypedDict, total=False):
    """状态文件 schema：拉起待验收时刻、验收失败时刻列表、停手待上报标记。"""

    pending_start: str | None
    failures: list[str]
    gave_up_pending_report: bool


def run_check(data_root: Path, *, inputs: CheckInputs) -> int:
    """单次检查，返回进程退出码；互斥量被占（重复触发）直接 0。"""
    handle = _acquire_watchdog_mutex(WATCHDOG_MUTEX_NAME)
    if handle is None:
        return 0
    try:
        _execute_check(data_root, inputs)
        return 0
    finally:
        _kernel32.CloseHandle(handle)


def read_state(path: Path) -> WatchdogState:
    """读状态文件；缺失或损坏一律按初始态处理（人工删除即复位）。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    failures = data.get("failures")
    pending_start = data.get("pending_start")
    return {
        "pending_start": pending_start if isinstance(pending_start, str) else None,
        "failures": (
            [item for item in failures if isinstance(item, str)]
            if isinstance(failures, list)
            else []
        ),
        "gave_up_pending_report": bool(data.get("gave_up_pending_report")),
    }


def write_state(path: Path, state: WatchdogState) -> None:
    """原子写状态文件（临时文件加替换）：本文件由看门狗与核心两个进程共写。

    Raises:
        OSError: 目录不可写等；调用方记日志退出，状态丢失可接受——下次检查
            会重新判死重新拉起，方向安全。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(json.dumps(state, ensure_ascii=False, sort_keys=True))
    os.replace(temporary, path)


def claim_gave_up_report(state_path: Path) -> str | None:
    """取走停手待上报标记；有则清标记并返回事件 detail，无则 None。

    供核心 serve 启动时调用：看门狗零数据库访问，事件由核心补写。
    """
    state = read_state(state_path)
    if not state.get("gave_up_pending_report"):
        return None
    failures = state.get("failures", [])
    tail = failures[-1] if failures else "未知时刻"
    state["gave_up_pending_report"] = False
    write_state(state_path, state)
    return f"看门狗已停止拉起：窗口内 {len(failures)} 次验收失败，最近一次 {tail}"


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


def _execute_check(data_root: Path, inputs: CheckInputs) -> None:
    run_dir = data_root / "run"
    state_path = run_dir / STATE_FILENAME
    state = read_state(state_path)
    failures = _failures_in_window(
        state.get("failures", []), inputs.now, inputs.policy.give_up_window
    )
    dead, age, core_pid = _judge_death(run_dir, inputs)
    _settle_acceptance(state, failures, age, inputs)
    if dead:
        action = _act_on_death(run_dir, state, failures, inputs)
    else:
        state["failures"] = [moment.isoformat() for moment in failures]
        action = ACTION_NONE
    write_state(state_path, state)
    log_path = data_root / "logs" / LOG_FILENAME
    _append_log(log_path, now=inputs.now, age=age, core_pid=core_pid, action=action)


def _judge_death(run_dir: Path, inputs: CheckInputs) -> tuple[bool, float | None, int | None]:
    """按三条件判死：心跳文件缺失、mtime 超限、心跳 pid 不存活。"""
    heartbeat_path = run_dir / HEARTBEAT_FILENAME
    age = _mtime_age_seconds(heartbeat_path, now=inputs.now)
    content = _read_json(heartbeat_path)
    core_pid = content.get("pid") if isinstance(content.get("pid"), int) else None
    dead = age is None or age > inputs.policy.stale_seconds
    if not dead and core_pid is not None:
        dead = not inputs.pid_alive(core_pid)
    return dead, age, core_pid


def _settle_acceptance(
    state: WatchdogState,
    failures: list[datetime],
    age: float | None,
    inputs: CheckInputs,
) -> None:
    """结算上次拉起的验收：到期仍无新心跳计一次失败，并清掉待验收标记。"""
    pending_start = _parse_iso(state.get("pending_start"))
    if pending_start is None:
        return
    due = timedelta(seconds=inputs.policy.acceptance_seconds)
    if inputs.now - pending_start < due:
        return
    # age 覆盖不了 pending_start 之后的时段，说明拉起后从未出现新心跳。
    if age is None or age >= (inputs.now - pending_start).total_seconds():
        failures.append(inputs.now)
    state["pending_start"] = None


def _act_on_death(
    run_dir: Path,
    state: WatchdogState,
    failures: list[datetime],
    inputs: CheckInputs,
) -> str:
    """死了之后做什么：误报豁免、停手或请求拉起；就地更新 state 并返回动作名。"""
    recorded = [moment.isoformat() for moment in failures]
    state["failures"] = recorded
    if _core_actually_running(run_dir, alive=inputs.pid_alive):
        return ACTION_NONE
    if len(failures) >= inputs.policy.give_up_max_failures:
        state["gave_up_pending_report"] = True
        return ACTION_GAVE_UP
    try:
        completed = inputs.runner(
            ["schtasks", "/run", "/tn", CORE_TASK_NAME],
            capture_output=True,
            check=False,
            timeout=_SCHEDULING_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return ACTION_NONE
    if completed.returncode != 0:
        return ACTION_NONE
    state["pending_start"] = inputs.now.isoformat()
    return ACTION_START_REQUESTED


def _core_actually_running(run_dir: Path, *, alive: Callable[[int], bool]) -> bool:
    """豁免核对：锁文件描述的进程仍活着且未被 pid 复用顶替，判死按误报处理。

    时钟前跳会让 mtime 误判死亡（docs/08 风险一节）；核对通过就不拉起。
    """
    lock = _read_json(run_dir / LOCK_FILENAME)
    pid = lock.get("pid")
    started_at = lock.get("started_at")
    if not isinstance(pid, int) or not isinstance(started_at, str):
        return False
    if not alive(pid):
        return False
    creation = _process_creation_time(pid)
    if creation is None:
        return True
    try:
        started = datetime.fromisoformat(started_at)
    except ValueError:
        return True
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return abs(creation - started) <= _CREATION_TOLERANCE


def _failures_in_window(failures: list[str], now: datetime, window: timedelta) -> list[datetime]:
    """把失败时刻解析回来并剔除滑出窗口的旧记录。"""
    parsed: list[datetime] = []
    for item in failures:
        try:
            moment = datetime.fromisoformat(item)
        except ValueError:
            continue
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        if now - moment < window:
            parsed.append(moment)
    return parsed


def _parse_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _mtime_age_seconds(path: Path, *, now: datetime) -> float | None:
    try:
        return now.timestamp() - path.stat().st_mtime
    except OSError:
        return None


def _append_log(
    path: Path,
    *,
    now: datetime,
    age: float | None,
    core_pid: int | None,
    action: str,
) -> None:
    """追加一行检查记录（docs/08 记录一节的四个字段）。"""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            {
                "at": now.isoformat(),
                "heartbeat_age_s": age,
                "core_pid": core_pid,
                "action": action,
            },
            ensure_ascii=False,
        )
        with path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")
    except OSError:
        return


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


__all__ = [
    "ACTION_GAVE_UP",
    "ACTION_NONE",
    "ACTION_START_REQUESTED",
    "CORE_TASK_NAME",
    "HEARTBEAT_FILENAME",
    "LOCK_FILENAME",
    "LOG_FILENAME",
    "STATE_FILENAME",
    "WATCHDOG_MUTEX_NAME",
    "CheckInputs",
    "CheckPolicy",
    "WatchdogState",
    "claim_gave_up_report",
    "is_pid_alive",
    "read_state",
    "run_check",
    "write_state",
]
