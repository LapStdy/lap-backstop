r"""独立看门狗：判死、豁免误报、经计划任务拉起、跨次验收与停手。

看门狗是短命进程：由计划任务每 5 分钟唤醒，3 秒内结束。它不依赖核心的任何
代码路径（docs/08 独立看门狗一节）——核心包损坏也要照常醒来，因此本模块只
用标准库，锁文件解析与进程核对在此独立实现，不与 core.hostops 其余模块共享。

跨次状态存 ``data\run\watchdog.state.json``：拉起后记 pending_start，下次检查
核对是否出现过新心跳；30 分钟滑动窗口内 3 次验收失败即停手，停手证据（窗口
失败次数、最近失败时刻、当时心跳与核心日志尾部）随标记一并落盘，由核心下次
启动取走并补写 ``host.watchdog_gave_up``（看门狗自身零数据库访问）。
"""

from __future__ import annotations

import ctypes
import json
import os
import re
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
# 停手补报事件名；看门狗只落状态文件，停手载荷由核心启动取走补写（docs/08）。
WATCHDOG_GAVE_UP_EVENT = "host.watchdog_gave_up"

# 单次检查 3 秒内必须结束（docs/08 自约束）：给 schtasks 留的窗口比它更小。
_SCHEDULING_TIMEOUT_SECONDS = 2.0
# 核心日志文件名（docs/08 磁盘与日志：core-YYYY-MM-DD[.N].jsonl，按日与续号取最新）。
_CORE_LOG_NAME = re.compile(r"^core-(?P<day>\d{4}-\d{2}-\d{2})(?:\.(?P<index>\d+))?\.jsonl$")
_LOG_TAIL_LINES = 50
# 核心日志单文件最大 200 MB：全量读会撞上 3 秒自约束，只从文件尾取一个字节窗口再裁行数。
_LOG_TAIL_WINDOW_BYTES = 262_144
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


class GaveUpReport(TypedDict):
    """停手当时的取证载荷：窗口失败数、最近失败时刻、心跳内容与核心日志尾部。"""

    failed_count: int
    last_failure_at: str | None
    heartbeat: dict[str, Any] | None
    log_tail: list[str]


class WatchdogState(TypedDict, total=False):
    """状态文件 schema：拉起待验收时刻、验收失败时刻列表、停手待上报标记与停手报告。"""

    pending_start: str | None
    failures: list[str]
    gave_up_pending_report: bool
    gave_up_report: GaveUpReport | None


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
        "gave_up_report": _parse_gave_up_report(data.get("gave_up_report")),
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


def claim_gave_up_report(state_path: Path) -> dict[str, object] | None:
    """取走停手待上报载荷；有则清标记并返回事件 payload，无则 None。

    payload 是人读 detail（通知路径依赖它）加 failed_count、last_failure_at、
    heartbeat、log_tail 四个证据字段。旧状态文件只有布尔标记、没有可解析
    报告时走兜底：失败次数与最近失败时刻从已存 failures 推，心跳与日志
    尾部为空。供核心 serve 启动时调用：看门狗零数据库访问，事件由核心补写。
    """
    state = read_state(state_path)
    if not state.get("gave_up_pending_report"):
        return None
    report = state.get("gave_up_report")
    if report is None:
        report = _fallback_gave_up_report(state)
    state["gave_up_pending_report"] = False
    state["gave_up_report"] = None
    write_state(state_path, state)
    return _gave_up_event_payload(report)


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
        action = _act_on_death(data_root, state, failures, inputs)
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
    data_root: Path,
    state: WatchdogState,
    failures: list[datetime],
    inputs: CheckInputs,
) -> str:
    """死了之后做什么：误报豁免、停手或请求拉起；就地更新 state 并返回动作名。"""
    run_dir = data_root / "run"
    recorded = [moment.isoformat() for moment in failures]
    state["failures"] = recorded
    if _core_actually_running(run_dir, alive=inputs.pid_alive):
        return ACTION_NONE
    if len(failures) >= inputs.policy.give_up_max_failures:
        state["gave_up_pending_report"] = True
        state["gave_up_report"] = _collect_gave_up_report(data_root, failures)
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


def _collect_gave_up_report(data_root: Path, failures: list[datetime]) -> GaveUpReport:
    """停手当时取证：窗口失败次数、最近失败时刻、最后心跳与核心日志尾部。

    心跳与日志是核心死亡现场的全部线索，停手后核心可能长期起不来，
    证据必须在停手当时落盘，不能等核心下次启动再补采。
    """
    last = failures[-1] if failures else None
    return {
        "failed_count": len(failures),
        "last_failure_at": _iso_seconds(last),
        "heartbeat": _read_heartbeat_snapshot(data_root / "run" / HEARTBEAT_FILENAME),
        "log_tail": _core_log_tail(data_root / "logs"),
    }


def _parse_gave_up_report(value: object) -> GaveUpReport | None:
    """校验停手报告形态；四字段不全或类型不符按不可解析处理，claim 走兜底。"""
    if not isinstance(value, dict):
        return None
    failed_count = value.get("failed_count")
    last_failure_at = value.get("last_failure_at")
    heartbeat = value.get("heartbeat")
    log_tail = value.get("log_tail")
    if not isinstance(failed_count, int) or isinstance(failed_count, bool):
        return None
    if last_failure_at is not None and not isinstance(last_failure_at, str):
        return None
    if heartbeat is not None and not isinstance(heartbeat, dict):
        return None
    if not isinstance(log_tail, list) or not all(isinstance(item, str) for item in log_tail):
        return None
    return {
        "failed_count": failed_count,
        "last_failure_at": last_failure_at,
        "heartbeat": heartbeat,
        "log_tail": list(log_tail),
    }


def _fallback_gave_up_report(state: WatchdogState) -> GaveUpReport:
    """兜底载荷：旧状态文件只有布尔标记时，从已存失败记录推证据字段。"""
    failures = state.get("failures", [])
    last = _parse_iso(failures[-1]) if failures else None
    return {
        "failed_count": len(failures),
        "last_failure_at": _iso_seconds(last),
        "heartbeat": None,
        "log_tail": [],
    }


def _gave_up_event_payload(report: GaveUpReport) -> dict[str, object]:
    """停手事件 payload：人读 detail 加四个证据字段（docs/08 停手一节）。"""
    last = report["last_failure_at"]
    detail = (
        f"看门狗已停止拉起：窗口内 {report['failed_count']} 次验收失败，"
        f"最近一次 {last if last is not None else '未知时刻'}"
    )
    return {
        "detail": detail,
        "failed_count": report["failed_count"],
        "last_failure_at": last,
        "heartbeat": report["heartbeat"],
        "log_tail": list(report["log_tail"]),
    }


def _iso_seconds(moment: datetime | None) -> str | None:
    """格式化为秒级 UTC ISO8601；None 原样通过。"""
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat(timespec="seconds")


def _read_heartbeat_snapshot(path: Path) -> dict[str, Any] | None:
    """读停手当时的心跳对象原样；读不到或不是 JSON 对象按无心跳处理。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _core_log_tail(logs_dir: Path) -> list[str]:
    """最新核心日志文件的尾部非空行（最多 50 行）；无日志或不可读返回空。"""
    latest = _latest_core_log(logs_dir)
    if latest is None:
        return []
    return _read_tail_lines(latest)


def _latest_core_log(logs_dir: Path) -> Path | None:
    """按名字里的日期与续号挑最新的 core-YYYY-MM-DD[.N].jsonl。"""
    try:
        entries = list(logs_dir.iterdir())
    except OSError:
        return None
    latest: tuple[tuple[str, int], Path] | None = None
    for path in entries:
        parsed = _parse_core_log_name(path.name)
        if parsed is None or not path.is_file():
            continue
        if latest is None or parsed > latest[0]:
            latest = (parsed, path)
    return None if latest is None else latest[1]


def _parse_core_log_name(name: str) -> tuple[str, int] | None:
    """解析核心日志文件名；返回日期串与续号（无续号按 1）。"""
    match = _CORE_LOG_NAME.fullmatch(name)
    if match is None:
        return None
    index = match.group("index")
    return match.group("day"), 1 if index is None else int(index)


def _read_tail_lines(path: Path) -> list[str]:
    """读文件尾部的字节窗口，返回最后 50 条非空行（跳过空行）。"""
    try:
        with path.open("rb") as stream:
            size = stream.seek(0, os.SEEK_END)
            start = max(0, size - _LOG_TAIL_WINDOW_BYTES)
            stream.seek(start)
            data = stream.read()
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").split("\n")
    if start > 0 and lines:
        # 窗口起点可能落在行中间，第一行是半截，弃掉。
        lines = lines[1:]
    return [line for line in lines if line.strip()][-_LOG_TAIL_LINES:]


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
    "WATCHDOG_GAVE_UP_EVENT",
    "WATCHDOG_MUTEX_NAME",
    "CheckInputs",
    "CheckPolicy",
    "GaveUpReport",
    "WatchdogState",
    "claim_gave_up_report",
    "is_pid_alive",
    "read_state",
    "run_check",
    "write_state",
]
