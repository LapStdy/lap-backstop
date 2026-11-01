r"""独立看门狗：判死、豁免误报、经计划任务拉起、跨次验收与停手。

看门狗是短命进程：由计划任务每 5 分钟唤醒，3 秒内结束。它不依赖核心的
数据库与运行时服务（docs/08 独立看门狗一节）——核心死了也要照常醒来，
因此看门狗各模块只用标准库，不与 core.hostops 其余模块共享代码。本模块
是对外入口与检查编排；状态文件读写与停手报告在 watchdog_state，核心日志
尾部采集与文件名词法在 watchdog_logtail，进程与互斥量核对在
watchdog_process，日志文件名文法取自仅依赖标准库的 core.log_names。

跨次状态存 ``data\run\watchdog.state.json``：拉起后记 pending_start，下次检查
核对是否出现过新心跳；30 分钟滑动窗口内 3 次验收失败即停手，停手证据（窗口
失败次数、最近失败时刻、当时心跳与核心日志尾部）随标记一并落盘，由核心下次
启动取走并补写 ``host.watchdog_gave_up``（看门狗自身零数据库访问）。
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NamedTuple

from core.hostops.watchdog_logtail import (
    # 尾部字符上限的测试与文档口径是 watchdog._LOG_TAIL_MAX_CHARS，同名再导出。
    _LOG_TAIL_MAX_CHARS as _LOG_TAIL_MAX_CHARS,
)
from core.hostops.watchdog_logtail import (
    _latest_core_log,
    _read_tail_lines,
)
from core.hostops.watchdog_process import (
    _CREATION_TOLERANCE,
    _acquire_watchdog_mutex,
    _kernel32,
    _process_creation_time,
    is_pid_alive,
)
from core.hostops.watchdog_state import (
    STATE_FILENAME,
    GaveUpReport,
    WatchdogState,
    _build_report,
    _iso_seconds,
    _parse_iso,
    read_state,
    take_gave_up_event_payload,
    write_state,
)

WATCHDOG_MUTEX_NAME = "Global\\lap-backstop-watchdog"
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

Runner = Callable[..., subprocess.CompletedProcess[bytes]]


@dataclass(frozen=True, slots=True)
class CheckPolicy:
    """判死与停手的数值；唯一真源是 [host] 默认值，由入口构造后传入。"""

    stale_seconds: float
    acceptance_seconds: float
    give_up_window: timedelta
    give_up_max_failures: int


@dataclass(frozen=True, slots=True)
class CheckInputs:
    """一次检查的注入项：时刻、数值、schtasks 执行器、pid 判定与 dead-man 挂载点。"""

    now: datetime
    policy: CheckPolicy
    runner: Runner
    pid_alive: Callable[[int], bool]
    # dead-man 挂载点（core.hostops.deadman）；注入件必须自行吞掉所有异常，
    # None 表示不挂载（进程内测试与未启用 dead-man 的路径都走 None）。
    deadman: Callable[[CheckReport], None] | None = None


@dataclass(frozen=True, slots=True)
class CheckReport:
    """一轮检查的结果快照：dead-man 据此决定发心跳还是报 fail。"""

    passed: bool
    action: str
    heartbeat_age_s: float | None
    core_pid: int | None


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


class DeathOutcome(NamedTuple):
    """判死后的处置结果：动作名与「是否按通过处理」（误报豁免记通过）。"""

    action: str
    passed: bool


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
        outcome = _act_on_death(data_root, state, failures, inputs)
        action, passed = outcome.action, outcome.passed
    else:
        state["failures"] = [moment.isoformat() for moment in failures]
        action = ACTION_NONE
        passed = True
    write_state(state_path, state)
    log_path = data_root / "logs" / LOG_FILENAME
    _append_log(
        log_path,
        CheckLogLine(at=inputs.now, heartbeat_age_s=age, core_pid=core_pid, action=action),
    )
    if inputs.deadman is not None:
        inputs.deadman(
            CheckReport(passed=passed, action=action, heartbeat_age_s=age, core_pid=core_pid)
        )


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
) -> DeathOutcome:
    """死了之后做什么：误报豁免、停手或请求拉起；就地更新 state。

    Returns:
        动作名与「是否按通过处理」。误报豁免说明核心确认存活，dead-man 照常
        发心跳、不发 fail；拉起失败与停手都按未通过处理。
    """
    run_dir = data_root / "run"
    state["failures"] = [moment.isoformat() for moment in failures]
    if _core_actually_running(run_dir, alive=inputs.pid_alive):
        return DeathOutcome(ACTION_NONE, passed=True)
    if len(failures) >= inputs.policy.give_up_max_failures:
        return _handle_give_up(data_root, state, failures)
    try:
        completed = inputs.runner(
            ["schtasks", "/run", "/tn", CORE_TASK_NAME],
            capture_output=True,
            check=False,
            timeout=_SCHEDULING_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return DeathOutcome(ACTION_NONE, passed=False)
    if completed.returncode != 0:
        return DeathOutcome(ACTION_NONE, passed=False)
    state["pending_start"] = inputs.now.isoformat()
    return DeathOutcome(ACTION_START_REQUESTED, passed=False)


def _handle_give_up(
    data_root: Path,
    state: WatchdogState,
    failures: list[datetime],
) -> DeathOutcome:
    """停手分支：取证载荷就地写进 state，待核心下次启动取走补写（docs/08 停手一节）。"""
    state["gave_up_pending_report"] = True
    state["gave_up_report"] = _collect_gave_up_report(data_root, failures)
    return DeathOutcome(ACTION_GAVE_UP, passed=False)


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


def _collect_gave_up_report(data_root: Path, failures: list[datetime]) -> GaveUpReport:
    """停手当时取证：窗口失败次数、最近失败时刻、最后心跳与核心日志尾部。

    心跳与日志是核心死亡现场的全部线索，停手后核心可能长期起不来，
    证据必须在停手当时落盘，不能等核心下次启动再补采。
    """
    last = failures[-1] if failures else None
    return _build_report(
        failed_count=len(failures),
        last_failure_at=_iso_seconds(last),
        heartbeat=_read_json_object(data_root / "run" / HEARTBEAT_FILENAME),
        log_tail=_core_log_tail(data_root / "logs"),
    )


def _read_json_object(path: Path) -> dict[str, Any] | None:
    """读文件里的 JSON 对象；缺失、损坏或顶层不是对象返回 None。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _core_log_tail(logs_dir: Path) -> list[str]:
    """最新核心日志文件的尾部非空行（最多 50 行、总计不超过 8 KiB）；无日志或不可读返回空。"""
    latest = _latest_core_log(logs_dir)
    if latest is None:
        return []
    return _read_tail_lines(latest)


def _read_json(path: Path) -> dict[str, Any]:
    return _read_json_object(path) or {}


def _mtime_age_seconds(path: Path, *, now: datetime) -> float | None:
    try:
        return now.timestamp() - path.stat().st_mtime
    except OSError:
        return None


@dataclass(frozen=True, slots=True)
class CheckLogLine:
    """一行检查记录的字段：字段名即写盘键，取值口径见 docs/08 记录一节。"""

    at: datetime
    heartbeat_age_s: float | None
    core_pid: int | None
    action: str


def _append_log(path: Path, line: CheckLogLine) -> None:
    """追加一行检查记录（docs/08 记录一节的四个字段）。

    写盘失败只吞 OSError：缺一行记录不拦住检查本身（docs/08 自约束）。
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "at": line.at.isoformat(),
            "heartbeat_age_s": line.heartbeat_age_s,
            "core_pid": line.core_pid,
            "action": line.action,
        }
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        return


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
    "CheckReport",
    "GaveUpReport",
    "WatchdogState",
    "is_pid_alive",
    "read_state",
    "run_check",
    "take_gave_up_event_payload",
    "write_state",
]
