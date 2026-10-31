r"""启动校时与补跑暂停：w32tm 执行、偏移测量与暂停标记落盘。

serve 启动时执行一次 ``w32tm /resync``，再用 ``w32tm /stripchart`` 读本机墙钟与
时间源的偏移（docs/08 时钟与 NTP）。命令不存在、超时或非零退出都只落结果，不阻断
启动；偏移超过阈值记 host.clock_skew 并暂停补跑：暂停期间调度跳过错过的发生点，
下次启动偏移回到阈值内才解除；偏移不可测时不下漂移结论，标记保持现状（docs/10
结论有证据）。执行器经 Runner 注入，测试替换为假件（docs/11 外部调用）。
"""

from __future__ import annotations

import json
import locale
import re
import sqlite3
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from core.api.store import put_settings
from storage.db.transaction import transaction
from storage.defaults import default

SYNC_COMMAND = ("w32tm", "/resync")
OFFSET_COMMAND = ("w32tm", "/stripchart", "/dataonly", "/samples:1")
TIMESYNC_EVENT = "host.timesync_checked"
SKEW_EVENT = "host.clock_skew"
CATCH_UP_PAUSED_KEY = "scheduler.catch_up_paused"

# 校时在 serve 启动路径上，时间源不可达时 w32tm 会等满超时；超时按失败处理，下次启动重试。
_SYNC_TIMEOUT_SECONDS = 10.0
_SKEW_THRESHOLD_SECONDS = float(default("host", "clock_skew_seconds"))

# stripchart 的偏移行形如「16:02:35, +00.1234567s」；多行输出取最后一个样本。
_OFFSET_PATTERN = re.compile(r"([+-]\d+(?:\.\d+)?)s\s*$", re.MULTILINE)

Runner = Callable[..., subprocess.CompletedProcess[bytes]]


@dataclass(frozen=True, slots=True)
class ClockCheck:
    """一次启动校时的结果：同步是否成功与校时后的墙钟偏移。

    ``offset_seconds`` 为 None 表示偏移不可测：测不出就不下漂移结论。
    """

    sync_ok: bool
    sync_detail: str
    offset_seconds: float | None


def startup_check(runner: Runner) -> ClockCheck:
    """执行一次校时并测量偏移；任何失败都转成结果，不抛异常、不阻断启动。

    Args:
        runner: 进程执行器；生产是 subprocess.run，测试注入假件。

    Returns:
        ClockCheck: 同步结果（sync_ok、sync_detail）与偏移秒数（测不出为 None）。
    """
    sync_ok, sync_detail = _resync(runner)
    return ClockCheck(sync_ok, sync_detail, _measure_offset(runner))


def parse_offset_seconds(output: str) -> float | None:
    """解析 stripchart 输出里的偏移秒数，带符号；找不到偏移行返回 None。

    Args:
        output: ``w32tm /stripchart /dataonly`` 的输出，偏移行形如
            「16:02:35, +00.1234567s」。

    Returns:
        偏移秒数，取 stripchart 报告值原样带符号。
    """
    matches = _OFFSET_PATTERN.findall(output)
    if not matches:
        return None
    return float(matches[-1])


def catch_up_paused(connection: sqlite3.Connection) -> bool:
    """读补跑暂停标记；键缺失、坏 JSON 或类型不是布尔都按未暂停处理。"""
    row = connection.execute(
        "SELECT value FROM settings WHERE key = ?", (CATCH_UP_PAUSED_KEY,)
    ).fetchone()
    if row is None:
        return False
    try:
        value = json.loads(str(row[0]))
    except json.JSONDecodeError:
        return False
    return isinstance(value, bool) and value


def apply_startup_check(
    connection: sqlite3.Connection,
    check: ClockCheck,
    *,
    at: datetime,
) -> list[tuple[str, Mapping[str, object]]]:
    """把一次启动校时落成补跑暂停标记与事件载荷。

    偏移超过阈值置暂停标记并记 host.clock_skew；偏移在阈值内解除已有标记；
    偏移不可测时标记保持现状。事件只报告当次测量，不携带历史。

    Args:
        connection: 内核单写连接。
        check: startup_check 的结果。
        at: 事件与 settings 的时间戳。

    Returns:
        按落库顺序的事件（事件名与载荷），由调用方统一写全局事件表。

    Raises:
        sqlite3.Error: 数据库不可写。
    """
    detail_parts = [check.sync_detail, _offset_text(check)]
    events: list[tuple[str, Mapping[str, object]]] = []
    if _is_skewed(check):
        if not catch_up_paused(connection):
            _set_catch_up_paused(connection, True, at=at)
            detail_parts.append("补跑已暂停")
        events.append(
            (
                SKEW_EVENT,
                {"detail": "；".join(detail_parts), "offset_seconds": check.offset_seconds},
            )
        )
    elif check.offset_seconds is not None and catch_up_paused(connection):
        _set_catch_up_paused(connection, False, at=at)
        detail_parts.append("补跑暂停已解除")
    events.append((TIMESYNC_EVENT, {"detail": "；".join(detail_parts), "ok": check.sync_ok}))
    return events


def _is_skewed(check: ClockCheck) -> bool:
    """偏移可测且绝对值超过阈值才算漂移；测不出不下结论。"""
    offset = check.offset_seconds
    return offset is not None and abs(offset) > _SKEW_THRESHOLD_SECONDS


def _resync(runner: Runner) -> tuple[bool, str]:
    """执行一次 w32tm /resync；命令不存在、超时与非零退出都归为失败并给出原因。"""
    try:
        completed = runner(
            list(SYNC_COMMAND),
            capture_output=True,
            check=False,
            timeout=_SYNC_TIMEOUT_SECONDS,
        )
    except FileNotFoundError:
        return False, "校时失败：找不到 w32tm 命令"
    except subprocess.TimeoutExpired:
        return False, f"校时失败：命令超时（{_SYNC_TIMEOUT_SECONDS:g} 秒）"
    except OSError as error:
        return False, f"校时失败：{error}"
    if completed.returncode != 0:
        return False, f"校时失败：{_failure_reason(completed)}"
    return True, "已执行 w32tm /resync"


def _measure_offset(runner: Runner) -> float | None:
    """测一次墙钟与时间源的偏移；校时失败也要测，命令失败或不可解析返回 None。"""
    try:
        completed = runner(
            list(OFFSET_COMMAND),
            capture_output=True,
            check=False,
            timeout=_SYNC_TIMEOUT_SECONDS,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if completed.returncode != 0:
        return None
    return parse_offset_seconds(_decode(completed.stdout))


def _failure_reason(completed: subprocess.CompletedProcess[bytes]) -> str:
    """失败原因：stderr 与 stdout 的首个非空行优先，空输出落到退出码。"""
    text = _decode(completed.stderr).strip() or _decode(completed.stdout).strip()
    first_line = text.splitlines()[0] if text else ""
    if not first_line:
        return f"w32tm 退出码 {completed.returncode}"
    return f"w32tm 退出码 {completed.returncode}：{first_line[:200]}"


def _decode(output: bytes) -> str:
    """按 UTF-8 优先解码命令输出，失败退回系统区域编码（同 tasks 失败原因口径）。"""
    for encoding in ("utf-8", locale.getencoding()):
        try:
            return output.decode(encoding)
        except UnicodeDecodeError:
            continue
    return output.decode("utf-8", errors="replace")


def _offset_text(check: ClockCheck) -> str:
    """偏移的人话说明；测不出就说测不出，不猜。"""
    if check.offset_seconds is None:
        return "墙钟偏移不可测"
    return f"墙钟偏移 {check.offset_seconds:.1f} 秒"


def _set_catch_up_paused(connection: sqlite3.Connection, paused: bool, *, at: datetime) -> None:
    """写补跑暂停标记；事务边界在这里收口，事件落库由调用方负责。"""
    with transaction(connection):
        put_settings(connection, {CATCH_UP_PAUSED_KEY: paused}, at=at)


__all__ = [
    "CATCH_UP_PAUSED_KEY",
    "OFFSET_COMMAND",
    "SKEW_EVENT",
    "SYNC_COMMAND",
    "TIMESYNC_EVENT",
    "ClockCheck",
    "Runner",
    "apply_startup_check",
    "catch_up_paused",
    "parse_offset_seconds",
    "startup_check",
]
