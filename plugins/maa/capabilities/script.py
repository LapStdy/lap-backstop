"""MAA 脚本能力（docs/plugins/maa.md 脚本节，ScriptAdapter 协议见 docs/05）。

GUI 模式的观测层：``launch`` 不启动进程，只记录日志目录与两份日志的起始
字节偏移；``observe`` 按偏移增量读取 ``gui.log`` 与 ``asst.log`` 的新增
行，解析信号并推导运行状态。契约的 ScriptState 没有「失败」一值：整轮
结束（完成、错误汇总、掉线、停止）都映射为 ``EXITED``，失败事实由
``task.error`` 与 ``run.failed`` 信号携带，交验证器判定。单任务出错不改
状态——MAA 会继续执行后续任务，整轮以错误汇总行收尾。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from contracts import (
    RecoveryAction,
    RecoveryResult,
    RunContext,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    Signal,
    StopMode,
    StopResult,
)
from plugins.maa.logparse import (
    ASST_LOG_NAME,
    GUI_LOG_NAME,
    LOG_DIR_NAME,
    ParsedSignal,
    file_size,
    parse_asst_line,
    parse_gui_line,
    read_new_lines,
)

DEFAULT_MAA_DIR = "H:/MAA"
DEFAULT_LOG_DIR = Path(DEFAULT_MAA_DIR) / LOG_DIR_NAME
_RECOVERY_UNAVAILABLE = "恢复动作不可用：未执行真机操作"
_EPOCH = datetime.min.replace(tzinfo=UTC)

# 内部运行状态；终态集合映射为 ScriptState.EXITED，首个终态生效。
_TERMINAL_STATUSES = frozenset({"completed", "failed", "disconnected", "stopped"})
_STATUS_BY_SIGNAL = {
    "run.completed": "completed",
    "run.failed": "failed",
    "run.disconnected": "disconnected",
}


class MaaScript:
    """script 能力：以日志偏移驱动观测，不启动 MAA 进程。"""

    def __init__(self) -> None:
        self._runs: dict[str, _RunState] = {}

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        """记录日志目录与起始偏移；启动前的既有内容不计入本次运行。"""
        log_dir = _log_dir(ctx.config)
        return ScriptHandle(
            id=f"maa-{ctx.run_id}",
            meta={
                "launched": False,
                "log_dir": str(log_dir),
                "gui_log_offset": file_size(log_dir / GUI_LOG_NAME),
                "asst_log_offset": file_size(log_dir / ASST_LOG_NAME),
                "launched_at": ctx.clock.utc_now().isoformat(),
            },
        )

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        """增量读取两份日志，返回新增信号与推导状态；只读，可重复调用。"""
        run = self._runs.setdefault(handle.id, _RunState.from_handle(handle))
        gui = _drain(run.gui_path, parse_gui_line, run.gui_offset, ctx)
        asst = _drain(run.asst_path, parse_asst_line, run.asst_offset, ctx)
        run.gui_offset, run.asst_offset = gui.offset, asst.offset
        if gui.truncated:
            ctx.log.info({"event": "maa.log.rolled", "file": GUI_LOG_NAME})
        if asst.truncated:
            ctx.log.info({"event": "maa.log.rolled", "file": ASST_LOG_NAME})
        if gui.unknown or asst.unknown:
            run.unknown_lines += gui.unknown + asst.unknown
            ctx.log.info({"event": "maa.log.unknown", "count": run.unknown_lines})
        signals = _merge_by_time(gui.signals, asst.signals)
        for signal in signals:
            _advance(run, signal, ctx.clock.utc_now())
        return ScriptObservation(
            state=ScriptState.EXITED if run.status in _TERMINAL_STATUSES else ScriptState.RUNNING,
            signals=tuple(signals),
            last_signal=run.last_signal,
            since=run.since,
        )

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult:
        """无进程可停，返回成功；已终态的运行不重复改状态。"""
        run = self._runs.get(handle.id)
        if run is not None and run.status not in _TERMINAL_STATUSES:
            run.status = "stopped"
            run.since = ctx.clock.utc_now()
        return StopResult(ok=True, detail="无进程可停止")

    def recover(
        self,
        handle: ScriptHandle,
        ctx: RunContext,
        action: RecoveryAction,
    ) -> RecoveryResult:
        """返回恢复失败；不执行任何内核动作的分解步骤。"""
        return RecoveryResult(ok=False, error_code="E_PLUGIN", detail=_RECOVERY_UNAVAILABLE)


@dataclass
class _RunState:
    """一次运行的可变观测游标：日志偏移、内部状态与最近信号。"""

    gui_path: Path
    asst_path: Path
    gui_offset: int
    asst_offset: int
    since: datetime | None
    status: str = "running"
    unknown_lines: int = 0
    last_signal: Signal | None = None

    @classmethod
    def from_handle(cls, handle: ScriptHandle) -> _RunState:
        """从句柄 meta 恢复游标；句柄是偏移与目录的唯一载体。"""
        meta = handle.meta
        log_dir = Path(str(meta.get("log_dir", str(DEFAULT_LOG_DIR))))
        launched_at = meta.get("launched_at")
        return cls(
            gui_path=log_dir / GUI_LOG_NAME,
            asst_path=log_dir / ASST_LOG_NAME,
            gui_offset=_offset(meta.get("gui_log_offset")),
            asst_offset=_offset(meta.get("asst_log_offset")),
            since=datetime.fromisoformat(launched_at) if isinstance(launched_at, str) else None,
        )


@dataclass(frozen=True, slots=True)
class _DrainResult:
    """单份日志一轮增量读取的产物：契约信号、下一轮偏移与未知行数。"""

    signals: list[Signal]
    offset: int
    unknown: int
    truncated: bool


def _drain(
    path: Path,
    parse: Callable[[str], ParsedSignal | None],
    offset: int,
    ctx: RunContext,
) -> _DrainResult:
    """读新增行并转契约信号；缺时间戳的行补观察时刻。"""
    chunk = read_new_lines(path, offset)
    signals: list[Signal] = []
    unknown = 0
    for text in chunk.lines:
        hit = parse(text)
        if hit is None:
            unknown += 1 if text.strip() else 0
            continue
        signals.append(Signal(kind=hit.kind, payload=hit.payload, at=hit.at or ctx.clock.utc_now()))
    return _DrainResult(signals, chunk.offset, unknown, chunk.truncated)


def _merge_by_time(*batches: list[Signal]) -> list[Signal]:
    """按信号时间归并两份日志；两表同刻时 gui 行在前（稳定排序）。"""
    return sorted(
        (signal for batch in batches for signal in batch),
        key=lambda signal: signal.at or _EPOCH,
    )


def _advance(run: _RunState, signal: Signal, now: datetime) -> None:
    """按信号推进内部状态；终态只生效一次，最近信号始终更新。"""
    run.last_signal = signal
    target = _STATUS_BY_SIGNAL.get(signal.kind)
    if target is not None and run.status not in _TERMINAL_STATUSES:
        run.status = target
        run.since = now


def _log_dir(config: Mapping[str, Any]) -> Path:
    """插件配置的 maa_dir 指向 MAA 根目录，日志在 debug/ 子目录。"""
    maa_dir = config.get("maa_dir", DEFAULT_MAA_DIR)
    return Path(str(maa_dir)) / LOG_DIR_NAME


def _offset(value: object) -> int:
    """句柄 meta 里的偏移收窄为非负整数；缺失或非法按 0 处理。"""
    return value if isinstance(value, int) and not isinstance(value, bool) else 0
