"""MAA 脚本能力（docs/plugins/maa.md 脚本节，ScriptAdapter 协议见 docs/05）。

GUI 模式：``launch`` 启动 ``MAA.exe``（工作目录＝MAA 目录、剔除代理环境变量），记录
pid 与两份日志的起始字节偏移；``stop`` 先发 ``WM_CLOSE``，宽限期内不退出才结束整棵
进程树。``observe`` 按偏移增量读取 ``gui.log`` 与 ``asst.log`` 的新增行，解析信号并
推导运行状态。契约的 ScriptState 没有「失败」一值：整轮结束（完成、错误汇总、掉线、
停止）都映射为 ``EXITED``，失败事实由 ``task.error`` 与 ``run.failed`` 信号携带，交
验证器判定。单任务出错不改状态——MAA 会继续执行后续任务，整轮以错误汇总行收尾。
``binding`` 模式的驱动与 ``recover`` 的动作分解不在本能力内。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
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
    KIND_RUN_COMPLETED,
    KIND_RUN_DISCONNECTED,
    KIND_RUN_FAILED,
    LOG_DIR_NAME,
    ParsedSignal,
    file_size,
    parse_asst_line,
    parse_gui_line,
    read_new_lines,
)
from plugins.maa.process_host import ProcessHost, WindowsProcessHost

# 默认值真源是 config.schema.json（内核保存/加载时按 schema 补齐）；此常量
# 供句柄 meta 缺失时兜底，契约测试锁定它与 schema 默认值一致。
DEFAULT_MAA_DIR = "H:/MAA"
DEFAULT_LOG_DIR = Path(DEFAULT_MAA_DIR) / LOG_DIR_NAME
MAA_EXE_NAME = "MAA.exe"
_GRACEFUL_STOP_SECONDS = 10.0
_FORCED_STOP_SECONDS = 5.0
_STOP_POLL_SECONDS = 0.2
_RECOVERY_UNAVAILABLE = "恢复动作不可用：未执行真机操作"
_EPOCH = datetime.min.replace(tzinfo=UTC)


class _RunStatus(StrEnum):
    """运行内部状态；终态映射 ScriptState.EXITED，失败细节由信号携带。"""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DISCONNECTED = "disconnected"
    STOPPED = "stopped"


_TERMINAL_STATUSES = frozenset(
    {
        _RunStatus.COMPLETED,
        _RunStatus.FAILED,
        _RunStatus.DISCONNECTED,
        _RunStatus.STOPPED,
    }
)
_STATUS_BY_SIGNAL = {
    KIND_RUN_COMPLETED: _RunStatus.COMPLETED,
    KIND_RUN_FAILED: _RunStatus.FAILED,
    KIND_RUN_DISCONNECTED: _RunStatus.DISCONNECTED,
}


class MaaScript:
    """script 能力：启动与停止 MAA 进程，以日志偏移驱动观测。"""

    def __init__(
        self,
        host: ProcessHost | None = None,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        self._host = host
        self._wait = wait
        self._runs: dict[str, _RunState] = {}

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        """启动 MAA 进程并记录起始偏移；已有同类进程时不接管，直接失败。"""
        maa_dir = _maa_dir(ctx.config)
        host = self._host_for()
        existing = host.running_pids(MAA_EXE_NAME)
        if existing:
            raise RuntimeError(f"MAA 已在运行（PID {existing[0]}），同一环境只允许一个驱动者")
        log_dir = maa_dir / LOG_DIR_NAME
        pid = host.start(maa_dir / MAA_EXE_NAME, maa_dir)
        return ScriptHandle(
            id=f"maa-{ctx.run_id}",
            pid=pid,
            meta={
                "launched": True,
                "pid": pid,
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
        for name, drained in ((GUI_LOG_NAME, gui), (ASST_LOG_NAME, asst)):
            if drained.truncated:
                ctx.log.info({"event": "maa.log.rolled", "file": name})
        if gui.unknown or asst.unknown:
            run.unknown_lines += gui.unknown + asst.unknown
            ctx.log.info({"event": "maa.log.unknown", "count": run.unknown_lines})
        signals = _merge_by_time(gui.signals, asst.signals)
        for signal in signals:
            run.advance(signal, ctx.clock.utc_now())
        return ScriptObservation(
            state=ScriptState.EXITED if run.status in _TERMINAL_STATUSES else ScriptState.RUNNING,
            signals=tuple(signals),
            last_signal=run.last_signal,
            since=run.since,
        )

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult:
        """按模式停止脚本：graceful 先请 MAA 自己退出，force 直接结束进程树。"""
        run = self._runs.setdefault(handle.id, _RunState.from_handle(handle))
        if run.status not in _TERMINAL_STATUSES:
            run.status = _RunStatus.STOPPED
            run.since = ctx.clock.utc_now()
        pid = handle.pid if handle.pid is not None else _meta_pid(handle)
        if pid is None:
            return StopResult(ok=True, detail="句柄没有进程，视作已停止")
        host = self._host_for()
        if not host.is_running(pid):
            return StopResult(ok=True, detail=f"MAA 进程已退出（PID {pid}）")
        if (
            mode is StopMode.GRACEFUL
            and host.request_stop(pid)
            and self._wait_exit(host, pid, _GRACEFUL_STOP_SECONDS, ctx)
        ):
            return StopResult(ok=True, detail=f"MAA 已自行退出（PID {pid}）")
        host.terminate(pid)
        if not self._wait_exit(host, pid, _FORCED_STOP_SECONDS, ctx):
            return StopResult(
                ok=False,
                error_code="E_SCRIPT",
                detail=f"结束 MAA 进程树后仍存活（PID {pid}）",
            )
        return StopResult(ok=True, detail=f"MAA 进程树已结束（PID {pid}）")

    def _host_for(self) -> ProcessHost:
        """真机端口按需构造：测试注入假件，生产用 Windows 实现。"""
        return self._host if self._host is not None else WindowsProcessHost()

    def _wait_exit(self, host: ProcessHost, pid: int, seconds: float, ctx: RunContext) -> bool:
        """按注入时钟轮询存活；等待走注入的 wait，测试不真等。"""
        deadline = ctx.clock.monotonic() + seconds
        while ctx.clock.monotonic() < deadline:
            if not host.is_running(pid):
                return True
            self._wait(_STOP_POLL_SECONDS)
        return not host.is_running(pid)

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
    status: _RunStatus = _RunStatus.RUNNING
    unknown_lines: int = 0
    last_signal: Signal | None = None

    def advance(self, signal: Signal, now: datetime) -> None:
        """按信号推进状态；终态只生效一次，最近信号始终更新。"""
        self.last_signal = signal
        target = _STATUS_BY_SIGNAL.get(signal.kind)
        if target is not None and self.status not in _TERMINAL_STATUSES:
            self.status = target
            self.since = now

    @classmethod
    def from_handle(cls, handle: ScriptHandle) -> _RunState:
        """从句柄 meta 恢复游标；句柄是偏移与目录的唯一载体。"""
        meta = handle.meta
        log_dir = Path(str(meta.get("log_dir", str(DEFAULT_LOG_DIR))))
        return cls(
            gui_path=log_dir / GUI_LOG_NAME,
            asst_path=log_dir / ASST_LOG_NAME,
            gui_offset=_offset(meta.get("gui_log_offset")),
            asst_offset=_offset(meta.get("asst_log_offset")),
            since=_aware_or_none(meta.get("launched_at")),
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
            if text.strip():
                unknown += 1
            continue
        # 原始行是验证器的证据材料；契约 Signal 没有独立字段，随载荷过境。
        signals.append(
            Signal(
                kind=hit.kind,
                payload={**hit.payload, "line": hit.line},
                at=hit.at or ctx.clock.utc_now(),
            )
        )
    return _DrainResult(signals, chunk.offset, unknown, chunk.truncated)


def _merge_by_time(*batches: list[Signal]) -> list[Signal]:
    """按信号时间归并两份日志；两表同刻时 gui 行在前（稳定排序）。"""
    return sorted(
        (signal for batch in batches for signal in batch),
        key=lambda signal: signal.at or _EPOCH,
    )


def _aware_or_none(value: object) -> datetime | None:
    """解析句柄里的 ISO 时间；naive 值按宿主本地时区补全，非法返回 None。"""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.astimezone()


def _maa_dir(config: Mapping[str, Any]) -> Path:
    """插件配置的 maa_dir 指向 MAA 根目录。"""
    return Path(str(config.get("maa_dir", DEFAULT_MAA_DIR)))


def _meta_pid(handle: ScriptHandle) -> int | None:
    """句柄顶层没有 pid 时回落到 meta.pid；非法值按没有进程处理。"""
    value = handle.meta.get("pid")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _offset(value: object) -> int:
    """句柄 meta 里的偏移收窄为非负整数；缺失或非法按 0 处理。"""
    return value if isinstance(value, int) and not isinstance(value, bool) else 0
