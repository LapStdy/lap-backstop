"""事件回放：按 seq 连续重放运行事件流，重建运行状态快照。

崩溃恢复与外部回放共用这一份重建逻辑；事件流 append-only，回放只读不写。
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Final

from contracts import EventType, RunState
from core.jobs.state_machine import can_transition
from storage.repositories.events import StoredEvent


class ReplayError(Exception):
    """事件流无法按序回放。"""


class EmptyEventStreamError(ReplayError):
    """事件流为空，没有可回放的事件。"""


class EventStreamGapError(ReplayError):
    """seq 跳号，事件流缺头或缺段。"""


class DuplicateEventError(ReplayError):
    """同一 seq 出现两次。"""


class OutOfOrderEventError(ReplayError):
    """seq 回退，事件流乱序。"""


class EventStreamConflictError(ReplayError):
    """事件内容与回放出的状态矛盾。"""


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    """按事件流重建出的运行状态快照；seq 从 1 连续，事件数与 last_seq 相等。"""

    run_id: str
    state: RunState
    attempt: int
    phases: Mapping[str, str]
    verdict_status: str | None
    last_seq: int
    finished: bool


def replay_run(events: Iterable[StoredEvent]) -> RunSnapshot:
    """按 seq 从 1 起连续回放运行事件流，重建状态快照。

    Args:
        events: 事件序列；seq 必须从 1 开始逐条加一。

    Returns:
        重建出的运行状态快照。

    Raises:
        EmptyEventStreamError: 序列为空。
        EventStreamGapError: seq 跳号（缺头或缺段）。
        DuplicateEventError: 相邻两条 seq 相同。
        OutOfOrderEventError: seq 回退。
        EventStreamConflictError: 流首不是 run.created、状态事件与回放状态矛盾或混入别的运行。
    """
    builder = _SnapshotBuilder()
    seen = 0
    for event in events:
        seen += 1
        _require_seq(event.seq, seen)
        builder.apply(event)
    if seen == 0:
        raise EmptyEventStreamError("事件流为空")
    return builder.snapshot()


def _require_seq(seq: int, position: int) -> None:
    """append-only 的写入者保证 seq 从 1 连续递增；偏离即流已损坏。"""
    if seq == position:
        return
    if seq == position - 1:
        raise DuplicateEventError(f"seq {seq} 出现两次，位置 {position}")
    if seq > position:
        raise EventStreamGapError(f"位置 {position} 期望 seq {position}，实际 {seq}，事件流缺段")
    raise OutOfOrderEventError(f"位置 {position} 的 seq {seq} 小于期望 {position}")


class _SnapshotBuilder:
    """逐事件累积状态；被拒迁移在写侧只留痕，回放侧同样不改状态。"""

    def __init__(self) -> None:
        self._run_id: str | None = None
        self._state: RunState | None = None
        self._attempt = 0
        self._phases: dict[str, str] = {}
        self._verdict: str | None = None
        self._last_seq = 0
        self._finished = False

    def apply(self, event: StoredEvent) -> None:
        if self._run_id is None:
            if event.type != EventType.RUN_CREATED.value or event.run_id is None:
                raise EventStreamConflictError(f"事件流必须以 run.created 开头，实际 {event.type}")
            self._run_id = event.run_id
        elif event.run_id != self._run_id:
            raise EventStreamConflictError(
                f"事件属于运行 {event.run_id}，混入运行 {self._run_id} 的流"
            )
        handler = _HANDLERS.get(event.type)
        if handler is not None:
            handler(self, event.payload)
        self._last_seq = event.seq

    def snapshot(self) -> RunSnapshot:
        if self._run_id is None or self._state is None:
            # 空流在 replay_run 已拦下；这里是纯函数对外的自保边界
            raise EmptyEventStreamError("快照没有起始事件")
        return RunSnapshot(
            run_id=self._run_id,
            state=self._state,
            attempt=self._attempt,
            phases=dict(self._phases),
            verdict_status=self._verdict,
            last_seq=self._last_seq,
            finished=self._finished,
        )

    def _on_created(self, payload: Mapping[str, Any]) -> None:
        self._state = RunState.SCHEDULED

    def _on_state_changed(self, payload: Mapping[str, Any]) -> None:
        if self._state is None:
            raise EventStreamConflictError("run.state.changed 出现在 run.created 之前")
        source = _parse_run_state(payload.get("from_state"))
        target = _parse_run_state(payload.get("to_state"))
        if source is not self._state:
            raise EventStreamConflictError(
                f"事件声称从 {source.value} 迁出，回放状态是 {self._state.value}"
            )
        if target is not self._state and can_transition(self._state, target):
            self._state = target

    def _on_recovery_started(self, payload: Mapping[str, Any]) -> None:
        self._attempt = max(self._attempt, _payload_int(payload, "attempt"))

    def _on_phase_started(self, payload: Mapping[str, Any]) -> None:
        self._phases[str(payload.get("phase", ""))] = "running"
        self._attempt = max(self._attempt, _payload_int(payload, "attempt"))

    def _on_phase_finished(self, payload: Mapping[str, Any]) -> None:
        self._phases[str(payload.get("phase", ""))] = str(payload.get("status", ""))

    def _on_verdict(self, payload: Mapping[str, Any]) -> None:
        self._verdict = str(payload.get("status", ""))

    def _on_finished(self, payload: Mapping[str, Any]) -> None:
        self._finished = True


_HANDLERS: Final[Mapping[str, Callable[[_SnapshotBuilder, Mapping[str, Any]], None]]] = {
    EventType.RUN_CREATED.value: _SnapshotBuilder._on_created,
    EventType.RUN_STATE_CHANGED.value: _SnapshotBuilder._on_state_changed,
    EventType.RECOVERY_STARTED.value: _SnapshotBuilder._on_recovery_started,
    EventType.RUN_PHASE_STARTED.value: _SnapshotBuilder._on_phase_started,
    EventType.RUN_PHASE_FINISHED.value: _SnapshotBuilder._on_phase_finished,
    EventType.RUN_VERDICT.value: _SnapshotBuilder._on_verdict,
    EventType.RUN_FINISHED.value: _SnapshotBuilder._on_finished,
}


def _parse_run_state(value: object) -> RunState:
    try:
        return RunState(str(value))
    except ValueError:
        raise EventStreamConflictError(f"未知运行状态：{value!r}") from None


def _payload_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, int):
        raise EventStreamConflictError(f"载荷 {key} 不是整数：{value!r}")
    return value


__all__ = [
    "DuplicateEventError",
    "EmptyEventStreamError",
    "EventStreamConflictError",
    "EventStreamGapError",
    "OutOfOrderEventError",
    "ReplayError",
    "RunSnapshot",
    "replay_run",
]
