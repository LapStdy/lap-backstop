"""阶段跟踪：起止时间、阶段事件与 run_phases 记录。"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from uuid import uuid4

from contracts import Clock, EventType, PhaseName, PhaseState
from storage.db.transaction import transaction
from storage.repositories.events import EventLog
from storage.repositories.run_phases import RunPhaseRepository, RunPhaseRow

PHASES: tuple[PhaseName, ...] = tuple(PhaseName)


class PhaseTracker:
    """给一次运行记录阶段开始/结束；事件与行同步写入。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock) -> None:
        self._connection = connection
        self._clock = clock
        self._phases = RunPhaseRepository(connection)
        self._events = EventLog(connection)

    def start(
        self,
        run_id: str,
        phase: PhaseName,
        *,
        attempt: int,
        timeout_seconds: int = 0,
    ) -> str:
        """记录阶段开始，返回阶段 id。"""
        if phase not in PHASES:
            raise ValueError(f"未知阶段：{phase!r}")
        now = self._clock.utc_now()
        phase_id = uuid4().hex
        with transaction(self._connection):
            self._phases.insert(
                RunPhaseRow(
                    id=phase_id,
                    run_id=run_id,
                    phase=phase.value,
                    attempt=attempt,
                    state=PhaseState.RUNNING,
                    started_at=now,
                    finished_at=None,
                    timeout_seconds=timeout_seconds,
                    detail={},
                )
            )
            self._events.append_run(
                run_id,
                EventType.RUN_PHASE_STARTED,
                {"run_id": run_id, "phase": phase, "attempt": attempt},
                at=now,
            )
        return phase_id

    def finish(
        self,
        phase_id: str,
        *,
        state: PhaseState,
        detail: str = "",
    ) -> None:
        """记录阶段结束并写耗时。"""
        phase = self._phases.get(phase_id)
        if phase is None:
            raise ValueError(f"阶段不存在：{phase_id}")
        now = self._clock.utc_now()
        duration_ms = _duration_ms(phase.started_at, now)
        with transaction(self._connection):
            self._phases.finish(
                phase_id,
                state=state,
                at=now,
                detail={"detail": detail},
            )
            self._events.append_run(
                phase.run_id,
                EventType.RUN_PHASE_FINISHED,
                {
                    "run_id": phase.run_id,
                    "phase": phase.phase,
                    "status": state.value,
                    "duration_ms": duration_ms,
                },
                at=now,
            )

    def for_run(self, run_id: str) -> tuple[RunPhaseRow, ...]:
        """某个运行的全部阶段。"""
        return self._phases.for_run(run_id)


def _duration_ms(started_at: datetime | None, now: datetime) -> int:
    if started_at is None:
        return 0
    return int((now - started_at).total_seconds() * 1000)


__all__ = ["PHASES", "PhaseTracker"]
