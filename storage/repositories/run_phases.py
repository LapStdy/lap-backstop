"""阶段仓储：run_phases 的起止与状态。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from contracts import PhaseState
from storage.repositories.iso_time import format_at, parse_optional_at

_COLUMNS = "id, run_id, phase, attempt, state, started_at, finished_at, timeout_seconds, detail"


@dataclass(frozen=True, slots=True)
class RunPhaseRow:
    """run_phases 的一行。"""

    id: str
    run_id: str
    phase: str
    attempt: int
    state: PhaseState
    started_at: datetime | None
    finished_at: datetime | None
    timeout_seconds: int
    detail: Mapping[str, Any]


class RunPhaseRepository:
    """run_phases 表的读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def insert(self, phase: RunPhaseRow) -> None:
        """写入一条阶段记录。"""
        self._connection.execute(
            "INSERT INTO run_phases (id, run_id, phase, attempt, state, started_at, "
            "finished_at, timeout_seconds, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                phase.id,
                phase.run_id,
                phase.phase,
                phase.attempt,
                phase.state.value,
                None if phase.started_at is None else format_at(phase.started_at),
                None if phase.finished_at is None else format_at(phase.finished_at),
                phase.timeout_seconds,
                json.dumps(dict(phase.detail), ensure_ascii=False, sort_keys=True),
            ),
        )

    def get(self, phase_id: str) -> RunPhaseRow | None:
        """按 id 取阶段。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM run_phases WHERE id = ?",
            (phase_id,),
        ).fetchone()
        return None if row is None else _row_to_row(row)

    def finish(
        self,
        phase_id: str,
        *,
        state: PhaseState,
        at: datetime,
        detail: Mapping[str, Any],
    ) -> None:
        """写入阶段终态与结束时间。"""
        self._connection.execute(
            "UPDATE run_phases SET state = ?, finished_at = ?, detail = ? WHERE id = ?",
            (
                state.value,
                format_at(at),
                json.dumps(dict(detail), ensure_ascii=False, sort_keys=True),
                phase_id,
            ),
        )

    def for_run(self, run_id: str) -> tuple[RunPhaseRow, ...]:
        """某个运行的全部阶段，按开始时间排序。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM run_phases WHERE run_id = ? "
            "ORDER BY started_at IS NULL, started_at, phase",
            (run_id,),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)


def _row_to_row(row: sqlite3.Row) -> RunPhaseRow:
    return RunPhaseRow(
        id=str(row["id"]),
        run_id=str(row["run_id"]),
        phase=str(row["phase"]),
        attempt=int(row["attempt"]),
        state=PhaseState(str(row["state"])),
        started_at=parse_optional_at(_optional_str(row["started_at"])),
        finished_at=parse_optional_at(_optional_str(row["finished_at"])),
        timeout_seconds=int(row["timeout_seconds"]),
        detail=_load_detail(row["detail"]),
    )


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _load_detail(value: object) -> Mapping[str, Any]:
    detail = json.loads(str(value))
    if not isinstance(detail, dict):
        raise sqlite3.DatabaseError("阶段 detail 不是 JSON 对象")
    return detail


__all__ = ["RunPhaseRepository", "RunPhaseRow"]
