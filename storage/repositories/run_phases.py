"""阶段仓储：run_phases 的起止与状态。"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from contracts import PhaseState
from storage.repositories.iso_time import format_at, parse_optional_at
from storage.repositories.json_columns import dump_json, load_json_object
from storage.repositories.sql import optional_str

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
    detail: Mapping[str, object]


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
                dump_json(phase.detail),
            ),
        )

    def get(self, phase_id: str) -> RunPhaseRow | None:
        """按 id 取阶段。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM run_phases WHERE id = ?",
            (phase_id,),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    def finish(
        self,
        phase_id: str,
        *,
        state: PhaseState,
        at: datetime,
        detail: Mapping[str, object],
    ) -> None:
        """写入阶段终态与结束时间。"""
        self._connection.execute(
            "UPDATE run_phases SET state = ?, finished_at = ?, detail = ? WHERE id = ?",
            (
                state.value,
                format_at(at),
                dump_json(detail),
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
        return tuple(_row_to_record(row) for row in rows)


def _row_to_record(row: sqlite3.Row) -> RunPhaseRow:
    return RunPhaseRow(
        id=str(row["id"]),
        run_id=str(row["run_id"]),
        phase=str(row["phase"]),
        attempt=int(row["attempt"]),
        state=PhaseState(str(row["state"])),
        started_at=parse_optional_at(optional_str(row["started_at"])),
        finished_at=parse_optional_at(optional_str(row["finished_at"])),
        timeout_seconds=int(row["timeout_seconds"]),
        detail=load_json_object(row["detail"], "阶段 detail"),
    )


__all__ = ["RunPhaseRepository", "RunPhaseRow"]
