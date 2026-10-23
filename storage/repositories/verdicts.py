"""结论仓储：Verdict 落库与逐项检查。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from contracts import VerdictStatus
from storage.repositories.iso_time import format_at, parse_at

_COLUMNS = "id, run_id, status, confidence, reason, policy_id, checks, evidence_ids, created_at"


@dataclass(frozen=True, slots=True)
class VerdictRow:
    """verdicts 的一行。"""

    id: str
    run_id: str
    status: VerdictStatus
    confidence: float
    reason: str
    policy_id: str
    checks: tuple[dict[str, object], ...]
    evidence_ids: tuple[str, ...]
    created_at: datetime


class VerdictRepository:
    """verdicts 表的读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def insert(self, verdict: VerdictRow) -> None:
        """写入一条结论。"""
        self._connection.execute(
            "INSERT INTO verdicts (id, run_id, status, confidence, reason, policy_id, "
            "checks, evidence_ids, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                verdict.id,
                verdict.run_id,
                verdict.status.value,
                verdict.confidence,
                verdict.reason,
                verdict.policy_id,
                json.dumps(list(verdict.checks), ensure_ascii=False, sort_keys=True),
                json.dumps(list(verdict.evidence_ids), ensure_ascii=False),
                format_at(verdict.created_at),
            ),
        )

    def get(self, verdict_id: str) -> VerdictRow | None:
        """按 id 取结论。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM verdicts WHERE id = ?",
            (verdict_id,),
        ).fetchone()
        return None if row is None else _row_to_row(row)

    def for_run(self, run_id: str) -> tuple[VerdictRow, ...]:
        """某个运行的全部结论，按创建时间排序。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM verdicts WHERE run_id = ? ORDER BY created_at",
            (run_id,),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)


def _row_to_row(row: sqlite3.Row) -> VerdictRow:
    return VerdictRow(
        id=str(row["id"]),
        run_id=str(row["run_id"]),
        status=VerdictStatus(str(row["status"])),
        confidence=float(row["confidence"]),
        reason=str(row["reason"]),
        policy_id=str(row["policy_id"]),
        checks=_load_checks(row["checks"]),
        evidence_ids=tuple(str(item) for item in json.loads(str(row["evidence_ids"]))),
        created_at=parse_at(str(row["created_at"])),
    )


def _load_checks(value: object) -> tuple[dict[str, object], ...]:
    items = json.loads(str(value))
    if not isinstance(items, list):
        raise sqlite3.DatabaseError("结论 checks 不是 JSON 数组")
    return tuple(item for item in items if isinstance(item, dict))


__all__ = ["VerdictRepository", "VerdictRow"]
