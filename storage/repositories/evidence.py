"""证据仓储：证据文件索引，文件本体落在证据根目录。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime

from contracts import EvidenceKind, EvidenceSource
from storage.repositories.iso_time import format_at, parse_at

_COLUMNS = "id, run_id, kind, source, path, sha256, summary, created_at"


@dataclass(frozen=True, slots=True)
class EvidenceRow:
    """evidence 的一行；``path`` 相对证据根目录。"""

    id: str
    run_id: str
    kind: EvidenceKind
    source: EvidenceSource
    path: str
    sha256: str
    summary: str
    created_at: datetime


class EvidenceRepository:
    """evidence 表的读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def insert(self, evidence: EvidenceRow) -> None:
        """写入一条证据索引。"""
        self._connection.execute(
            "INSERT INTO evidence (id, run_id, kind, source, path, sha256, summary, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                evidence.id,
                evidence.run_id,
                evidence.kind.value,
                evidence.source.value,
                evidence.path,
                evidence.sha256,
                evidence.summary,
                format_at(evidence.created_at),
            ),
        )

    def get(self, evidence_id: str) -> EvidenceRow | None:
        """按 id 取证据。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM evidence WHERE id = ?",
            (evidence_id,),
        ).fetchone()
        return None if row is None else _row_to_row(row)

    def for_run(self, run_id: str) -> tuple[EvidenceRow, ...]:
        """某个运行的全部证据，按创建时间排序。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM evidence WHERE run_id = ? ORDER BY created_at",
            (run_id,),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def known_ids(self, run_id: str) -> frozenset[str]:
        """某个运行已落库的证据 id 集合。"""
        rows = self._connection.execute(
            "SELECT id FROM evidence WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        return frozenset(str(row[0]) for row in rows)


def _row_to_row(row: sqlite3.Row) -> EvidenceRow:
    return EvidenceRow(
        id=str(row["id"]),
        run_id=str(row["run_id"]),
        kind=EvidenceKind(str(row["kind"])),
        source=EvidenceSource(str(row["source"])),
        path=str(row["path"]),
        sha256=str(row["sha256"]),
        summary=str(row["summary"]),
        created_at=parse_at(str(row["created_at"])),
    )


__all__ = ["EvidenceRepository", "EvidenceRow"]
