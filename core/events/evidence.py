"""证据服务：插件把文件放进证据目录，内核算哈希、分配 id 并落库。"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from uuid import uuid4

from contracts import Clock, EventType, EvidenceKind, EvidenceSource
from storage.db.transaction import transaction
from storage.repositories.events import EventLog
from storage.repositories.evidence import EvidenceRepository, EvidenceRow


class EvidenceService:
    """证据文件的唯一入口；路径必须落在证据根目录内。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        evidence_root: Path,
        clock: Clock,
    ) -> None:
        self._connection = connection
        self._root = evidence_root.resolve()
        self._clock = clock
        self._evidence = EvidenceRepository(connection)
        self._events = EventLog(connection)

    def record(
        self,
        run_id: str,
        *,
        kind: EvidenceKind,
        path: Path,
        summary: str,
        source: EvidenceSource = EvidenceSource.PLUGIN,
    ) -> str:
        """登记一份证据，返回内核分配的 id。

        Raises:
            ValueError: 路径逃出证据根目录。
            FileNotFoundError: 证据文件不存在。
        """
        absolute = self._resolve(path)
        relative = absolute.relative_to(self._root)
        digest = hashlib.sha256(absolute.read_bytes()).hexdigest()
        evidence_id = uuid4().hex
        now = self._clock.utc_now()
        with transaction(self._connection):
            self._evidence.insert(
                EvidenceRow(
                    id=evidence_id,
                    run_id=run_id,
                    kind=kind,
                    source=source,
                    path=relative.as_posix(),
                    sha256=digest,
                    summary=summary,
                    created_at=now,
                )
            )
            self._events.append_run(
                run_id,
                EventType.EVIDENCE,
                {
                    "run_id": run_id,
                    "kind": kind.value,
                    "path": relative.as_posix(),
                    "sha256": digest,
                },
                at=now,
            )
        return evidence_id

    def for_run(self, run_id: str) -> tuple[EvidenceRow, ...]:
        """某个运行的全部证据。"""
        return self._evidence.for_run(run_id)

    def known_ids(self, run_id: str) -> frozenset[str]:
        """某个运行已登记的证据 id。"""
        return self._evidence.known_ids(run_id)

    def _resolve(self, path: Path) -> Path:
        candidate = path if path.is_absolute() else self._root / path
        resolved = candidate.resolve()
        if not resolved.is_relative_to(self._root):
            raise ValueError(f"证据路径逃出证据根目录：{path}")
        return resolved


__all__ = ["EvidenceService"]
