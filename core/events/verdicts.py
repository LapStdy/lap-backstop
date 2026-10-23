"""结论服务：校验证据引用、落库 Verdict 并回填到运行。"""

from __future__ import annotations

import sqlite3
from uuid import uuid4

from contracts import Clock, EventType, Verdict, VerdictStatus
from storage.db.transaction import transaction
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRepository
from storage.repositories.verdicts import VerdictRepository, VerdictRow


class MissingEvidenceError(ValueError):
    """结论引用了不存在的证据，对应错误码 E_VERIFY_NO_EVIDENCE。"""

    code = "E_VERIFY_NO_EVIDENCE"


class VerdictService:
    """结论落库；内核不信任插件给的证据引用，先与证据表核对。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock) -> None:
        self._connection = connection
        self._clock = clock
        self._verdicts = VerdictRepository(connection)
        self._runs = JobRunRepository(connection)
        self._events = EventLog(connection)

    def record(self, run_id: str, verdict: Verdict) -> str:
        """写入结论并返回结论 id。

        Raises:
            MissingEvidenceError: 没有证据，或引用了不属于该运行的证据。
        """
        if not verdict.evidence_ids:
            raise MissingEvidenceError("结论缺少证据")
        known = _known_evidence(self._connection, run_id)
        unknown = set(verdict.evidence_ids) - known
        if unknown:
            raise MissingEvidenceError(f"结论引用了不存在的证据：{sorted(unknown)}")
        verdict_id = uuid4().hex
        now = self._clock.utc_now()
        with transaction(self._connection):
            self._verdicts.insert(
                VerdictRow(
                    id=verdict_id,
                    run_id=run_id,
                    status=verdict.status,
                    confidence=verdict.confidence,
                    reason=verdict.reason,
                    policy_id=verdict.policy_id,
                    checks=tuple(
                        {
                            "name": check.name,
                            "status": check.status.value,
                            "detail": check.detail,
                            "evidence_ids": list(check.evidence_ids),
                        }
                        for check in verdict.checks
                    ),
                    evidence_ids=verdict.evidence_ids,
                    created_at=now,
                )
            )
            self._runs.set_verdict(run_id, verdict_id)
            self._events.append_run(
                run_id,
                EventType.RUN_VERDICT,
                {
                    "run_id": run_id,
                    "status": verdict.status.value,
                    "policy_id": verdict.policy_id,
                    "reason": verdict.reason,
                    "confidence": verdict.confidence,
                    "evidence_ids": list(verdict.evidence_ids),
                },
                at=now,
            )
        return verdict_id

    def status_of(self, verdict: Verdict) -> VerdictStatus:
        """结论状态原样返回；保留给调用方做映射。"""
        return verdict.status


def _known_evidence(connection: sqlite3.Connection, run_id: str) -> frozenset[str]:
    rows = connection.execute(
        "SELECT id FROM evidence WHERE run_id = ?",
        (run_id,),
    ).fetchall()
    return frozenset(str(row[0]) for row in rows)


__all__ = ["MissingEvidenceError", "VerdictService"]
