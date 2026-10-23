"""证据与结论落库：哈希、路径约束、证据引用与模板要求校验。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    CheckResult,
    CheckStatus,
    EvidenceKind,
    Verdict,
    VerdictStatus,
)
from core.clock import VirtualClock
from core.events.evidence import EvidenceService
from core.events.verdicts import MissingEvidenceError, VerdictPolicyError, VerdictService
from core.jobs.run_service import JobRunService
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.verdicts import VerdictRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
POLICY: dict[str, Any] = {
    "id": "policy-1",
    "verifier_plugin_id": "fake",
    "required_evidence": ["log"],
    "params": {},
}


def insert_template(connection: sqlite3.Connection, policy: dict[str, Any] | None = None) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', 'env-1', '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            json.dumps(POLICY if policy is None else policy),
            "2026-10-23T00:00:00+00:00",
            "2026-10-23T00:00:00+00:00",
        ),
    )


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    insert_template(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    _record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key="run-key",
            priority=0,
            deadline_at=None,
        )
    )
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    try:
        yield connection, clock, runs, evidence_root
    finally:
        connection.close()


def test_evidence_is_hashed_and_recorded(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, evidence_root = env
    run_id = runs.non_terminal()[0].id
    target = evidence_root / "shot.log"
    target.write_text("hello", encoding="utf-8")
    service = EvidenceService(connection, evidence_root, clock)
    evidence_id = service.record(
        run_id,
        kind=EvidenceKind.LOG,
        path=Path("shot.log"),
        summary="测试证据",
    )
    stored = service.for_run(run_id)
    assert [item.id for item in stored] == [evidence_id]
    assert stored[0].sha256 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    row = connection.execute(
        "SELECT COUNT(*) FROM run_events WHERE type = 'evidence' AND run_id = ?",
        (run_id,),
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 1


def test_evidence_path_cannot_escape_root(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, evidence_root = env
    run_id = runs.non_terminal()[0].id
    outside = evidence_root.parent / "outside.log"
    outside.write_text("x", encoding="utf-8")
    service = EvidenceService(connection, evidence_root, clock)
    with pytest.raises(ValueError):
        service.record(
            run_id,
            kind=EvidenceKind.LOG,
            path=Path("../outside.log"),
            summary="逃逸",
        )


def test_verdict_without_evidence_is_rejected(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, _evidence_root = env
    run_id = runs.non_terminal()[0].id
    verdicts = VerdictService(connection, clock)
    with pytest.raises(MissingEvidenceError):
        verdicts.record(run_id, _verdict(()))


def test_verdict_with_unknown_evidence_is_rejected(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, evidence_root = env
    run_id = runs.non_terminal()[0].id
    evidence = EvidenceService(connection, evidence_root, clock)
    verdicts = VerdictService(connection, clock)
    with pytest.raises(MissingEvidenceError):
        verdicts.record(run_id, _verdict(("not-there",)))
    known = _record_log(evidence, evidence_root, run_id)
    with pytest.raises(MissingEvidenceError):
        verdicts.record(run_id, _verdict((known,), check_evidence_ids=("not-there",)))


def test_verdict_records_known_evidence_and_backfills_policy(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, evidence_root = env
    run_id = runs.non_terminal()[0].id
    evidence = EvidenceService(connection, evidence_root, clock)
    known = _record_log(evidence, evidence_root, run_id)
    verdicts = VerdictService(connection, clock)
    verdict_id, effective = verdicts.record(run_id, _verdict((known,)))
    stored = VerdictRepository(connection).for_run(run_id)
    assert [item.id for item in stored] == [verdict_id]
    assert stored[0].status is VerdictStatus.COMPLETED
    assert stored[0].policy_id == "policy-1"
    assert effective.policy_id == "policy-1"
    assert runs.get(run_id).verdict_id == verdict_id


def test_verdict_missing_required_kind_forced_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, evidence_root = env
    policy = {**POLICY, "required_evidence": ["log", "screenshot"]}
    connection.execute(
        "UPDATE job_templates SET verification_policy = ? WHERE id = ?",
        (json.dumps(policy), TEMPLATE_ID),
    )
    run_id = runs.non_terminal()[0].id
    evidence = EvidenceService(connection, evidence_root, clock)
    known = _record_log(evidence, evidence_root, run_id)
    verdicts = VerdictService(connection, clock)
    verdict_id, effective = verdicts.record(run_id, _verdict((known,)))
    stored = VerdictRepository(connection).get(verdict_id)
    assert stored is not None
    assert stored.status is VerdictStatus.UNKNOWN
    assert "screenshot" in stored.reason
    assert effective.status is VerdictStatus.UNKNOWN


def test_verdict_policy_without_required_evidence_is_rejected(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, _evidence_root = env
    connection.execute(
        "UPDATE job_templates SET verification_policy = ? WHERE id = ?",
        (json.dumps({"id": "policy-1", "verifier_plugin_id": "fake"}), TEMPLATE_ID),
    )
    run_id = runs.non_terminal()[0].id
    verdicts = VerdictService(connection, clock)
    with pytest.raises(VerdictPolicyError):
        verdicts.record(run_id, _verdict(("any",)))


def _verdict(
    evidence_ids: tuple[str, ...],
    *,
    check_evidence_ids: tuple[str, ...] | None = None,
) -> Verdict:
    checks_evidence = evidence_ids if check_evidence_ids is None else check_evidence_ids
    return Verdict(
        status=VerdictStatus.COMPLETED,
        policy_id="fake-policy",
        confidence=1.0,
        reason="任务完成",
        checks=(
            CheckResult(
                name="fake",
                status=CheckStatus.OK,
                detail="ok",
                evidence_ids=checks_evidence,
                at=T0,
            ),
        ),
        evidence_ids=evidence_ids,
        created_at=T0,
    )


def _record_log(evidence: EvidenceService, evidence_root: Path, run_id: str) -> str:
    target = evidence_root / "shot.log"
    target.write_text("hello", encoding="utf-8")
    return evidence.record(
        run_id,
        kind=EvidenceKind.LOG,
        path=Path("shot.log"),
        summary="测试证据",
    )
