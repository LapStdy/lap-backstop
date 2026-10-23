"""证据与结论落库：哈希、路径约束、证据引用校验。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

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
from core.events.verdicts import MissingEvidenceError, VerdictService
from core.jobs.run_service import JobRunService
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.verdicts import VerdictRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', 'env-1', '1', '{}', '{}', 1, ?, ?)",
        (TEMPLATE_ID, "2026-10-23T00:00:00+00:00", "2026-10-23T00:00:00+00:00"),
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


def test_verdict_requires_known_evidence(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, evidence_root = env
    run_id = runs.non_terminal()[0].id
    evidence = EvidenceService(connection, evidence_root, clock)
    verdicts = VerdictService(connection, clock)
    missing = _verdict(evidence_ids=())
    with pytest.raises(MissingEvidenceError):
        verdicts.record(run_id, missing)
    unknown = _verdict(evidence_ids=("not-there",))
    with pytest.raises(MissingEvidenceError):
        verdicts.record(run_id, unknown)
    target = evidence_root / "shot.log"
    target.write_text("hello", encoding="utf-8")
    evidence_id = evidence.record(
        run_id,
        kind=EvidenceKind.LOG,
        path=Path("shot.log"),
        summary="测试证据",
    )
    verdict_id = verdicts.record(run_id, _verdict(evidence_ids=(evidence_id,)))
    stored = VerdictRepository(connection).for_run(run_id)
    assert [item.id for item in stored] == [verdict_id]
    assert runs.get(run_id).verdict_id == verdict_id


def _verdict(evidence_ids: tuple[str, ...]) -> Verdict:
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
                evidence_ids=evidence_ids,
                at=T0,
            ),
        ),
        evidence_ids=evidence_ids,
        created_at=T0,
    )
