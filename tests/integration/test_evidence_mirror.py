"""证据增量镜像：增量口径、阈值裁剪、失败续做与事件，在真实库与临时目录上验证。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.hostops import evidence_mirror
from core.hostops.evidence_mirror import (
    EVIDENCE_MIRROR_FAILED_EVENT,
    EVIDENCE_MIRROR_FINISHED_EVENT,
    EvidenceMirrorError,
    mirror_evidence,
    mirror_threshold_bytes,
    plan_evidence,
    record_failure,
    record_finished,
)
from storage.repositories.iso_time import format_at

NOW = datetime(2026, 10, 25, 4, 30, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """真实 SQLite 文件连接（外键打开），播种一个模板与一个终态运行。"""
    from storage import migrate, open_database

    database = open_database(tmp_path / "backstop.sqlite")
    migrate(database)
    database.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', 'env-1', '1', '{}', '{}', 1, ?, ?)",
        (TEMPLATE_ID, format_at(NOW), format_at(NOW)),
    )
    database.execute(
        "INSERT INTO job_runs (id, template_id, created_at, idempotency_key, state) "
        "VALUES ('run-1', ?, ?, 'key-run-1', 'succeeded')",
        (TEMPLATE_ID, format_at(NOW)),
    )
    try:
        yield database
    finally:
        database.close()


class CountingCopier:
    """记录搬运次数的替身：计数后委托真实搬运，断言用例读 calls。"""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(self, source: Path, target: Path) -> None:
        self.calls.append(source.name)
        evidence_mirror.copy_file(source, target)


def write_evidence(root: Path, name: str, content: bytes) -> Path:
    """在证据根下写一份文件；目录按需创建。"""
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def seed_evidence_row(connection: sqlite3.Connection, evidence_id: str, path: str) -> None:
    connection.execute(
        "INSERT INTO evidence (id, run_id, kind, source, path, sha256, summary, created_at) "
        "VALUES (?, 'run-1', 'log', 'kernel', ?, 'sha', '', ?)",
        (evidence_id, path, format_at(NOW)),
    )


def seed_verdict(connection: sqlite3.Connection, evidence_ids: list[str]) -> None:
    connection.execute(
        "INSERT INTO verdicts (id, run_id, status, confidence, reason, policy_id, "
        "checks, evidence_ids, created_at) VALUES ('verdict-1', 'run-1', 'completed', "
        "1.0, '', 'policy', '[]', ?, ?)",
        (json.dumps(evidence_ids), format_at(NOW)),
    )


def test_first_mirror_copies_all_and_second_skips_unchanged(
    tmp_path: Path, connection: sqlite3.Connection
) -> None:
    root = tmp_path / "evidence"
    write_evidence(root, "shot-a.png", b"alpha")
    write_evidence(root, "logs/day-1.txt", b"day one")
    copier = CountingCopier()

    plan = plan_evidence(connection, root, threshold_bytes=mirror_threshold_bytes())
    first = mirror_evidence(plan, tmp_path / "backups" / "evidence", copier=copier)

    assert plan.relatives == ("logs/day-1.txt", "shot-a.png")
    assert plan.skipped_by_policy == 0
    assert first.copied_new == 2
    assert copier.calls == ["day-1.txt", "shot-a.png"]
    assert (tmp_path / "backups" / "evidence" / "logs" / "day-1.txt").read_bytes() == b"day one"

    second = mirror_evidence(plan, tmp_path / "backups" / "evidence", copier=copier)
    assert second.copied_new == 0
    assert second.copied_changed == 0
    assert second.skipped_unchanged == 2
    assert len(copier.calls) == 2


def test_same_size_different_content_counts_as_changed(
    tmp_path: Path, connection: sqlite3.Connection
) -> None:
    root = tmp_path / "evidence"
    target = tmp_path / "backups" / "evidence"
    write_evidence(root, "shot.png", b"aaaa")
    copier = CountingCopier()
    mirror_evidence(plan_evidence(connection, root, threshold_bytes=1 << 30), target, copier=copier)

    write_evidence(root, "shot.png", b"bbbb")
    outcome = mirror_evidence(
        plan_evidence(connection, root, threshold_bytes=1 << 30), target, copier=copier
    )

    assert outcome.copied_changed == 1
    assert outcome.skipped_unchanged == 0
    assert (target / "shot.png").read_bytes() == b"bbbb"


def test_threshold_prunes_to_verdict_referenced_evidence(
    tmp_path: Path, connection: sqlite3.Connection
) -> None:
    root = tmp_path / "evidence"
    write_evidence(root, "referenced.txt", b"kept")
    write_evidence(root, "unindexed.txt", b"no row")
    write_evidence(root, "unverdicted.txt", b"row without verdict")
    seed_evidence_row(connection, "ev-1", "referenced.txt")
    seed_evidence_row(connection, "ev-2", "unverdicted.txt")
    seed_verdict(connection, ["ev-1"])

    plan = plan_evidence(connection, root, threshold_bytes=1)
    outcome = mirror_evidence(plan, tmp_path / "backups" / "evidence")

    assert plan.relatives == ("referenced.txt",)
    assert plan.skipped_by_policy == 2
    assert outcome.skipped_by_policy == 2
    assert outcome.copied_new == 1
    assert (tmp_path / "backups" / "evidence" / "referenced.txt").exists()
    assert not (tmp_path / "backups" / "evidence" / "unverdicted.txt").exists()


def test_threshold_below_total_keeps_all_files(
    tmp_path: Path, connection: sqlite3.Connection
) -> None:
    root = tmp_path / "evidence"
    write_evidence(root, "referenced.txt", b"kept")
    write_evidence(root, "unverdicted.txt", b"row without verdict")
    seed_evidence_row(connection, "ev-1", "referenced.txt")
    seed_evidence_row(connection, "ev-2", "unverdicted.txt")
    seed_verdict(connection, ["ev-1"])

    under = plan_evidence(connection, root, threshold_bytes=mirror_threshold_bytes())

    assert under.relatives == ("referenced.txt", "unverdicted.txt")
    assert under.skipped_by_policy == 0


def test_threshold_bytes_convert_gib_default() -> None:
    assert mirror_threshold_bytes() == 20 * 1024**3


def test_rerun_sweeps_orphan_staging_files(tmp_path: Path, connection: sqlite3.Connection) -> None:
    root = tmp_path / "evidence"
    target = tmp_path / "backups" / "evidence"
    write_evidence(root, "shot.png", b"alpha")
    orphan = target / "shot.png.mirror-tmp"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"half-written")

    plan = plan_evidence(connection, root, threshold_bytes=1 << 30)
    outcome = mirror_evidence(plan, target)

    assert outcome.copied_new == 1
    assert not orphan.exists()
    assert (target / "shot.png").read_bytes() == b"alpha"


def test_missing_evidence_root_yields_empty_plan(
    tmp_path: Path, connection: sqlite3.Connection
) -> None:
    plan = plan_evidence(connection, tmp_path / "evidence", threshold_bytes=1 << 30)
    outcome = mirror_evidence(plan, tmp_path / "backups" / "evidence")

    assert plan.relatives == ()
    assert outcome.detail() == {
        "copied_new": 0,
        "copied_changed": 0,
        "skipped_unchanged": 0,
        "skipped_by_policy": 0,
        "bytes_copied": 0,
    }


def test_midway_failure_keeps_backup_and_resumes_on_rerun(
    tmp_path: Path, connection: sqlite3.Connection
) -> None:
    root = tmp_path / "evidence"
    target = tmp_path / "backups" / "evidence"
    write_evidence(root, "first.txt", b"first")
    write_evidence(root, "second.txt", b"second")
    plan = plan_evidence(connection, root, threshold_bytes=1 << 30)

    def fail_on_second(source: Path, target_path: Path) -> None:
        if source.name == "second.txt":
            raise OSError("磁盘已满")
        evidence_mirror.copy_file(source, target_path)

    with pytest.raises(EvidenceMirrorError, match=r"second\.txt：磁盘已满"):
        mirror_evidence(plan, target, copier=fail_on_second)

    kept = target / "first.txt"
    assert kept.read_bytes() == b"first"
    assert not list(target.rglob("*.mirror-tmp"))

    resumed = mirror_evidence(plan, target)
    assert resumed.copied_new == 1
    assert resumed.skipped_unchanged == 1
    assert (target / "second.txt").read_bytes() == b"second"


def test_finished_and_failure_events_carry_contract_detail(
    tmp_path: Path, connection: sqlite3.Connection
) -> None:
    root = tmp_path / "evidence"
    write_evidence(root, "shot.png", b"alpha")
    outcome = mirror_evidence(
        plan_evidence(connection, root, threshold_bytes=1 << 30),
        tmp_path / "backups" / "evidence",
    )
    record_finished(connection, outcome, at=NOW)
    record_failure(connection, "shot.png：磁盘已满", at=NOW)

    finished = _latest_global(connection, EVIDENCE_MIRROR_FINISHED_EVENT)
    assert finished is not None
    assert finished["detail"] == {
        "copied_new": 1,
        "copied_changed": 0,
        "skipped_unchanged": 0,
        "skipped_by_policy": 0,
        "bytes_copied": 5,
    }
    failed = _latest_global(connection, EVIDENCE_MIRROR_FAILED_EVENT)
    assert failed is not None
    assert failed["detail"] == "shot.png：磁盘已满"


def _latest_global(connection: sqlite3.Connection, event: str) -> dict[str, object] | None:
    """最近一条指定 event 字段的全局事件载荷；没有返回 None。"""
    rows = connection.execute(
        "SELECT payload FROM events_global WHERE type = 'host.event' ORDER BY seq DESC LIMIT 100"
    ).fetchall()
    for row in rows:
        payload = json.loads(str(row["payload"]))
        if payload.get("event") == event:
            return payload
    return None
