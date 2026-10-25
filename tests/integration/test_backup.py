"""数据库快照在真实 SQLite 文件上的行为：一致性、manifest、完整性与校验。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.clock import VirtualClock
from storage import BackupError, create_snapshot, migrate, open_database, verify_snapshot
from storage.db import backup as backup_module

UPDATED_AT = "2026-10-25T00:00:00+00:00"


@pytest.fixture
def database(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    connection.executemany(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        [("api.host", "127.0.0.1", UPDATED_AT), ("api.port", "8787", UPDATED_AT)],
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def clock() -> VirtualClock:
    return VirtualClock(datetime(2026, 10, 25, 4, 0, 0, tzinfo=UTC))


@pytest.fixture
def backups_dir(tmp_path: Path) -> Path:
    return tmp_path / "backups"


def test_snapshot_matches_source_and_survives_later_writes(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    result = create_snapshot(database, backups_dir, clock=clock)
    assert result.kind == "daily"
    assert result.schema_version == 1
    assert result.row_counts["settings"] == 2
    assert _sha256(result.path) == result.sha256

    snapshot = sqlite3.connect(f"file:{result.path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        rows = snapshot.execute("SELECT key, value FROM settings ORDER BY key").fetchall()
    finally:
        snapshot.close()
    assert [tuple(row) for row in rows] == [("api.host", "127.0.0.1"), ("api.port", "8787")]

    database.execute(
        "INSERT INTO settings (key, value, updated_at) "
        "VALUES ('scheduler.catch_up', 'skip', '2026-10-25T00:01:00+00:00')"
    )
    assert verify_snapshot(result.path).row_counts["settings"] == 2


def test_snapshot_names_and_manifest_use_clock_timestamp(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    result = create_snapshot(database, backups_dir, clock=clock)
    assert result.path.name == "db-20261025-040000.sqlite"
    assert result.manifest_path.name == "db-20261025-040000.manifest.json"
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["kind"] == "daily"
    assert manifest["created_at"] == "2026-10-25T04:00:00+00:00"
    assert manifest["row_counts"]["settings"] == 2


def test_second_snapshot_in_same_second_rejected(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    first = create_snapshot(database, backups_dir, clock=clock)
    with pytest.raises(BackupError, match="同名快照已存在"):
        create_snapshot(database, backups_dir, clock=clock)
    assert verify_snapshot(first.path).sha256 == first.sha256


def test_integrity_failure_leaves_no_snapshot(
    database: sqlite3.Connection,
    clock: VirtualClock,
    backups_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(connection: sqlite3.Connection) -> None:
        raise sqlite3.DatabaseError("注入的完整性故障")

    monkeypatch.setattr(backup_module, "verify_integrity", broken)
    with pytest.raises(BackupError, match="integrity_check"):
        create_snapshot(database, backups_dir, clock=clock)
    assert list(backups_dir.iterdir()) == []


def test_verify_snapshot_rejects_tampered_file(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    result = create_snapshot(database, backups_dir, clock=clock)
    with result.path.open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(BackupError, match="SHA-256"):
        verify_snapshot(result.path)


def test_verify_snapshot_rejects_missing_manifest(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    result = create_snapshot(database, backups_dir, clock=clock)
    result.manifest_path.unlink()
    with pytest.raises(BackupError, match="manifest 不可读"):
        verify_snapshot(result.path)


def test_verify_snapshot_returns_manifest_record_for_weekly(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    result = create_snapshot(database, backups_dir, clock=clock, kind="weekly")
    verified = verify_snapshot(result.path)
    assert verified == result
    assert verified.kind == "weekly"


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()
