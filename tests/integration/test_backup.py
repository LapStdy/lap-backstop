"""数据库快照在真实 SQLite 文件上的行为：一致性、manifest、完整性与校验。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from core.clock import VirtualClock
from storage import (
    BackupError,
    create_pre_migrate_snapshot,
    create_pre_upgrade_snapshot,
    create_snapshot,
    export_configs,
    migrate,
    open_database,
    prune_backups,
    verify_snapshot,
)
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
    assert result.schema_version == 2
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


def test_export_configs_writes_settings_and_plugins_summary(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    database.execute(
        "INSERT INTO plugins (id, name, version, api_version, capabilities, config_schema, "
        "error_map, requirements, enabled, installed_at) VALUES "
        "('maa', 'MAA', '6.20.1', 1, '[]', '{}', '{}', '{}', 1, '2026-10-25T00:00:00+00:00')"
    )
    result = export_configs(database, backups_dir, clock=clock)
    assert result.settings_path.name == "settings-20261025-040000.json"
    assert result.plugins_path.name == "plugins-20261025-040000.json"
    assert result.settings_count == 2
    assert result.plugins_count == 1

    settings = json.loads(result.settings_path.read_text(encoding="utf-8"))
    assert settings["values"] == {"api.host": "127.0.0.1", "api.port": "8787"}
    plugins = json.loads(result.plugins_path.read_text(encoding="utf-8"))
    assert plugins["plugins"] == [{"id": "maa", "version": "6.20.1", "enabled": True}]


def test_export_configs_leave_out_secret_setting_keys(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    database.executemany(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
        [
            ("api.token", '"secret-token"', UPDATED_AT),
            ("notify.token", '"SCT-secret-9x"', UPDATED_AT),
            ("notify.webhook", '"https://example.test/hook"', UPDATED_AT),
        ],
    )

    result = export_configs(database, backups_dir, clock=clock)

    settings = json.loads(result.settings_path.read_text(encoding="utf-8"))
    assert "api.token" not in settings["values"]
    assert "notify.token" not in settings["values"]
    assert "notify.webhook" not in settings["values"]
    assert result.settings_count == 2


def test_prune_keeps_newest_by_kind_and_skips_pre_migrate(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    for _ in range(16):
        create_snapshot(database, backups_dir, clock=clock)
        _advance(clock)
    for _ in range(9):
        create_snapshot(database, backups_dir, clock=clock, kind="weekly")
        _advance(clock)
    pre_migrate = create_pre_migrate_snapshot(database, backups_dir, version=2, clock=clock)

    result = prune_backups(backups_dir)

    kinds = [
        json.loads(manifest.read_text(encoding="utf-8"))["kind"]
        for manifest in backups_dir.glob("*.manifest.json")
    ]
    assert kinds.count("daily") == 14
    assert kinds.count("weekly") == 8
    assert kinds.count("pre-migrate") == 1
    assert len(result.deleted) == 6
    assert not (backups_dir / "db-20261025-040000.sqlite").exists()
    assert pre_migrate.path.exists()
    assert pre_migrate.manifest_path.exists()


def test_prune_keeps_newest_export_generations(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    for _ in range(22):
        export_configs(database, backups_dir, clock=clock)
        _advance(clock)

    result = prune_backups(backups_dir)

    assert len(list(backups_dir.glob("settings-*.json"))) == 20
    assert len(list(backups_dir.glob("plugins-*.json"))) == 20
    assert not (backups_dir / "settings-20261025-040000.json").exists()
    assert len(result.deleted) == 4


def test_prune_empty_directory_is_noop(backups_dir: Path) -> None:
    backups_dir.mkdir(parents=True)
    assert prune_backups(backups_dir).deleted == ()


def _advance(clock: VirtualClock, seconds: int = 1) -> None:
    clock.advance(timedelta(seconds=seconds))


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def test_pre_upgrade_snapshot_is_exempt_from_routine_pruning(
    database: sqlite3.Connection, clock: VirtualClock, backups_dir: Path
) -> None:
    snapshot = create_pre_upgrade_snapshot(database, backups_dir, version="1.2.0", clock=clock)

    assert verify_snapshot(snapshot.path).kind == "pre-upgrade"
    assert snapshot.path.name.startswith("pre-upgrade-1.2.0-")
    prune_backups(backups_dir)
    assert snapshot.path.exists(), "pre-upgrade 快照被例行备份修剪删除"
