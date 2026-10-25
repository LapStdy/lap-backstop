"""core.__main__ 的 backup 与 db check：在真实库文件上按 docs/08 的口径运行。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from core.__main__ import EXIT_ERROR, EXIT_OK, main
from storage import migrate, open_database, verify_snapshot

UPDATED_AT = "2026-10-26T00:00:00+00:00"


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    connection = open_database(tmp_path / "backstop.sqlite")
    try:
        migrate(connection)
        connection.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            ("api.host", "127.0.0.1", UPDATED_AT),
        )
    finally:
        connection.close()
    return tmp_path


def test_backup_writes_snapshot_manifest_and_config_exports(data_root: Path) -> None:
    assert main(["backup", "--data-root", str(data_root)]) == EXIT_OK
    backups = data_root / "backups"
    snapshots = sorted(backups.glob("db-*.sqlite"))
    manifests = sorted(backups.glob("db-*.manifest.json"))
    assert len(snapshots) == len(manifests) == 1
    result = verify_snapshot(snapshots[0])
    assert result.row_counts["settings"] == 1
    assert result.schema_version == 1
    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert payload["file"] == snapshots[0].name
    assert payload["sha256"] == result.sha256
    assert len(sorted(backups.glob("settings-*.json"))) == 1
    assert len(sorted(backups.glob("plugins-*.json"))) == 1


def test_backup_without_database_hints_to_initialize(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["backup", "--data-root", str(tmp_path)]) == EXIT_ERROR
    assert "库文件不存在" in capsys.readouterr().err
    assert not (tmp_path / "backups").exists()


def test_backup_on_unreadable_database_leaves_no_snapshot(tmp_path: Path) -> None:
    (tmp_path / "backstop.sqlite").write_bytes(b"not a database" * 64)
    assert main(["backup", "--data-root", str(tmp_path)]) == EXIT_ERROR
    assert not list((tmp_path / "backups").glob("db-*"))


def test_db_check_passes_on_current_database(data_root: Path) -> None:
    assert main(["db", "check", "--data-root", str(data_root)]) == EXIT_OK


def test_db_check_migrates_restored_empty_database(tmp_path: Path) -> None:
    sqlite3.connect(tmp_path / "backstop.sqlite").close()
    assert main(["db", "check", "--data-root", str(tmp_path)]) == EXIT_OK
    connection = open_database(tmp_path / "backstop.sqlite")
    try:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    finally:
        connection.close()
    assert int(version) == 1


def test_db_check_rejects_corrupt_database(tmp_path: Path) -> None:
    (tmp_path / "backstop.sqlite").write_bytes(b"not a database" * 64)
    assert main(["db", "check", "--data-root", str(tmp_path)]) == EXIT_ERROR


def test_db_check_without_database_hints_to_restore(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["db", "check", "--data-root", str(tmp_path)]) == EXIT_ERROR
    assert "库文件不存在" in capsys.readouterr().err


def test_worker_subcommand_stays_out_of_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["--help"])
    assert raised.value.code == 0
    assert "worker" not in capsys.readouterr().out
