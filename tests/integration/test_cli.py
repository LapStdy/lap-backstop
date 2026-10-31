"""core.__main__ 的 backup 与 db check：在真实库文件上按 docs/08 的口径运行。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from contracts import NotificationLevel
from core.__main__ import EXIT_ERROR, EXIT_OK, main
from core.api.store import put_settings
from core.notify import selftest
from storage import BackupError, migrate, open_database, verify_snapshot
from tests.notify_support import ENABLED, NOW, StubChannel

UPDATED_AT = "2026-10-26T00:00:00+00:00"


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    connection = open_database(tmp_path / "backstop.sqlite")
    try:
        migrate(connection)
        connection.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            ("api.host", json.dumps("127.0.0.1"), UPDATED_AT),
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
    assert result.schema_version == 2
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


def test_backup_failure_sends_error_notification(
    data_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    connection = open_database(data_root / "backstop.sqlite")
    try:
        put_settings(connection, ENABLED, at=NOW)
    finally:
        connection.close()
    stub = StubChannel()
    monkeypatch.setattr(selftest, "build_channel", lambda _config: stub)

    def fail_snapshot(*_args: object, **_kwargs: object) -> None:
        raise BackupError("integrity_check 未通过：db-1.sqlite")

    monkeypatch.setattr("storage.create_snapshot", fail_snapshot)
    assert main(["backup", "--data-root", str(data_root)]) == EXIT_ERROR
    assert "备份失败" in capsys.readouterr().err
    assert len(stub.sent) == 1
    assert stub.sent[0].level is NotificationLevel.ERROR
    assert stub.sent[0].title == "数据库备份失败"
    assert "integrity_check 未通过" in stub.sent[0].body


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
    assert int(version) == 2


def test_db_check_rejects_corrupt_database(tmp_path: Path) -> None:
    (tmp_path / "backstop.sqlite").write_bytes(b"not a database" * 64)
    assert main(["db", "check", "--data-root", str(tmp_path)]) == EXIT_ERROR


def test_db_check_failure_sends_error_notification(
    data_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection = open_database(data_root / "backstop.sqlite")
    try:
        put_settings(connection, ENABLED, at=NOW)
        connection.execute("PRAGMA user_version = 9")
    finally:
        connection.close()
    stub = StubChannel()
    monkeypatch.setattr(selftest, "build_channel", lambda _config: stub)
    assert main(["db", "check", "--data-root", str(data_root)]) == EXIT_ERROR
    assert len(stub.sent) == 1
    assert stub.sent[0].level is NotificationLevel.ERROR
    assert stub.sent[0].title == "数据库检查未通过"
    assert "高于代码支持" in stub.sent[0].body


def test_db_check_without_database_hints_to_restore(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["db", "check", "--data-root", str(tmp_path)]) == EXIT_ERROR
    assert "库文件不存在" in capsys.readouterr().err


def test_evidence_mirror_copies_into_backups_and_writes_finished_event(
    data_root: Path,
) -> None:
    evidence = data_root / "evidence" / "shot.png"
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(b"alpha")

    assert main(["evidence-mirror", "--data-root", str(data_root)]) == EXIT_OK

    copied = data_root / "backups" / "evidence" / "shot.png"
    assert copied.read_bytes() == b"alpha"
    connection = open_database(data_root / "backstop.sqlite")
    try:
        rows = connection.execute(
            "SELECT payload FROM events_global WHERE type = 'host.event'"
        ).fetchall()
    finally:
        connection.close()
    events = [json.loads(str(row["payload"]))["event"] for row in rows]
    assert "host.evidence_mirror_finished" in events


def test_evidence_mirror_without_database_hints_to_initialize(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["evidence-mirror", "--data-root", str(tmp_path)]) == EXIT_ERROR
    assert "库文件不存在" in capsys.readouterr().err
    assert not (tmp_path / "backups" / "evidence").exists()


def test_evidence_mirror_failure_records_event_and_exits_error(
    data_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from core.hostops import evidence_mirror
    from core.hostops.evidence_mirror import EvidenceMirrorError

    def fail(*_args: object, **_kwargs: object) -> object:
        raise EvidenceMirrorError("shot.png：注入的搬运故障")

    monkeypatch.setattr(evidence_mirror, "mirror_evidence", fail)
    assert main(["evidence-mirror", "--data-root", str(data_root)]) == EXIT_ERROR
    assert "证据镜像失败" in capsys.readouterr().err
    connection = open_database(data_root / "backstop.sqlite")
    try:
        rows = connection.execute(
            "SELECT payload FROM events_global WHERE type = 'host.event'"
        ).fetchall()
    finally:
        connection.close()
    failures = [
        json.loads(str(row["payload"]))
        for row in rows
        if json.loads(str(row["payload"]))["event"] == "host.evidence_mirror_failed"
    ]
    assert len(failures) == 1
    assert failures[0]["detail"] == "shot.png：注入的搬运故障"


def test_worker_subcommand_stays_out_of_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["--help"])
    assert raised.value.code == 0
    assert "worker" not in capsys.readouterr().out
