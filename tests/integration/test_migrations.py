"""迁移在真实 SQLite 文件上的行为：结构、版本、幂等、外键与迁移前快照。"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.clock import VirtualClock
from storage import (
    Migration,
    MigrationError,
    current_version,
    migrate,
    open_database,
    verify_integrity,
    verify_snapshot,
)

EXPECTED_TABLES = {
    "advisor_calls",
    "config_revisions",
    "environments",
    "events_global",
    "evidence",
    "job_runs",
    "job_templates",
    "notifications",
    "plugins",
    "request_idempotency",
    "resource_leases",
    "resources",
    "run_events",
    "run_phases",
    "schedules",
    "settings",
    "verdicts",
}


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    database = open_database(tmp_path / "state.db")
    try:
        yield database
    finally:
        database.close()


def test_migrate_creates_schema_and_records_version(connection: sqlite3.Connection) -> None:
    assert current_version(connection) == 0
    assert migrate(connection) == 1
    assert current_version(connection) == 1
    verify_integrity(connection)
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    assert {row[0] for row in rows} == EXPECTED_TABLES


def test_migrate_is_idempotent(connection: sqlite3.Connection) -> None:
    assert migrate(connection) == 1
    assert migrate(connection) == 1
    assert current_version(connection) == 1


def test_database_uses_wal(connection: sqlite3.Connection) -> None:
    row = connection.execute("PRAGMA journal_mode").fetchone()
    assert row is not None
    assert row[0] == "wal"


def test_foreign_keys_are_enforced(connection: sqlite3.Connection) -> None:
    migrate(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO job_runs (id, template_id, created_at, idempotency_key, state) "
            "VALUES ('run-1', 'missing-template', '2026-10-23T00:00:00+00:00', 'k1', 'scheduled')"
        )


def test_state_check_rejects_unknown_value(connection: sqlite3.Connection) -> None:
    migrate(connection)
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES ('t1', 't', 'p', 'e', '1', '{}', '{}', 1, "
        "'2026-10-23T00:00:00+00:00', '2026-10-23T00:00:00+00:00')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO job_runs (id, template_id, created_at, idempotency_key, state) "
            "VALUES ('run-1', 't1', '2026-10-23T00:00:00+00:00', 'k1', 'bogus')"
        )


def test_idempotency_key_is_unique(connection: sqlite3.Connection) -> None:
    migrate(connection)
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES ('t1', 't', 'p', 'e', '1', '{}', '{}', 1, "
        "'2026-10-23T00:00:00+00:00', '2026-10-23T00:00:00+00:00')"
    )
    connection.execute(
        "INSERT INTO job_runs (id, template_id, created_at, idempotency_key, state) "
        "VALUES ('run-1', 't1', '2026-10-23T00:00:00+00:00', 's1:2026-10-23T00:00:00+00:00', "
        "'scheduled')"
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO job_runs (id, template_id, created_at, idempotency_key, state) "
            "VALUES ('run-2', 't1', '2026-10-23T00:00:00+00:00', "
            "'s1:2026-10-23T00:00:00+00:00', 'scheduled')"
        )


def test_missing_first_migration_is_rejected(connection: sqlite3.Connection) -> None:
    with pytest.raises(MigrationError):
        migrate(connection, [Migration(2, "later", lambda _: None)])


def test_database_newer_than_code_is_rejected(connection: sqlite3.Connection) -> None:
    migrate(connection)
    connection.execute("PRAGMA user_version = 9")
    with pytest.raises(MigrationError):
        migrate(connection)


def test_migrate_snapshots_before_pending_migration(tmp_path: Path) -> None:
    connection = open_database(tmp_path / "state.db")
    try:
        migrations = [
            Migration(1, "base", _create_table("first")),
            Migration(2, "second", _create_table("second")),
        ]
        backups_dir = tmp_path / "backups"
        clock = VirtualClock(datetime(2026, 10, 25, 4, 0, 0, tzinfo=UTC))
        assert migrate(connection, migrations, backups_dir=backups_dir, clock=clock) == 2
        snapshots = list(backups_dir.glob("pre-migrate-2-*.sqlite"))
        assert len(snapshots) == 1
        result = verify_snapshot(snapshots[0])
        assert result.kind == "pre-migrate"
        assert result.schema_version == 0
    finally:
        connection.close()


def test_migrate_skips_snapshot_when_up_to_date(tmp_path: Path) -> None:
    connection = open_database(tmp_path / "state.db")
    try:
        migrate(connection)
        backups_dir = tmp_path / "backups"
        clock = VirtualClock(datetime(2026, 10, 25, 4, 0, 0, tzinfo=UTC))
        migrate(connection, backups_dir=backups_dir, clock=clock)
        assert not backups_dir.exists()
    finally:
        connection.close()


def test_migrate_requires_clock_when_backups_dir_given(tmp_path: Path) -> None:
    connection = open_database(tmp_path / "state.db")
    try:
        with pytest.raises(MigrationError, match="clock"):
            migrate(connection, backups_dir=tmp_path / "backups")
    finally:
        connection.close()


def test_failing_migration_rolls_back_whole_version(tmp_path: Path) -> None:
    connection = open_database(tmp_path / "state.db")
    try:
        base = [Migration(1, "base", _create_table("first"))]
        assert migrate(connection, base) == 1

        def broken(connection: sqlite3.Connection) -> None:
            connection.execute("CREATE TABLE second (id TEXT PRIMARY KEY)")
            raise sqlite3.OperationalError("注入的迁移故障")

        with pytest.raises(sqlite3.OperationalError, match="注入的迁移故障"):
            migrate(
                connection,
                [Migration(1, "base", _create_table("first")), Migration(2, "broken", broken)],
            )
        assert current_version(connection) == 1
        tables = _user_tables(connection)
        assert "first" in tables
        assert "second" not in tables

        fixed = [
            Migration(1, "base", _create_table("first")),
            Migration(2, "fixed", _create_table("second")),
        ]
        assert migrate(connection, fixed) == 2
        assert "second" in _user_tables(connection)
    finally:
        connection.close()


def _user_tables(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _create_table(name: str) -> Callable[[sqlite3.Connection], None]:
    def up(connection: sqlite3.Connection) -> None:
        connection.execute(f"CREATE TABLE {name} (id TEXT PRIMARY KEY)")

    return up
