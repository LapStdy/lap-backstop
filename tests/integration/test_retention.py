"""保留清理：日闸门、证据文件、运行记录与日志的删除口径。

所有时刻由虚拟时钟给出；文件落在 tmp_path 数据根，数据库是真实 SQLite 文件，
与 serve 路径共用同一份 PRAGMA（外键打开）。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import pytest

from contracts import EventType, RunState
from core.clock import VirtualClock
from core.hostops.retention import (
    RETENTION_PRUNED_EVENT,
    RETENTION_PRUNED_ON_KEY,
    RetentionPolicy,
    RetentionRunner,
)
from core.hostops.serve import HeartbeatWriter, ServeLoop, ServeOptions
from core.jobs.run_service import JobRunService
from core.supervisor import Supervisor
from storage import migrate, open_database
from storage.repositories.iso_time import format_at

SHANGHAI = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"


def local_moment(hour: int, minute: int, *, day: int = 30) -> datetime:
    """2026-10-<day> 的 Asia/Shanghai 本地时刻，转成 UTC 交给虚拟时钟。"""
    return datetime(2026, 10, day, hour, minute, tzinfo=SHANGHAI).astimezone(UTC)


def policy_with(**overrides: Any) -> RetentionPolicy:
    """在默认策略上替换个别期限，避免测试真造 20 GB 证据。"""
    return replace(RetentionPolicy.from_defaults(), **overrides)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    connection = open_database(tmp_path / "backstop.sqlite")
    migrate(connection)
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', 'env-1', '1', '{}', '{}', 1, ?, ?)",
        (TEMPLATE_ID, NOW.isoformat(), NOW.isoformat()),
    )
    try:
        yield connection
    finally:
        connection.close()


def seed_run(
    connection: sqlite3.Connection,
    run_id: str,
    *,
    state: RunState = RunState.SUCCEEDED,
    created_at: datetime = NOW,
) -> None:
    connection.execute(
        "INSERT INTO job_runs (id, template_id, created_at, idempotency_key, state) "
        "VALUES (?, ?, ?, ?, ?)",
        (run_id, TEMPLATE_ID, format_at(created_at), f"key-{run_id}", state.value),
    )


def seed_evidence(
    connection: sqlite3.Connection,
    run_id: str,
    name: str,
    *,
    created_at: datetime,
) -> None:
    connection.execute(
        "INSERT INTO evidence (id, run_id, kind, source, path, sha256, summary, created_at) "
        "VALUES (?, ?, 'log', 'kernel', ?, 'sha', '', ?)",
        (f"ev-{run_id}-{name}", run_id, name, format_at(created_at)),
    )


def write_evidence(tmp_path: Path, name: str, content: bytes = b"payload") -> Path:
    path = tmp_path / "evidence" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def seed_run_event(connection: sqlite3.Connection, run_id: str, *, at: datetime = NOW) -> None:
    connection.execute(
        "INSERT INTO run_events (id, run_id, seq, type, ts, payload) "
        "VALUES (?, ?, 1, 'run.created', ?, '{}')",
        (f"r:{run_id}:1", run_id, format_at(at)),
    )


def seed_phase(connection: sqlite3.Connection, run_id: str) -> None:
    connection.execute(
        "INSERT INTO run_phases (id, run_id, phase, attempt, state, timeout_seconds) "
        "VALUES (?, ?, 'execute', 1, 'ok', 300)",
        (f"phase-{run_id}", run_id),
    )


def count(connection: sqlite3.Connection, table: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0])


def pruned_on(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        "SELECT value FROM settings WHERE key = ?",
        (RETENTION_PRUNED_ON_KEY,),
    ).fetchone()
    return None if row is None else str(row[0])


def pruned_payloads(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    payloads = []
    rows = connection.execute(
        "SELECT payload FROM events_global WHERE type = ? ORDER BY seq",
        (EventType.CORE_EVENT.value,),
    ).fetchall()
    for row in rows:
        payload = json.loads(str(row["payload"]))
        if payload.get("event") == RETENTION_PRUNED_EVENT:
            payloads.append(payload)
    return payloads


def test_runs_at_local_0330_once_and_again_next_day(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 29))
    runner = RetentionRunner(database, clock, data_root=tmp_path)
    assert runner.run_once() is None
    assert pruned_on(database) is None

    clock.advance(timedelta(minutes=1))
    outcome = runner.run_once()
    assert outcome is not None
    assert outcome.date == "2026-10-30"
    assert pruned_on(database) == json.dumps("2026-10-30")
    assert pruned_payloads(database) == [
        {"event": RETENTION_PRUNED_EVENT, "detail": outcome.detail()}
    ]
    assert set(outcome.detail()) == {
        "date",
        "evidence_files_deleted",
        "evidence_bytes_freed",
        "evidence_bytes_after",
        "run_events_deleted",
        "run_phases_deleted",
        "verdicts_deleted",
        "runs_deleted",
        "advisor_deleted",
        "log_files_deleted",
    }

    assert runner.run_once() is None
    clock.advance(timedelta(days=1))
    assert runner.run_once() is not None
    assert len(pruned_payloads(database)) == 2


def test_restart_same_day_skips_via_settings(tmp_path: Path) -> None:
    clock = VirtualClock(local_moment(3, 30))
    path = tmp_path / "backstop.sqlite"
    first = open_database(path)
    migrate(first)
    try:
        assert RetentionRunner(first, clock, data_root=tmp_path).run_once() is not None
    finally:
        first.close()
    reopened = open_database(path)
    try:
        assert RetentionRunner(reopened, clock, data_root=tmp_path).run_once() is None
        assert len(pruned_payloads(reopened)) == 1
    finally:
        reopened.close()


class _StubSupervisor:
    """失败路径专用的最小 Supervisor：只计数 tick。"""

    def __init__(self) -> None:
        self.ticks = 0

    def tick(self) -> None:
        self.ticks += 1

    @property
    def is_disk_critical(self) -> bool:
        return False


class _RecordingLogger:
    """只留 error 行的记录器；断言维护失败进了日志。"""

    def __init__(self) -> None:
        self.errors: list[dict[str, Any]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        self.errors.append(dict(fields))


def test_serve_loop_survives_retention_failure_and_keeps_ticking(tmp_path: Path) -> None:
    clock = VirtualClock(local_moment(3, 30))
    connection = open_database(tmp_path / "backstop.sqlite")
    migrate(connection)
    broken = open_database(tmp_path / "broken.sqlite")
    broken.close()
    recorder = _RecordingLogger()
    supervisor = _StubSupervisor()
    try:
        loop = ServeLoop(
            cast(Supervisor, supervisor),
            JobRunService(connection, clock),
            HeartbeatWriter(
                path=tmp_path / "run" / "heartbeat.json",
                started_at=NOW,
                pid=1,
                version="dev",
            ),
            ServeOptions(clock=clock, max_ticks=2, tick_interval_seconds=0.0, log=recorder),
            retention=RetentionRunner(broken, clock, data_root=tmp_path),
        )
        assert loop.run() == 0
        assert supervisor.ticks == 2
        assert recorder.errors
        assert recorder.errors[0]["event"] == "maintenance.retention_prune_failed"
        # 失败不落闸门键，重启或下个 tick 还会补试
        assert pruned_on(connection) is None
    finally:
        connection.close()


def test_evidence_files_expire_after_30_days_but_rows_stay(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 30))
    seed_run(database, "run-old", created_at=NOW - timedelta(days=40))
    expired = write_evidence(tmp_path, "run-old/old.png", b"old")
    seed_evidence(database, "run-old", "run-old/old.png", created_at=NOW - timedelta(days=31))
    fresh = write_evidence(tmp_path, "run-old/fresh.png", b"fresh")
    seed_evidence(database, "run-old", "run-old/fresh.png", created_at=NOW - timedelta(days=2))

    outcome = RetentionRunner(database, clock, data_root=tmp_path).run_once()

    assert outcome is not None
    assert not expired.exists()
    assert fresh.exists()
    assert count(database, "evidence") == 2
    assert outcome.evidence_files_deleted == 1
    assert outcome.evidence_bytes_freed == 3
    assert outcome.evidence_bytes_after == 5


def test_evidence_quota_deletes_oldest_terminal_files(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 30))
    seed_run(database, "run-old", created_at=NOW - timedelta(days=10))
    seed_run(database, "run-new", created_at=NOW - timedelta(days=1))
    oldest = write_evidence(tmp_path, "run-old/old.bin", b"a" * 100)
    seed_evidence(database, "run-old", "run-old/old.bin", created_at=NOW - timedelta(days=5))
    newest = write_evidence(tmp_path, "run-new/new.bin", b"b" * 100)
    seed_evidence(database, "run-new", "run-new/new.bin", created_at=NOW - timedelta(days=1))

    runner = RetentionRunner(
        database, clock, data_root=tmp_path, policy=policy_with(evidence_total_bytes=150)
    )
    outcome = runner.run_once()

    assert outcome is not None
    assert not oldest.exists()
    assert newest.exists()
    assert count(database, "evidence") == 2
    assert outcome.evidence_files_deleted == 1
    assert outcome.evidence_bytes_freed == 100
    assert outcome.evidence_bytes_after == 100


def test_run_events_and_phases_expire_after_30_days(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 30))
    seed_run(database, "run-old", created_at=NOW - timedelta(days=40))
    seed_run_event(database, "run-old")
    seed_phase(database, "run-old")
    seed_run(database, "run-new", created_at=NOW - timedelta(days=5))
    seed_run_event(database, "run-new")
    seed_phase(database, "run-new")

    outcome = RetentionRunner(database, clock, data_root=tmp_path).run_once()

    assert outcome is not None
    assert outcome.run_events_deleted == 1
    assert outcome.run_phases_deleted == 1
    assert count(database, "run_events") == 1
    assert count(database, "run_phases") == 1


def test_terminal_runs_older_than_90_days_are_deleted_without_fk_errors(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 30))
    old = NOW - timedelta(days=120)
    seed_run(database, "run-old", created_at=old)
    database.execute(
        "INSERT INTO verdicts (id, run_id, status, confidence, reason, policy_id, created_at) "
        "VALUES ('v-1', 'run-old', 'completed', 0.9, 'ok', 'p-1', ?)",
        (format_at(old),),
    )
    database.execute("UPDATE job_runs SET verdict_id = 'v-1' WHERE id = 'run-old'")
    write_evidence(tmp_path, "run-old/old.png")
    seed_evidence(database, "run-old", "run-old/old.png", created_at=old)
    seed_run_event(database, "run-old")
    seed_phase(database, "run-old")
    database.execute(
        "INSERT INTO request_idempotency (key, scope, run_id, created_at) "
        "VALUES ('idem-old', 'run_now', 'run-old', ?)",
        (format_at(old),),
    )
    database.execute(
        "INSERT INTO notifications (id, run_id, channel, status, attempts) "
        "VALUES ('n-1', 'run-old', 'ntfy', 'sent', 1)"
    )
    database.execute(
        "INSERT INTO resources (id, type, name, capacity) "
        "VALUES ('res-1', 'environment', '模拟器', 1)"
    )
    database.execute(
        "INSERT INTO resource_leases "
        "(id, resource_id, run_id, owner, state, acquired_at, expires_at) "
        "VALUES ('lease-1', 'res-1', 'run-old', 'core', 'released', ?, ?)",
        (format_at(old), format_at(old)),
    )
    # advisor_calls 也引用 job_runs：即使创建时间没到 90 天，也要随运行一起清掉。
    database.execute(
        "INSERT INTO advisor_calls (id, run_id, purpose, model, created_at) "
        "VALUES ('a-1', 'run-old', 'p', 'm', ?)",
        (format_at(NOW),),
    )

    outcome = RetentionRunner(database, clock, data_root=tmp_path).run_once()

    assert outcome is not None
    assert outcome.verdicts_deleted == 1
    assert outcome.runs_deleted == 1
    assert outcome.advisor_deleted == 1
    for table in (
        "job_runs",
        "verdicts",
        "evidence",
        "run_events",
        "run_phases",
        "request_idempotency",
        "notifications",
        "resource_leases",
        "advisor_calls",
    ):
        assert count(database, table) == 0, table


def test_advisor_calls_expire_after_90_days(database: sqlite3.Connection, tmp_path: Path) -> None:
    clock = VirtualClock(local_moment(3, 30))
    database.execute(
        "INSERT INTO advisor_calls (id, purpose, model, created_at) VALUES ('a-old', 'p', 'm', ?)",
        (format_at(NOW - timedelta(days=100)),),
    )
    database.execute(
        "INSERT INTO advisor_calls (id, purpose, model, created_at) VALUES ('a-new', 'p', 'm', ?)",
        (format_at(NOW - timedelta(days=10)),),
    )

    outcome = RetentionRunner(database, clock, data_root=tmp_path).run_once()

    assert outcome is not None
    assert outcome.advisor_deleted == 1
    assert count(database, "advisor_calls") == 1


def test_logs_expire_by_age_and_keep_watchdog_and_unknown_files(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 30))
    logs = tmp_path / "logs"
    logs.mkdir()
    old_raw = logs / "core-2026-10-01.jsonl"
    fresh_raw = logs / "core-2026-10-20.jsonl"
    old_rotated = logs / "core-2026-10-01.2.jsonl"
    old_archive = logs / "core-2026-06-01.jsonl.gz"
    fresh_archive = logs / "core-2026-10-01.jsonl.gz"
    watchdog = logs / "watchdog.jsonl"
    other = logs / "notes.txt"
    for path in (
        old_raw,
        fresh_raw,
        old_rotated,
        old_archive,
        fresh_archive,
        watchdog,
        other,
    ):
        path.write_text("{}\n", encoding="utf-8")

    outcome = RetentionRunner(database, clock, data_root=tmp_path).run_once()

    assert outcome is not None
    assert outcome.log_files_deleted == 3
    assert not old_raw.exists()
    assert not old_rotated.exists()
    assert not old_archive.exists()
    assert fresh_raw.exists()
    assert fresh_archive.exists()
    assert watchdog.exists()
    assert other.exists()


def test_missing_logs_directory_is_not_an_error(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 30))
    outcome = RetentionRunner(database, clock, data_root=tmp_path).run_once()
    assert outcome is not None
    assert outcome.log_files_deleted == 0


def test_non_terminal_run_evidence_is_untouched(
    database: sqlite3.Connection, tmp_path: Path
) -> None:
    clock = VirtualClock(local_moment(3, 30))
    seed_run(
        database,
        "run-live",
        state=RunState.EXECUTING,
        created_at=NOW - timedelta(days=100),
    )
    kept = write_evidence(tmp_path, "run-live/live.png")
    seed_evidence(database, "run-live", "run-live/live.png", created_at=NOW - timedelta(days=100))

    outcome = RetentionRunner(database, clock, data_root=tmp_path).run_once()

    assert outcome is not None
    assert kept.exists()
    assert count(database, "evidence") == 1
    assert count(database, "job_runs") == 1
    assert outcome.runs_deleted == 0
    assert outcome.evidence_files_deleted == 0
