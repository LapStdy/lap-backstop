"""场景：恢复策略驱动重试、退避、动作路由与预算耗尽。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import RunState, VerdictStatus
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.worker.executor import WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.verdicts import VerdictRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"


def insert_template(
    connection: sqlite3.Connection,
    scenario: str,
    *,
    fail_times: int = 0,
    recovery: dict[str, object] | None = None,
) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, ?, 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": scenario, "fail_times": fail_times}),
            json.dumps(
                {
                    "id": "fake-policy",
                    "verifier_plugin_id": "fake",
                    "required_evidence": ["log"],
                    "params": {},
                }
            ),
            json.dumps(recovery or {}),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    try:
        yield connection, clock, JobRunService(connection, clock), tmp_path / "data"
    finally:
        connection.close()


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    *,
    scheduled_at: datetime = T0,
    deadline_at: datetime | None = None,
    key: str = "run-key",
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=deadline_at,
        )
    )
    runs.enqueue(record.id)
    return record.id


def make_executor(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    *,
    provider: FakeEnvironmentProvider | None = None,
    script: FakeScriptAdapter | None = None,
) -> WorkerExecutor:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider() if provider is None else provider,
            script=FakeScriptAdapter() if script is None else script,
            verifier=FakeVerifier(),
        )
    )
    return WorkerExecutor(connection, clock, registry, data_root=data_root)


def event_types(connection: sqlite3.Connection, run_id: str) -> list[str]:
    rows = connection.execute(
        "SELECT type FROM run_events WHERE run_id = ? ORDER BY seq",
        (run_id,),
    ).fetchall()
    return [str(row[0]) for row in rows]


def test_retry_then_success(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(
        connection,
        "fail",
        fail_times=1,
        recovery={"max_attempts": 3, "backoff_seconds": [30, 120, 300]},
    )
    run_id = prepare_run(connection, runs)
    script = FakeScriptAdapter()
    executor = make_executor(connection, clock, data_root, script=script)
    first = executor.execute(run_id)
    assert first.state is RunState.RECOVERING
    assert first.attempt == 1
    assert "recover" in script.calls
    assert "recovery.started" in event_types(connection, run_id)
    clock.advance(timedelta(seconds=29))
    assert executor.execute(run_id).state is RunState.RECOVERING
    clock.advance(timedelta(seconds=1))
    final = executor.execute(run_id)
    assert final.state is RunState.SUCCEEDED
    assert final.attempt == 1
    assert [item.status for item in VerdictRepository(connection).for_run(run_id)] == [
        VerdictStatus.COMPLETED
    ]
    assert "recovery.finished" in event_types(connection, run_id)


def test_retry_budget_exhaustion(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(
        connection,
        "fail",
        recovery={"max_attempts": 2, "backoff_seconds": [30]},
    )
    run_id = prepare_run(connection, runs)
    executor = make_executor(connection, clock, data_root)
    assert executor.execute(run_id).state is RunState.RECOVERING
    clock.advance(timedelta(seconds=30))
    exhausted = executor.execute(run_id)
    assert exhausted.state is RunState.FAILED
    assert exhausted.error_code == "E_RECOVERY_EXHAUSTED"
    assert "recovery.finished" in event_types(connection, run_id)


def test_environment_failure_routes_to_reset(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(connection, "never-ready", recovery={"max_attempts": 3})
    run_id = prepare_run(connection, runs)
    provider = FakeEnvironmentProvider()
    executor = make_executor(connection, clock, data_root, provider=provider)
    result = executor.execute(run_id)
    assert result.state is RunState.RECOVERING
    assert result.attempt == 1
    assert "reset" in provider.calls


def test_deadline_aborts_without_retry(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(connection, "instant")
    run_id = prepare_run(
        connection,
        runs,
        deadline_at=T0 + timedelta(minutes=30),
    )
    clock.advance(timedelta(hours=1))
    result = make_executor(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"


def test_disabled_reset_falls_back_to_retry(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    insert_template(
        connection,
        "never-ready",
        recovery={"max_attempts": 3, "reset_environment": False},
    )
    run_id = prepare_run(connection, runs)
    provider = FakeEnvironmentProvider()
    result = make_executor(connection, clock, data_root, provider=provider).execute(run_id)
    assert result.state is RunState.RECOVERING
    assert "reset" not in provider.calls
