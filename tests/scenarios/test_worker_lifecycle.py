"""场景：worker 执行器跑完一条 JobRun 生命周期。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import (
    EnvironmentState,
    PhaseState,
    RunContext,
    RunState,
    Verdict,
    VerdictStatus,
)
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.worker.executor import CancelFlag, WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.run_phases import RunPhaseRepository
from storage.repositories.verdicts import VerdictRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"


def insert_template(connection: sqlite3.Connection, scenario: str) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": scenario}),
            json.dumps(
                {
                    "id": "fake-policy",
                    "verifier_plugin_id": "fake",
                    "required_evidence": ["log"],
                    "params": {},
                }
            ),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


def make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    return registry


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    data_root = tmp_path / "data"
    try:
        yield connection, clock, runs, data_root
    finally:
        connection.close()


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    scenario: str,
    *,
    scheduled_at: datetime = T0,
    deadline_at: datetime | None = None,
    key: str = "schedule-1:2026-10-23T12:00:00+00:00",
) -> str:
    insert_template(connection, scenario)
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


def executor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    *,
    cancel: CancelFlag | None = None,
) -> WorkerExecutor:
    return WorkerExecutor(
        connection,
        clock,
        make_registry(),
        data_root=data_root,
        cancel=cancel,
    )


def phases(connection: sqlite3.Connection, run_id: str) -> dict[str, PhaseState]:
    rows = RunPhaseRepository(connection).for_run(run_id)
    return {row.phase: row.state for row in rows}


def test_instant_scenario_succeeds_end_to_end(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.SUCCEEDED
    assert result.finished_at is not None
    verdicts = VerdictRepository(connection).for_run(run_id)
    assert [item.status for item in verdicts] == [VerdictStatus.COMPLETED]
    evidence = connection.execute(
        "SELECT COUNT(*) FROM evidence WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert evidence is not None
    assert int(evidence[0]) == 1
    assert phases(connection, run_id) == {
        "prepare": PhaseState.OK,
        "execute": PhaseState.OK,
        "verify": PhaseState.OK,
        "cleanup": PhaseState.OK,
    }
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_executor_waits_for_scheduled_time(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(
        connection,
        runs,
        "instant",
        scheduled_at=T0 + timedelta(hours=1),
    )
    executor = executor_for(connection, clock, data_root)
    waiting = executor.execute(run_id)
    assert waiting.state is RunState.WAITING
    clock.advance(timedelta(hours=1))
    finished = executor.execute(run_id)
    assert finished.state is RunState.SUCCEEDED


def test_fail_scenario_enters_recovery(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "fail")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.RECOVERING
    assert result.attempt == 1


def test_partial_scenario_ends_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "partial")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert [item.status for item in VerdictRepository(connection).for_run(run_id)] == [
        VerdictStatus.UNKNOWN
    ]


def test_hang_scenario_stops_script_and_ends_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "hang")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert phases(connection, run_id)["execute"] is PhaseState.TIMED_OUT


def test_crash_scenario_enters_recovery_with_plugin_crash(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "crash")
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.RECOVERING
    assert result.error_code == "E_PLUGIN_CRASH"
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_missed_deadline_fails_before_execution(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(
        connection,
        runs,
        "instant",
        deadline_at=T0 + timedelta(minutes=30),
    )
    clock.advance(timedelta(hours=1))
    result = executor_for(connection, clock, data_root).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_DEADLINE"


def test_cancel_flag_prevents_start(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    flag = CancelFlag()
    flag.cancel()
    result = executor_for(connection, clock, data_root, cancel=flag).execute(run_id)
    assert result.state is RunState.CANCELLED
    assert phases(connection, run_id) == {}


class _NoEvidenceVerifier:
    def verify(self, ctx: RunContext) -> Verdict:
        return Verdict(
            status=VerdictStatus.COMPLETED,
            policy_id="fake-policy",
            confidence=1.0,
            reason="没有证据",
            evidence_ids=(),
            created_at=ctx.clock.utc_now(),
        )


def test_verifier_without_evidence_ends_unknown(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=_NoEvidenceVerifier(),
        )
    )
    executor = WorkerExecutor(connection, clock, registry, data_root=data_root)
    result = executor.execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert result.error_code == "E_VERIFY_NO_EVIDENCE"
