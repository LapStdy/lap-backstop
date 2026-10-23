"""场景：执行器经 worker 子进程跑完一条生命周期。"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from contracts import EnvironmentState, PhaseState, RunState, VerdictStatus
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.rpc_host import RpcPluginHost
from core.resources.service import LeaseService
from core.worker.executor import WorkerExecutor
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.run_phases import RunPhaseRepository
from storage.repositories.verdicts import VerdictRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
PLUGINS_ROOT = Path(__file__).resolve().parents[2] / "plugins"


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


@pytest.fixture
def env(tmp_path: Path) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, Path]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    data_root = tmp_path / "data"
    try:
        yield connection, clock, runs, data_root
    finally:
        connection.close()


@pytest.fixture
def host() -> Iterator[RpcPluginHost]:
    session = RpcPluginHost(PLUGINS_ROOT, ["fake"])
    try:
        yield session
    finally:
        session.close()


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    scenario: str,
    *,
    key: str = "schedule-1:2026-10-23T12:00:00+00:00",
) -> str:
    insert_template(connection, scenario)
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    runs.enqueue(record.id)
    return record.id


def executor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    host: RpcPluginHost,
) -> WorkerExecutor:
    return WorkerExecutor(connection, clock, host.registry, data_root=data_root)


def phases(connection: sqlite3.Connection, run_id: str) -> dict[str, PhaseState]:
    rows = RunPhaseRepository(connection).for_run(run_id)
    return {row.phase: row.state for row in rows}


def test_instant_run_succeeds_through_worker(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    host: RpcPluginHost,
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    result = executor_for(connection, clock, data_root, host).execute(run_id)
    assert result.state is RunState.SUCCEEDED
    assert phases(connection, run_id) == {
        "prepare": PhaseState.OK,
        "execute": PhaseState.OK,
        "verify": PhaseState.OK,
        "cleanup": PhaseState.OK,
    }
    assert [item.status for item in VerdictRepository(connection).for_run(run_id)] == [
        VerdictStatus.COMPLETED
    ]
    evidence_file = data_root / "evidence" / run_id / "fake.log"
    assert evidence_file.is_file()
    assert EnvironmentService(connection, clock).get(ENV_ID).state is EnvironmentState.OFF
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_plugin_runs_in_a_separate_process(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    host: RpcPluginHost,
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "instant")
    executor_for(connection, clock, data_root, host).execute(run_id)
    channel = host.channels["fake"]
    assert channel.pid is not None
    assert channel.pid != os.getpid()


def test_plugin_exception_fails_with_plugin_error(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    host: RpcPluginHost,
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "crash")
    result = executor_for(connection, clock, data_root, host).execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_PLUGIN"
    assert LeaseService(connection, clock).active_for_run(run_id) == ()


def test_hang_scenario_marks_execute_timed_out(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    host: RpcPluginHost,
) -> None:
    connection, clock, runs, data_root = env
    run_id = prepare_run(connection, runs, "hang")
    result = executor_for(connection, clock, data_root, host).execute(run_id)
    assert result.state is RunState.UNKNOWN
    assert phases(connection, run_id)["execute"] is PhaseState.TIMED_OUT


def test_host_close_ends_worker_process(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    session = RpcPluginHost(PLUGINS_ROOT, ["fake"])
    run_id = prepare_run(connection, runs, "instant")
    executor_for(connection, clock, data_root, session).execute(run_id)
    channel = session.channels["fake"]
    assert channel.pid is not None
    session.close()
    assert channel.pid is None
