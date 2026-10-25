"""契约：插件故障只影响当次调用，内核与其余运行照常（docs/12 契约表「隔离」）。

判据「崩溃只影响当次调用」在真实 worker 子进程与 RPC 通道上验证：
同一 host 内，一次插件异常造成的失败运行不破坏数据库、不占住租约，
下一个运行仍由同一个 worker 进程跑完。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from contracts import RunState
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.rpc_host import RpcPluginHost
from core.worker.executor import WorkerExecutor
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
ENV_ID = "env-1"
PLUGINS_ROOT = Path(__file__).resolve().parents[2] / "plugins"


def insert_template(connection: sqlite3.Connection, template_id: str, scenario: str) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            template_id,
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
    try:
        yield connection, clock, JobRunService(connection, clock), tmp_path / "data"
    finally:
        connection.close()


@pytest.fixture
def host() -> Iterator[RpcPluginHost]:
    session = RpcPluginHost(PLUGINS_ROOT, ["fake"])
    try:
        yield session
    finally:
        session.close()


def enqueue_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    template_id: str,
    scenario: str,
) -> str:
    insert_template(connection, template_id, scenario)
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=template_id,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key=f"isol-{template_id}",
            priority=0,
            deadline_at=None,
        )
    )
    runs.enqueue(record.id)
    return record.id


def test_plugin_failure_only_affects_current_call(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
    host: RpcPluginHost,
) -> None:
    connection, clock, runs, data_root = env
    failed_id = enqueue_run(connection, runs, "template-crash", "crash")
    failed = WorkerExecutor(connection, clock, host.registry, data_root=data_root).execute(
        failed_id
    )
    assert failed.state is RunState.FAILED
    assert failed.error_code == "E_PLUGIN"

    next_id = enqueue_run(connection, runs, "template-instant", "instant")
    worker_pid = host.channels["fake"].pid
    assert worker_pid is not None
    succeeded = WorkerExecutor(connection, clock, host.registry, data_root=data_root).execute(
        next_id
    )
    assert succeeded.state is RunState.SUCCEEDED
    assert host.channels["fake"].pid == worker_pid
    assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
