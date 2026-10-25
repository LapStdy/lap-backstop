"""场景：运行事件流按 seq 回放出与库内一致的最终状态（docs/12 必须测「事件写入与回放」P-800）。

docs/01 P-804 与 docs/02 的判据：回放顺序与 seq 一致；从 run.state.changed
重放的状态链连续，终点等于库内终态；证据按事件引用可打开。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import RunState
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.worker.executor import WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, "
        '\'{"max_attempts": 3, "backoff_seconds": [30]}\', 1, ?, ?)',
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": "fail", "fail_times": 1}),
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
    insert_template(connection)
    clock = VirtualClock(T0)
    try:
        yield connection, clock, JobRunService(connection, clock), tmp_path / "data"
    finally:
        connection.close()


def run_once(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    runs: JobRunService,
    data_root: Path,
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key="replay-key",
            priority=0,
            deadline_at=None,
        )
    )
    runs.enqueue(record.id)
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    executor = WorkerExecutor(connection, clock, registry, data_root=data_root)
    assert executor.execute(record.id).state is RunState.RECOVERING
    clock.advance(timedelta(seconds=30))
    final = executor.execute(record.id)
    assert final.state is RunState.SUCCEEDED
    return record.id


def state_chain(runs: JobRunService, run_id: str) -> list[tuple[str, str]]:
    return [
        (str(event.payload.get("from_state")), str(event.payload.get("to_state")))
        for event in runs.events(run_id)
        if event.type == "run.state.changed"
    ]


def test_event_stream_replays_final_state(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    connection, clock, runs, data_root = env
    run_id = run_once(connection, clock, runs, data_root)

    events = runs.events(run_id)
    seqs = [event.seq for event in events]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)

    chain = state_chain(runs, run_id)
    assert chain[0][0] == RunState.SCHEDULED.value
    assert all(chain[i][1] == chain[i + 1][0] for i in range(len(chain) - 1))
    assert chain[-1][1] == RunState.SUCCEEDED.value
    assert chain[-1][1] == runs.get(run_id).state.value

    finished = [event for event in events if event.type == "run.finished"]
    assert finished[-1].payload.get("status") == RunState.SUCCEEDED.value

    evidence_root = data_root / "evidence"
    evidence_events = [event for event in events if event.type == "evidence"]
    assert evidence_events
    for event in evidence_events:
        path = str(event.payload.get("path"))
        assert (evidence_root / path).is_file()
