"""场景：多插件接入时的派发路由——降级让位、全降级停派、恢复后重新参与。"""

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
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from core.supervisor_routing import ALL_DEGRADED_CODE, ROUTE_ALL_DEGRADED_EVENT, ROUTE_SKIPPED_EVENT
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
STABLE_DISK_FREE = 100 * 1024**3
REASON_A = "连续 3 次失败（启动超时）"
REASON_B = "连续 3 次失败（调用中退出）"


def _insert_template(connection: sqlite3.Connection, *, suffix: str, plugin_id: str) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', ?, ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            f"template-{suffix}",
            plugin_id,
            f"env-{suffix}",
            json.dumps({"scenario": "instant"}),
            json.dumps(
                {
                    "id": f"policy-{suffix}",
                    "verifier_plugin_id": plugin_id,
                    "required_evidence": ["log"],
                    "params": {},
                }
            ),
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


def _two_plugin_registry(reasons: dict[str, str]) -> PluginRegistry:
    registry = PluginRegistry(degradation=reasons.get)
    for suffix in ("a", "b"):
        registry.register(
            PluginCapabilities(
                plugin_id=f"fake-{suffix}",
                environment=FakeEnvironmentProvider(),
                script=FakeScriptAdapter(),
                verifier=FakeVerifier(),
            )
        )
    return registry


def _enqueue_run(runs: JobRunService, *, suffix: str) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=f"template-{suffix}",
            schedule_id=None,
            scheduled_at=None,
            created_at=T0,
            idempotency_key=f"run-key-{suffix}",
            priority=0,
            deadline_at=None,
        )
    )
    runs.enqueue(record.id)
    return record.id


def _global_events(connection: sqlite3.Connection) -> list[tuple[str, dict[str, object]]]:
    rows = connection.execute("SELECT payload FROM events_global ORDER BY seq").fetchall()
    events: list[tuple[str, dict[str, object]]] = []
    for (payload,) in rows:
        fields = json.loads(payload)
        if isinstance(fields, dict) and "event" in fields:
            events.append((str(fields["event"]), fields))
    return events


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, dict[str, str]]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    reasons: dict[str, str] = {}
    try:
        yield connection, clock, JobRunService(connection, clock), reasons
    finally:
        connection.close()


def _supervisor(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    reasons: dict[str, str],
) -> Supervisor:
    return Supervisor(
        connection,
        clock,
        _two_plugin_registry(reasons),
        data_root=Path(connection.execute("PRAGMA database_list").fetchone()[2]).parent,
        disk_free=lambda: STABLE_DISK_FREE,
    )


def test_dispatch_routes_to_available_plugin_and_records_skip(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, dict[str, str]],
) -> None:
    """两插件其一降级：本轮派发未降级插件的运行，事件记录跳过原因。"""
    connection, clock, runs, reasons = env
    _insert_template(connection, suffix="a", plugin_id="fake-a")
    _insert_template(connection, suffix="b", plugin_id="fake-b")
    run_a = _enqueue_run(runs, suffix="a")
    run_b = _enqueue_run(runs, suffix="b")
    reasons["fake-a"] = REASON_A

    _supervisor(connection, clock, reasons).tick()

    assert runs.get(run_b).state is RunState.SUCCEEDED
    assert runs.get(run_a).state is RunState.QUEUED
    skips = [fields for name, fields in _global_events(connection) if name == ROUTE_SKIPPED_EVENT]
    assert len(skips) == 1
    payload = skips[0]["payload"]
    assert isinstance(payload, dict)
    plugins = payload["plugins"]
    assert isinstance(plugins, list) and len(plugins) == 1
    assert plugins[0]["plugin_id"] == "fake-a"
    assert plugins[0]["run_id"] == run_a
    assert plugins[0]["reason"] == REASON_A
    assert payload["chosen_run_id"] == run_b
    assert payload["chosen_plugin_id"] == "fake-b"


def test_dispatch_without_any_available_plugin_records_all_degraded(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, dict[str, str]],
) -> None:
    """两插件全部降级：本轮不派发，事件带约定错误码与两条降级原因。"""
    connection, clock, runs, reasons = env
    _insert_template(connection, suffix="a", plugin_id="fake-a")
    _insert_template(connection, suffix="b", plugin_id="fake-b")
    run_a = _enqueue_run(runs, suffix="a")
    run_b = _enqueue_run(runs, suffix="b")
    reasons.update({"fake-a": REASON_A, "fake-b": REASON_B})

    _supervisor(connection, clock, reasons).tick()

    assert runs.get(run_a).state is RunState.QUEUED
    assert runs.get(run_b).state is RunState.QUEUED
    blocks = [
        fields for name, fields in _global_events(connection) if name == ROUTE_ALL_DEGRADED_EVENT
    ]
    assert len(blocks) == 1
    payload = blocks[0]["payload"]
    assert isinstance(payload, dict)
    assert payload["error_code"] == ALL_DEGRADED_CODE
    plugins = payload["plugins"]
    assert isinstance(plugins, list)
    assert {item["plugin_id"]: item["reason"] for item in plugins} == {
        "fake-a": REASON_A,
        "fake-b": REASON_B,
    }


def test_recovered_plugin_rejoins_candidates_on_next_request(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, dict[str, str]],
) -> None:
    """插件在两次请求之间恢复：第二次派发重新参与候选，不被缓存挡住。"""
    connection, clock, runs, reasons = env
    _insert_template(connection, suffix="a", plugin_id="fake-a")
    _insert_template(connection, suffix="b", plugin_id="fake-b")
    run_a = _enqueue_run(runs, suffix="a")
    run_b = _enqueue_run(runs, suffix="b")
    supervisor = _supervisor(connection, clock, reasons)
    reasons["fake-a"] = REASON_A

    supervisor.tick()
    assert runs.get(run_a).state is RunState.QUEUED

    reasons.pop("fake-a")
    supervisor.tick()
    assert runs.get(run_a).state is RunState.SUCCEEDED
    assert runs.get(run_b).state is RunState.SUCCEEDED


def test_single_available_plugin_dispatches_without_route_events(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, dict[str, str]],
) -> None:
    """单插件未降级：行为与现状一致，不产生路由事件。"""
    connection, clock, runs, reasons = env
    _insert_template(connection, suffix="a", plugin_id="fake-a")
    run_a = _enqueue_run(runs, suffix="a")

    _supervisor(connection, clock, reasons).tick()

    assert runs.get(run_a).state is RunState.SUCCEEDED
    assert _global_events(connection) == []
