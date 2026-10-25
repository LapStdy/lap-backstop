"""场景：崩溃恢复的启动探测——中间态环境各探一次，失败置 unknown 等下一次探测。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from contracts import (
    EnvironmentProbe,
    EnvironmentRef,
    EnvironmentState,
    PhaseResult,
    RunContext,
    RunState,
)
from core.clock import VirtualClock
from core.environments.service import EnvironmentService
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)

_STABLE_DISK_FREE = 100 * 1024**3
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
PLUGIN_ID = "probe"


class ScriptedProbeProvider:
    """按环境脚本化探针结果：状态返回探针、异常原样抛出；耗尽后回落 ready。"""

    def __init__(self) -> None:
        self._outcomes: dict[str, list[EnvironmentState | Exception]] = {}
        self.probe_calls: dict[str, int] = {}

    def script(self, env_id: str, outcomes: list[EnvironmentState | Exception]) -> None:
        self._outcomes[env_id] = list(outcomes)

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        self.probe_calls[env.id] = self.probe_calls.get(env.id, 0) + 1
        queue = self._outcomes.get(env.id)
        outcome = queue.pop(0) if queue else EnvironmentState.READY
        if isinstance(outcome, Exception):
            raise outcome
        return EnvironmentProbe(state=outcome, detail="scripted", observed_at=ctx.clock.utc_now())

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        now = ctx.clock.utc_now()
        return PhaseResult(ok=True, detail="scripted prepare", started_at=now, ended_at=now)

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        return PhaseResult(ok=True, detail="scripted reset")

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        return PhaseResult(ok=True, detail="scripted cleanup")

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        return PhaseResult(ok=True, detail="scripted warm")


VERIFICATION_POLICY = json.dumps(
    {"id": "fake-policy", "verifier_plugin_id": "fake", "required_evidence": ["log"], "params": {}}
)


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', ?, ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            PLUGIN_ID,
            ENV_ID,
            json.dumps({"scenario": "instant"}),
            VERIFICATION_POLICY,
            T0.isoformat(),
            T0.isoformat(),
        ),
    )


def make_env(service: EnvironmentService, env_id: str = ENV_ID) -> None:
    service.register(env_id, provider_id=PLUGIN_ID, name=env_id)


def crash_leave(connection: sqlite3.Connection, env_id: str, state: EnvironmentState) -> None:
    """把环境行改成崩溃残留的持久化状态；绕开状态机模拟崩溃前的旧值。"""
    connection.execute("UPDATE environments SET state = ? WHERE id = ?", (state.value, env_id))


def env_transitions(connection: sqlite3.Connection, env_id: str) -> list[tuple[str, str]]:
    rows = connection.execute(
        "SELECT payload FROM events_global WHERE type = 'env.state.changed' ORDER BY seq"
    ).fetchall()
    return [
        (str(payload["from_state"]), str(payload["to_state"]))
        for row in rows
        if (payload := json.loads(str(row[0]))).get("environment_id") == env_id
    ]


def supervisor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    provider: ScriptedProbeProvider,
    data_root: Path,
) -> Supervisor:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id=PLUGIN_ID,
            environment=provider,
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    return Supervisor(
        connection, clock, registry, data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )


def prepare_run(runs: JobRunService, *, key: str) -> str:
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


def recovering_run(runs: JobRunService, *, key: str) -> str:
    run_id = prepare_run(runs, key=key)
    runs.transition(run_id, RunState.PREPARING)
    runs.mark_recovering(
        run_id,
        next_attempt=1,
        retry_at=T0 - timedelta(seconds=1),
        error_code="E_SCRIPT_TASK_FAILED",
    )
    return run_id


Env = tuple[
    sqlite3.Connection,
    VirtualClock,
    JobRunService,
    EnvironmentService,
    ScriptedProbeProvider,
    Path,
]


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[Env]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    insert_template(connection)
    clock = VirtualClock(T0)
    try:
        yield (
            connection,
            clock,
            JobRunService(connection, clock),
            EnvironmentService(connection, clock),
            ScriptedProbeProvider(),
            tmp_path / "data",
        )
    finally:
        connection.close()


def test_startup_probe_restores_busy_environment_to_ready(
    env: Env,
) -> None:
    connection, clock, runs, service, provider, data_root = env
    make_env(service)
    crash_leave(connection, ENV_ID, EnvironmentState.BUSY)
    run_id = prepare_run(runs, key="run-key")
    provider.script(ENV_ID, [EnvironmentState.READY])

    supervisor_for(connection, clock, provider, data_root).startup()

    assert runs.get(run_id).state is RunState.QUEUED
    assert service.get(ENV_ID).state is EnvironmentState.READY
    assert provider.probe_calls[ENV_ID] == 1
    assert env_transitions(connection, ENV_ID) == [("busy", "ready")]


def test_startup_probe_marks_environment_failed(
    env: Env,
) -> None:
    connection, clock, _runs, service, provider, data_root = env
    make_env(service)
    crash_leave(connection, ENV_ID, EnvironmentState.BOOTING)
    provider.script(ENV_ID, [EnvironmentState.FAILED])

    supervisor_for(connection, clock, provider, data_root).startup()

    assert service.get(ENV_ID).state is EnvironmentState.FAILED
    assert provider.probe_calls[ENV_ID] == 1
    assert env_transitions(connection, ENV_ID) == [("booting", "failed")]


def test_startup_probe_timeout_marks_unknown(
    env: Env,
) -> None:
    connection, clock, _runs, service, provider, data_root = env
    make_env(service)
    crash_leave(connection, ENV_ID, EnvironmentState.BUSY)
    provider.script(ENV_ID, [TimeoutError("探针超时")])

    supervisor_for(connection, clock, provider, data_root).startup()

    assert service.get(ENV_ID).state is EnvironmentState.UNKNOWN
    assert provider.probe_calls[ENV_ID] == 1
    assert env_transitions(connection, ENV_ID) == [("busy", "unknown")]


def test_startup_probes_degraded_environment_to_unknown(
    env: Env,
) -> None:
    connection, clock, _runs, service, provider, data_root = env
    make_env(service)
    crash_leave(connection, ENV_ID, EnvironmentState.DEGRADED)
    provider.script(ENV_ID, [TimeoutError("探针超时")])

    supervisor_for(connection, clock, provider, data_root).startup()

    assert service.get(ENV_ID).state is EnvironmentState.UNKNOWN
    assert provider.probe_calls[ENV_ID] == 1
    assert env_transitions(connection, ENV_ID) == [("degraded", "unknown")]


def test_unknown_environment_waits_next_startup_not_ticks(
    env: Env,
) -> None:
    connection, clock, _runs, service, provider, data_root = env
    make_env(service)
    crash_leave(connection, ENV_ID, EnvironmentState.BUSY)
    provider.script(ENV_ID, [TimeoutError("探针超时")])
    supervisor = supervisor_for(connection, clock, provider, data_root)

    supervisor.startup()
    assert provider.probe_calls[ENV_ID] == 1

    clock.jump_to(T0 + timedelta(minutes=10))
    for _ in range(3):
        supervisor.tick()

    assert provider.probe_calls[ENV_ID] == 1
    assert service.get(ENV_ID).state is EnvironmentState.UNKNOWN


def test_consecutive_unknown_recovers_on_later_probe(
    env: Env,
) -> None:
    connection, clock, runs, service, provider, data_root = env
    make_env(service)
    crash_leave(connection, ENV_ID, EnvironmentState.BUSY)
    run_id = prepare_run(runs, key="run-key")
    provider.script(
        ENV_ID,
        [TimeoutError("超时一"), TimeoutError("超时二"), EnvironmentState.BOOTING],
    )
    supervisor = supervisor_for(connection, clock, provider, data_root)

    supervisor.startup()
    supervisor.startup()
    assert service.get(ENV_ID).state is EnvironmentState.UNKNOWN
    assert provider.probe_calls[ENV_ID] == 2
    assert env_transitions(connection, ENV_ID) == [("busy", "unknown")]

    supervisor.startup()
    assert service.get(ENV_ID).state is EnvironmentState.BOOTING
    assert ("unknown", "booting") in env_transitions(connection, ENV_ID)

    supervisor.tick()
    assert runs.get(run_id).state is RunState.SUCCEEDED
    assert ("booting", "ready") in env_transitions(connection, ENV_ID)


def test_startup_probe_once_per_environment(
    env: Env,
) -> None:
    connection, clock, runs, service, provider, data_root = env
    for env_id in (ENV_ID, "env-idle", "env-ready", "env-off"):
        make_env(service, env_id)
    run_a = recovering_run(runs, key="run-a")
    run_b = recovering_run(runs, key="run-b")
    crash_leave(connection, ENV_ID, EnvironmentState.BUSY)
    crash_leave(connection, "env-idle", EnvironmentState.UNKNOWN)
    crash_leave(connection, "env-ready", EnvironmentState.READY)
    crash_leave(connection, "env-off", EnvironmentState.OFF)
    provider.script(ENV_ID, [EnvironmentState.READY])
    provider.script("env-idle", [EnvironmentState.OFF])

    supervisor_for(connection, clock, provider, data_root).startup()

    assert runs.get(run_a).state is RunState.QUEUED
    assert runs.get(run_b).state is RunState.QUEUED
    assert provider.probe_calls.get(ENV_ID) == 1
    assert provider.probe_calls.get("env-idle") == 1
    assert "env-ready" not in provider.probe_calls
    assert "env-off" not in provider.probe_calls
