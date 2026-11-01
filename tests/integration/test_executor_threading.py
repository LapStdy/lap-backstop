"""执行线程化：tick 派发不等待执行，连接归属执行线程，租约续租仍由 tick 驱动。

判据全部是外部可观测结果：终态、事件条数、日志事件、租约行与是否阻塞；线程同步
一律用事件（进入/放行）与可控的虚拟时钟，不用 sleep。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from contracts import RunContext, RunState, ScriptHandle, ScriptObservation, ScriptRef
from core.clock import VirtualClock
from core.jobs.phases import PhaseTracker
from core.jobs.run_service import JobRunService
from core.logging import JsonLineLogger
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from tests.supervisor_support import JOIN_TIMEOUT_SECONDS

T0 = datetime(2026, 10, 31, 8, 0, tzinfo=UTC)
TEMPLATE_A = "template-a"
TEMPLATE_B = "template-b"
TEMPLATE_C = "template-c"
SCHEDULE_C = "schedule-c"
ENV_A = "env-a"
ENV_B = "env-b"
ENV_C = "env-c"
_STABLE_DISK_FREE = 100 * 1024**3
# 门要活得比 join 上限久，等待执行线程收尾时不会把门先耗到超时；上限仍兜底红态。
_GATE_TIMEOUT = 60.0
_POLL_INTERVAL = 0.05


def _insert_template(
    connection: sqlite3.Connection,
    template_id: str,
    environment_id: str,
    *,
    recovery: dict[str, object] | None = None,
) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, ?, 1, ?, ?)",
        (
            template_id,
            environment_id,
            json.dumps({"scenario": "instant"}),
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


def _insert_schedule(connection: sqlite3.Connection, *, next_run_at: datetime) -> None:
    connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, window_minutes, "
        "coalesce, priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
        "job_template_id, enabled, next_run_at, created_at, updated_at) "
        "VALUES (?, '每小时', 'interval', '60', NULL, 'UTC', 'run_once', 120, 1, 0, 180, 0, 0, "
        "?, 1, ?, ?, ?)",
        (SCHEDULE_C, TEMPLATE_C, next_run_at.isoformat(), T0.isoformat(), T0.isoformat()),
    )


class _GatedScriptAdapter(FakeScriptAdapter):
    """指定环境的 observe 停到测试放行或抛未预期异常：进入与放行都有事件，同步不靠等待时长。"""

    def __init__(self, *, crash_environment_id: str | None = None) -> None:
        super().__init__()
        self._crash_environment_id = crash_environment_id
        self.entered: dict[str, threading.Event] = {}
        self._release: dict[str, threading.Event] = {}
        self.launch_counts: dict[str, int] = {}

    def gate(self, environment_id: str) -> threading.Event:
        """给环境装门，返回放行事件。"""
        self.entered[environment_id] = threading.Event()
        release = threading.Event()
        self._release[environment_id] = release
        return release

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        count = self.launch_counts.get(ctx.environment_id, 0)
        self.launch_counts[ctx.environment_id] = count + 1
        return super().launch(script, ctx)

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        if ctx.environment_id == self._crash_environment_id:
            raise RuntimeError("假适配器未预期异常")
        release = self._release.get(ctx.environment_id)
        if release is not None:
            self.entered[ctx.environment_id].set()
            assert release.wait(timeout=_GATE_TIMEOUT), "测试没有放行执行器"
        return super().observe(handle, ctx)


def _wait_until(condition: Callable[[], bool], *, timeout: float = _GATE_TIMEOUT) -> None:
    """轮询等异步线程的落库/日志完成；只兜 IO 时机，断言的正确性不靠等待时长。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(_POLL_INTERVAL)
    assert condition(), "条件在时限内没有成立"


_SupervisorFactory = Callable[..., tuple[Supervisor, "_GatedScriptAdapter"]]


@pytest.fixture
def env(
    tmp_path: Path,
) -> Iterator[tuple[sqlite3.Connection, VirtualClock, JobRunService, _SupervisorFactory]]:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    made: list[Supervisor] = []

    def make(
        *,
        log: JsonLineLogger | None = None,
        crash_environment_id: str | None = None,
    ) -> tuple[Supervisor, _GatedScriptAdapter]:
        adapter = _GatedScriptAdapter(crash_environment_id=crash_environment_id)
        registry = PluginRegistry()
        registry.register(
            PluginCapabilities(
                plugin_id="fake",
                environment=FakeEnvironmentProvider(),
                script=adapter,
                verifier=FakeVerifier(),
            )
        )
        supervisor = Supervisor(
            connection,
            clock,
            registry,
            data_root=tmp_path,
            log=log,
            disk_free=lambda: _STABLE_DISK_FREE,
        )
        made.append(supervisor)
        return supervisor, adapter

    try:
        yield connection, clock, runs, make
    finally:
        for supervisor in made:
            supervisor.join_executors(JOIN_TIMEOUT_SECONDS)
        connection.close()


def _create_run(
    runs: JobRunService,
    template_id: str,
    *,
    key: str,
    priority: int = 0,
    scheduled_at: datetime = T0,
    deadline_at: datetime | None = None,
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=template_id,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=priority,
            deadline_at=deadline_at,
        )
    )
    runs.enqueue(record.id)
    return record.id


def _lease_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT state, expires_at, renewed_at FROM resource_leases WHERE run_id = ?", (run_id,)
    ).fetchone()


def _renewed_at(row: sqlite3.Row) -> datetime | None:
    value = row["renewed_at"]
    return None if value is None else datetime.fromisoformat(str(value))


def _log_entries(log_path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_two_environments_run_concurrently_and_same_env_second_is_rejected(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, _SupervisorFactory],
    tmp_path: Path,
) -> None:
    connection, _clock, runs, make = env
    _insert_template(connection, TEMPLATE_A, ENV_A)
    _insert_template(connection, TEMPLATE_B, ENV_B)
    log_path = tmp_path / "logs" / "core.jsonl"
    supervisor, adapter = make(log=JsonLineLogger(log_path))
    first = _create_run(runs, TEMPLATE_A, key="env-a-1", priority=10)
    second = _create_run(runs, TEMPLATE_B, key="env-b-1")
    release_a = adapter.gate(ENV_A)
    release_b = adapter.gate(ENV_B)

    supervisor.tick()
    assert adapter.entered[ENV_A].wait(_GATE_TIMEOUT), "第一个运行没有进入执行"
    supervisor.tick()
    assert adapter.entered[ENV_B].wait(_GATE_TIMEOUT), "第二个运行没有进入执行"
    assert runs.get(first).state is RunState.EXECUTING
    assert runs.get(second).state is RunState.EXECUTING

    third = _create_run(runs, TEMPLATE_A, key="env-a-2")
    supervisor.tick()

    def third_rejected() -> bool:
        return any(
            entry.get("event") == "supervisor.resource_busy" and entry.get("run_id") == third
            for entry in _log_entries(log_path)
        )

    _wait_until(third_rejected)
    assert runs.get(third).state is RunState.QUEUED, "同环境第二运行应被租约容量拒绝"
    assert "resource.leased" not in [event.type for event in runs.events(third)]

    release_a.set()
    release_b.set()
    supervisor.join_executors(JOIN_TIMEOUT_SECONDS)
    assert runs.get(first).state is RunState.SUCCEEDED
    assert runs.get(second).state is RunState.SUCCEEDED
    assert adapter.launch_counts == {ENV_A: 1, ENV_B: 1}


def test_run_executes_exactly_once_across_consecutive_ticks(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, _SupervisorFactory],
) -> None:
    connection, clock, runs, make = env
    _insert_template(connection, TEMPLATE_A, ENV_A)
    supervisor, adapter = make()
    run_id = _create_run(runs, TEMPLATE_A, key="once")
    release = adapter.gate(ENV_A)

    supervisor.tick()
    assert adapter.entered[ENV_A].wait(_GATE_TIMEOUT), "运行没有进入执行"
    for _ in range(3):
        supervisor.tick()
    release.set()
    supervisor.join_executors(JOIN_TIMEOUT_SECONDS)

    record = runs.get(run_id)
    assert record.state is RunState.SUCCEEDED
    assert adapter.launch_counts == {ENV_A: 1}, f"适配器被多次调用：{adapter.launch_counts}"
    phases = PhaseTracker(connection, clock).for_run(run_id)
    assert sorted(row.phase for row in phases) == ["cleanup", "execute", "prepare", "verify"]
    event_types = [event.type for event in runs.events(run_id)]
    assert not any(event_type.startswith("recovery.") for event_type in event_types)
    residual = connection.execute(
        "SELECT COUNT(*) FROM events_global WHERE payload LIKE '%supervisor.executor_failed%'"
    ).fetchone()
    assert residual is not None and int(residual[0]) == 0, "执行线程兜底路径被触发"

