"""停机收尾：阶段边界写 run.interrupted 并释放租约，不把运行推进终态。"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

from contracts import RunContext, RunState, ScriptHandle, ScriptObservation, ScriptState
from core.clock import VirtualClock
from core.hostops.serve import run_serve
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.resources.service import LeaseService
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.resources import ResourceRepository
from tests.integration.test_serve_api import configure, serve_options

T0 = datetime(2026, 10, 30, 3, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
_STABLE_DISK_FREE = 100 * 1024**3


def _insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": "instant"}),
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


def _create_run(runs: JobRunService, *, scheduled_at: datetime, key: str) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=scheduled_at,
            created_at=T0,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    return record.id


class _ShutdownOnObserveScript(FakeScriptAdapter):
    """第一次观察时置位停机事件，模拟 executing 阶段收到停机信号。"""

    def __init__(self, stop: threading.Event) -> None:
        super().__init__()
        self._stop = stop

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        self._stop.set()
        return ScriptObservation(state=ScriptState.RUNNING, since=ctx.clock.utc_now())


def test_shutdown_during_execution_writes_interrupted_and_releases_lease(
    tmp_path: Path,
) -> None:
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    try:
        _insert_template(connection)
        run_id = _create_run(runs, scheduled_at=T0, key="shutdown-drain")
        runs.enqueue(run_id)
        stop = threading.Event()
        registry = PluginRegistry()
        registry.register(
            PluginCapabilities(
                plugin_id="fake",
                environment=FakeEnvironmentProvider(),
                script=_ShutdownOnObserveScript(stop),
                verifier=FakeVerifier(),
            )
        )
        supervisor = Supervisor(
            connection,
            clock,
            registry,
            data_root=tmp_path,
            disk_free=lambda: _STABLE_DISK_FREE,
            shutdown=stop.is_set,
        )
        supervisor.tick()

        record = runs.get(run_id)
        assert record.state is RunState.EXECUTING, f"停机后运行状态被改写：{record.state.value}"
        assert record.finished_at is None
        events = runs.events(run_id)
        interrupted = [event for event in events if event.type == "run.interrupted"]
        assert len(interrupted) == 1, f"run.interrupted 条数不对：{[e.type for e in events]}"
        assert interrupted[0].payload == {"run_id": run_id, "reason": "shutdown"}
        assert not runs.cancel_requested(run_id), "停机收尾被当成用户取消"
        assert LeaseService(connection, clock).active_for_run(run_id) == ()
        released = [event for event in events if event.type == "resource.released"]
        assert [event.payload["reason"] for event in released] == ["shutdown"]
        assert not any(event.type == "run.finished" for event in events)
    finally:
        connection.close()


def _waiting_run_with_lease(data_root: Path) -> str:
    """建一条等待到点、持有租约的运行：停机时没有执行中的运行。"""
    connection = open_database(data_root / "backstop.sqlite")
    clock = VirtualClock(T0)
    try:
        _insert_template(connection)
        runs = JobRunService(connection, clock)
        run_id = _create_run(runs, scheduled_at=T0 + timedelta(hours=1), key="shutdown-idle")
        runs.enqueue(run_id)
        runs.transition(run_id, RunState.PREPARING)
        ResourceRepository(connection).ensure(ENV_ID, resource_type="environment", name=ENV_ID)
        LeaseService(connection, clock).acquire(run_id, (ENV_ID,))
        runs.transition(run_id, RunState.WAITING)
        return run_id
    finally:
        connection.close()


def test_shutdown_without_active_run_exits_zero_and_clears_leases(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    configure(data_root, {"api.host": "127.0.0.1", "api.port": 0})
    run_id = _waiting_run_with_lease(data_root)
    stop = threading.Event()
    stop.set()
    code = run_serve(data_root, serve_options(clock=VirtualClock(T0), stop=stop))
    assert code == 0
    reader = sqlite3.connect(data_root / "backstop.sqlite")
    try:
        active = reader.execute(
            "SELECT COUNT(*) FROM resource_leases WHERE state IN ('leased', 'expiring')"
        ).fetchone()
        interrupted = reader.execute(
            "SELECT COUNT(*) FROM run_events WHERE type = 'run.interrupted'"
        ).fetchone()
        released = reader.execute(
            "SELECT payload FROM run_events WHERE run_id = ? AND type = 'resource.released'",
            (run_id,),
        ).fetchall()
        state = reader.execute("SELECT state FROM job_runs WHERE id = ?", (run_id,)).fetchone()
    finally:
        reader.close()
    assert active is not None and int(active[0]) == 0
    assert interrupted is not None and int(interrupted[0]) == 0
    assert [json.loads(str(row[0]))["reason"] for row in released] == ["shutdown"]
    assert state is not None and str(state[0]) == "waiting"
