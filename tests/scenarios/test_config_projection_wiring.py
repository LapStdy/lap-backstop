"""场景：配置投影接线——开跑前构造并下发整份目标，失败回滚后按恢复策略收口。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    ApplyResult,
    ConfigDiff,
    ConfigProjection,
    RunContext,
    RunState,
    ScriptHandle,
    ScriptRef,
)
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.errors import PluginError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.worker.executor import WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"
REVISION = "20260101-120000"
SHA = "sha-fake-target"

# instant 场景一次成功运行的事件序列基线：投影缺失或关闭时逐条不变。
BASELINE_EVENT_SEQUENCE: list[str] = [
    "run.created",
    "run.state.changed",
    "run.queued",
    "resource.leased",
    "run.state.changed",
    "run.phase.started",
    "run.phase.finished",
    "run.state.changed",
    "run.state.changed",
    "run.phase.started",
    "run.signal",
    "run.phase.finished",
    "run.state.changed",
    "run.phase.started",
    "evidence",
    "run.verdict",
    "run.phase.finished",
    "run.state.changed",
    "run.phase.started",
    "run.phase.finished",
    "resource.released",
    "run.state.changed",
    "run.finished",
]


def insert_template(
    connection: sqlite3.Connection,
    scenario: str,
    *,
    config: dict[str, Any] | None = None,
    recovery: dict[str, object] | None = None,
) -> None:
    if config is None:
        config = {"scenario": scenario}
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, ?, 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps(config),
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


def prepare_run(connection: sqlite3.Connection, runs: JobRunService) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=T0,
            created_at=T0,
            idempotency_key="run-key",
            priority=0,
            deadline_at=None,
        )
    )
    runs.enqueue(record.id)
    return record.id


@dataclass(frozen=True, slots=True)
class ExecutorStubs:
    """make_executor 的假件注入：缺省用各自的 Fake 实现。"""

    script: FakeScriptAdapter | None = None
    projection: ConfigProjection | None = None


def make_executor(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    stubs: ExecutorStubs,
) -> WorkerExecutor:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter() if stubs.script is None else stubs.script,
            verifier=FakeVerifier(),
            config_projection=stubs.projection,
        )
    )
    return WorkerExecutor(connection, clock, registry, data_root=data_root)


class RecordingProjection:
    """按时间线记录调用的假投影：apply 与 rollback 按构造的回答，可抛通道异常。"""

    def __init__(
        self,
        timeline: list[str],
        result: ApplyResult | None = None,
        error: Exception | None = None,
        rollback_error: Exception | None = None,
    ) -> None:
        self.timeline = timeline
        self.result = result if result is not None else _applied_result()
        self.error = error
        self.rollback_error = rollback_error
        self.rolled_back: list[str] = []

    def desired(self, ctx: RunContext, config: Mapping[str, Any]) -> Mapping[str, Any]:
        """记录并返回一份内核不解释的目标。"""
        self.timeline.append("desired")
        return {"Current": "Default", "Configurations": {"Default": {"TaskQueue": []}}}

    def apply(self, ctx: RunContext, desired: Mapping[str, Any]) -> ApplyResult:
        """记录并按构造返回结果或抛通道异常。"""
        self.timeline.append("apply")
        if self.error is not None:
            raise self.error
        return self.result

    def rollback(self, ctx: RunContext, revision: str) -> ApplyResult:
        """记录回滚的修订号并按构造返回或抛通道异常。"""
        self.timeline.append("rollback")
        self.rolled_back.append(revision)
        if self.rollback_error is not None:
            raise self.rollback_error
        return ApplyResult(ok=True, detail=f"已恢复到修订 {revision}")

    def diff(
        self,
        ctx: RunContext,
        desired: Mapping[str, Any],
        actual: Mapping[str, Any],
    ) -> list[ConfigDiff]:
        """接线不经过 diff；返回空差异即可满足协议。"""
        return []


class RecordingScript(FakeScriptAdapter):
    """在假脚本的 launch 上加一条时间线记录，与投影调用共用同一序列。"""

    def __init__(self, timeline: list[str]) -> None:
        super().__init__()
        self._timeline = timeline

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        """记录 launch 后按 fake 场景启动。"""
        self._timeline.append("launch")
        return super().launch(script, ctx)


def _applied_result() -> ApplyResult:
    """成功回执：修订、哈希与 changed 齐全。"""
    return ApplyResult(
        ok=True,
        meta={"revision": REVISION, "sha256": SHA, "changed": True},
    )


def run_events(connection: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    """运行事件流：type、seq 与载荷，按 seq 升序。"""
    rows = connection.execute(
        "SELECT type, seq, payload FROM run_events WHERE run_id = ? ORDER BY seq",
        (run_id,),
    ).fetchall()
    return [
        {"type": str(row["type"]), "seq": int(row["seq"]), "payload": json.loads(row["payload"])}
        for row in rows
    ]


def events_of_type(
    connection: sqlite3.Connection, run_id: str, event_type: str
) -> list[dict[str, Any]]:
    """某类事件的载荷列表。"""
    return [event for event in run_events(connection, run_id) if event["type"] == event_type]


def test_projection_applies_before_script_launch(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """现状 1：desired → apply → launch 有序，config.applied 一条且先于脚本信号。"""
    connection, clock, runs, data_root = env
    insert_template(connection, "instant")
    run_id = prepare_run(connection, runs)
    timeline: list[str] = []
    executor = make_executor(
        connection,
        clock,
        data_root,
        ExecutorStubs(script=RecordingScript(timeline), projection=RecordingProjection(timeline)),
    )
    result = executor.execute(run_id)
    assert result.state is RunState.SUCCEEDED
    assert timeline == ["desired", "apply", "launch"]
    applied = events_of_type(connection, run_id, "config.applied")
    assert len(applied) == 1
    assert applied[0]["payload"] == {
        "run_id": run_id,
        "plugin_id": "fake",
        "revision": REVISION,
        "sha256": SHA,
        "changed": True,
    }
    signal_seqs = [
        event["seq"] for event in run_events(connection, run_id) if event["type"] == "run.signal"
    ]
    assert signal_seqs and applied[0]["seq"] < min(signal_seqs)


def test_plugin_without_projection_keeps_event_sequence(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """现状 1 反面：无投影能力的 fake 插件不产生 config.*，事件序列与改动前逐条相同。"""
    connection, clock, runs, data_root = env
    insert_template(connection, "instant")
    run_id = prepare_run(connection, runs)
    executor = make_executor(connection, clock, data_root, ExecutorStubs())
    result = executor.execute(run_id)
    assert result.state is RunState.SUCCEEDED
    types = [event["type"] for event in run_events(connection, run_id)]
    assert types == BASELINE_EVENT_SEQUENCE
    assert all(not event_type.startswith("config.") for event_type in types)


def test_apply_failure_rolls_back_and_skips_launch(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """现状 2：ok=false 时回滚一次并记 config.rolled_back，脚本不 launch，按策略收口。"""
    connection, clock, runs, data_root = env
    insert_template(connection, "instant")
    run_id = prepare_run(connection, runs)
    timeline: list[str] = []
    projection = RecordingProjection(
        timeline,
        result=ApplyResult(ok=False, error_code="E_CONFIG_LOCKED", meta={"revision": REVISION}),
    )
    executor = make_executor(
        connection,
        clock,
        data_root,
        ExecutorStubs(script=RecordingScript(timeline), projection=projection),
    )
    result = executor.execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_CONFIG_LOCKED"
    assert timeline == ["desired", "apply", "rollback"]
    assert projection.rolled_back == [REVISION]
    assert events_of_type(connection, run_id, "config.applied") == []
    rolled = events_of_type(connection, run_id, "config.rolled_back")
    assert len(rolled) == 1
    assert rolled[0]["payload"] == {
        "run_id": run_id,
        "revision": REVISION,
        "ok": True,
        "error_code": None,
    }
    assert "launch" not in projection.timeline


def test_rollback_channel_error_keeps_apply_error_code(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """收口补充：回滚再抛通道异常时记失败回执，运行错误码仍是 apply 的码。"""
    connection, clock, runs, data_root = env
    insert_template(connection, "instant")
    run_id = prepare_run(connection, runs)
    timeline: list[str] = []
    projection = RecordingProjection(
        timeline,
        result=ApplyResult(ok=False, error_code="E_CONFIG_LOCKED", meta={"revision": REVISION}),
        rollback_error=PluginError("回滚通道故障"),
    )
    executor = make_executor(
        connection,
        clock,
        data_root,
        ExecutorStubs(script=RecordingScript(timeline), projection=projection),
    )
    result = executor.execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_CONFIG_LOCKED"
    assert timeline == ["desired", "apply", "rollback"]
    rolled = events_of_type(connection, run_id, "config.rolled_back")
    assert len(rolled) == 1
    assert rolled[0]["payload"]["ok"] is False
    assert rolled[0]["payload"]["error_code"] == "E_PLUGIN"


def test_apply_failure_without_revision_skips_rollback(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """现状 2 反面：没有合法修订不回滚也不记事件，错误码缺省 E_CONFIG。"""
    connection, clock, runs, data_root = env
    insert_template(connection, "instant")
    run_id = prepare_run(connection, runs)
    timeline: list[str] = []
    projection = RecordingProjection(
        timeline,
        result=ApplyResult(ok=False, error_code=None, meta={}),
    )
    executor = make_executor(
        connection,
        clock,
        data_root,
        ExecutorStubs(script=RecordingScript(timeline), projection=projection),
    )
    result = executor.execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_CONFIG"
    assert timeline == ["desired", "apply"]
    assert projection.rolled_back == []
    assert events_of_type(connection, run_id, "config.rolled_back") == []


def test_disabled_projection_skips_all_calls(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """现状 3：开关关闭时不调 desired 与 apply，不写 config.*，脚本照常 launch。"""
    connection, clock, runs, data_root = env
    insert_template(
        connection,
        "instant",
        config={"scenario": "instant", "config_projection": {"enabled": False}},
    )
    run_id = prepare_run(connection, runs)
    timeline: list[str] = []
    executor = make_executor(
        connection,
        clock,
        data_root,
        ExecutorStubs(script=RecordingScript(timeline), projection=RecordingProjection(timeline)),
    )
    result = executor.execute(run_id)
    assert result.state is RunState.SUCCEEDED
    assert timeline == ["launch"]
    types = [event["type"] for event in run_events(connection, run_id)]
    assert types == BASELINE_EVENT_SEQUENCE
    assert all(not event_type.startswith("config.") for event_type in types)


def test_projection_channel_error_emits_no_applied_event(
    env: tuple[sqlite3.Connection, VirtualClock, JobRunService, Path],
) -> None:
    """现状 5：通道异常走 ok=false 同一收口，不写 config.applied，租约已释放。"""
    connection, clock, runs, data_root = env
    insert_template(connection, "instant")
    run_id = prepare_run(connection, runs)
    timeline: list[str] = []
    projection = RecordingProjection(timeline, error=PluginError("配置通道故障"))
    executor = make_executor(
        connection,
        clock,
        data_root,
        ExecutorStubs(script=RecordingScript(timeline), projection=projection),
    )
    result = executor.execute(run_id)
    assert result.state is RunState.FAILED
    assert result.error_code == "E_PLUGIN"
    assert timeline == ["desired", "apply"]
    assert events_of_type(connection, run_id, "config.applied") == []
    assert events_of_type(connection, run_id, "config.rolled_back") == []
    leases = connection.execute(
        "SELECT COUNT(*) FROM resource_leases WHERE run_id = ? AND state != 'released'",
        (run_id,),
    ).fetchone()
    assert leases is not None and int(leases[0]) == 0
