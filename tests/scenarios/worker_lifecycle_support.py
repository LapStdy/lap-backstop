"""worker 执行器生命周期场景的共享脚手架：运行预备、假插件注册与执行器装配。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from contracts import Logger, PhaseState, RunContext, Verdict
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.errors import (
    WorkerCallError,
    WorkerCancelledError,
    WorkerCrashError,
    WorkerTimeoutError,
)
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.worker.executor import CancelFlag, ExecutorOptions, WorkerExecutor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun
from storage.repositories.run_phases import RunPhaseRepository

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"


DEFAULT_POLICY: Mapping[str, Any] = {
    "id": "fake-policy",
    "verifier_plugin_id": "fake",
    "required_evidence": ["log"],
    "params": {},
}


def insert_template(
    connection: sqlite3.Connection,
    scenario: str,
    *,
    policy: Mapping[str, Any] | None = None,
) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'fake', ?, ?, '1', ?, '{}', 1, ?, ?)",
        (
            TEMPLATE_ID,
            ENV_ID,
            json.dumps({"scenario": scenario}),
            json.dumps(DEFAULT_POLICY if policy is None else policy),
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


@dataclass(frozen=True, slots=True)
class RunOverrides:
    """prepare_run 的字段覆盖：默认按 T0 整点、无截止、锚定键与模板默认策略。"""

    scheduled_at: datetime = T0
    deadline_at: datetime | None = None
    key: str = "schedule-1:2026-10-23T12:00:00+00:00"
    policy: Mapping[str, Any] | None = None


_DEFAULT_OVERRIDES = RunOverrides()


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    scenario: str,
    overrides: RunOverrides = _DEFAULT_OVERRIDES,
) -> str:
    insert_template(connection, scenario, policy=overrides.policy)
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=overrides.scheduled_at,
            created_at=T0,
            idempotency_key=overrides.key,
            priority=0,
            deadline_at=overrides.deadline_at,
        )
    )
    runs.enqueue(record.id)
    return record.id


@dataclass(frozen=True, slots=True)
class ExecutorStubOptions:
    """executor_for 的可选注入：取消令牌与日志；默认无。"""

    cancel: CancelFlag | None = None
    log: Logger | None = None


_DEFAULT_EXECUTOR_OPTIONS = ExecutorStubOptions()


def executor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
    options: ExecutorStubOptions = _DEFAULT_EXECUTOR_OPTIONS,
) -> WorkerExecutor:
    return WorkerExecutor(
        connection,
        clock,
        make_registry(),
        ExecutorOptions(data_root=data_root, cancel=options.cancel, log=options.log),
    )


def phases(connection: sqlite3.Connection, run_id: str) -> dict[str, PhaseState]:
    rows = RunPhaseRepository(connection).for_run(run_id)
    return {row.phase: row.state for row in rows}


class _RaisingVerifier:
    """验证阶段抛出指定异常，用来覆盖跨进程错误到运行终态的映射。"""

    def __init__(
        self,
        error: WorkerTimeoutError | WorkerCancelledError | WorkerCrashError | WorkerCallError,
    ) -> None:
        self._error = error

    def verify(self, ctx: RunContext) -> Verdict:
        raise self._error


def _registry_with(
    *,
    environment: FakeEnvironmentProvider | None = None,
    script: FakeScriptAdapter | None = None,
    verifier: Any | None = None,
) -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider() if environment is None else environment,
            script=FakeScriptAdapter() if script is None else script,
            verifier=FakeVerifier() if verifier is None else verifier,
        )
    )
    return registry
