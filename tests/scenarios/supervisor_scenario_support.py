"""test_supervisor 系列场景的共享脚手架：种子行、假插件注册、env 夹具与运行预备。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.api.store import TOKEN_KEY, put_settings
from core.clock import VirtualClock
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.supervisor import Supervisor
from plugins.fake.plugin import FakeEnvironmentProvider, FakeScriptAdapter, FakeVerifier
from storage import migrate, open_database
from storage.repositories.job_runs import NewRun

T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
TOKEN = "secret-token"

_STABLE_DISK_FREE = 100 * 1024**3
TEMPLATE_ID = "template-1"
ENV_ID = "env-1"


def insert_template(connection: sqlite3.Connection) -> None:
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
    put_settings(connection, {TOKEN_KEY: TOKEN}, at=T0)
    clock = VirtualClock(T0)
    runs = JobRunService(connection, clock)
    data_root = tmp_path
    try:
        yield connection, clock, runs, data_root
    finally:
        connection.close()


def supervisor_for(
    connection: sqlite3.Connection,
    clock: VirtualClock,
    data_root: Path,
) -> Supervisor:
    return Supervisor(
        connection, clock, make_registry(), data_root=data_root, disk_free=lambda: _STABLE_DISK_FREE
    )


@dataclass(frozen=True, slots=True)
class RunOverrides:
    """prepare_run 的 NewRun 字段覆盖：默认无调度、按 T0 整点、无截止、键 run-key。"""

    schedule_id: str | None = None
    scheduled_at: datetime = T0
    deadline_at: datetime | None = None
    key: str = "run-key"


_DEFAULT_OVERRIDES = RunOverrides()


def prepare_run(
    connection: sqlite3.Connection,
    runs: JobRunService,
    overrides: RunOverrides = _DEFAULT_OVERRIDES,
) -> str:
    record, _ = runs.create_scheduled(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=overrides.schedule_id,
            scheduled_at=overrides.scheduled_at,
            created_at=T0,
            idempotency_key=overrides.key,
            priority=0,
            deadline_at=overrides.deadline_at,
        )
    )
    return record.id
