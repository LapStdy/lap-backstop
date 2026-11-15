"""装配点把模板验证策略原样送进 RunContext（ADR 0016 拆卡第 2 步）。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from contracts import RunState
from core.clock import VirtualClock
from core.logging import NullLogger
from core.worker.executor_scope import RunScopeFactory
from core.worker.run_scope import CancelFlag, RunScope
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRow

if TYPE_CHECKING:
    from core.plugins.registry import PluginRegistry
    from core.worker.executor import ExecutorServices

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=UTC)

POLICY = {"id": "p-1", "params": {"mode": "screen"}}


class _Registry:
    """能力解析桩：本文件只关心上下文装配，不关心能力对象。"""

    def environment(self, plugin_id: str) -> object:
        return object()

    def script(self, plugin_id: str) -> object:
        return object()

    def verifier(self, plugin_id: str) -> object:
        return object()


class _Resources:
    def ensure(self, resource_id: str, spec: object) -> None:
        return None


class _Environments:
    def register(self, environment_id: str, *, provider_id: str, name: str) -> None:
        return None


def make_run() -> JobRunRecord:
    return JobRunRecord(
        id="run-1",
        template_id="template-1",
        schedule_id=None,
        scheduled_at=None,
        created_at=NOW,
        idempotency_key="idem-1",
        state=RunState.EXECUTING,
        attempt=1,
        priority=0,
        deadline_at=NOW + timedelta(minutes=30),
        started_at=None,
        finished_at=None,
        verdict_id=None,
        error_code=None,
        detail={},
    )


def make_template(verification_policy: Mapping[str, Any]) -> JobTemplateRow:
    return JobTemplateRow(
        id="template-1",
        name="模板",
        plugin_id="fake",
        environment_id="env-1",
        config={"scenario": "instant"},
        config_schema_version="1",
        verification_policy=verification_policy,
        recovery_policy={},
        enabled=True,
    )


def make_scope(tmp_path: Path, verification_policy: Mapping[str, Any]) -> RunScope:
    """经 RunScopeFactory.begin 组装一次运行；事件与证据桩不被调用。"""
    services = cast(
        "ExecutorServices",
        SimpleNamespace(
            resources=_Resources(),
            environments=_Environments(),
            evidence=object(),
            events=object(),
            clock=VirtualClock(NOW),
            log=NullLogger(),
        ),
    )
    factory = RunScopeFactory(
        services,
        cast("PluginRegistry", _Registry()),
        tmp_path,
        CancelFlag(),
    )
    return factory.begin(make_run(), make_template(verification_policy))


def test_build_context_carries_template_policy(tmp_path: Path) -> None:
    scope = make_scope(tmp_path, POLICY)
    assert scope.context.verification_policy == {"id": "p-1", "params": {"mode": "screen"}}
    assert scope.context.verification_policy["params"] == {"mode": "screen"}


def test_build_context_history_empty_policy_defaults_to_empty_mapping(tmp_path: Path) -> None:
    scope = make_scope(tmp_path, {})
    assert scope.context.verification_policy == {}
