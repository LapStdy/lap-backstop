"""worker 执行器的运行范围组装：插件能力解析、环境登记与插件上下文。

一次执行可复用的状态都装进 RunScope（run_scope.py）；这里只做组装，不做阶段
判断。插件上下文的事件出口与证据写入经 core/plugins 与 core/events 的白名单
校验（docs/05 插件边界、docs/03 事件）。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from contracts import (
    CancelToken,
    EnvironmentRef,
    EvidenceKind,
    RunContext,
)
from core.environments.service import EnvironmentCall
from core.events.evidence import EvidenceDraft
from core.plugins import rpc
from core.worker.run_scope import RunScope
from storage.defaults import default
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRow
from storage.repositories.resources import ResourceSpec

if TYPE_CHECKING:
    from core.plugins.registry import PluginRegistry
    from core.worker.executor import ExecutorServices

_DEFAULT_DEADLINE = timedelta(minutes=int(default("worker", "deadline_minutes")))

Emit = Callable[[str, Mapping[str, Any]], None]
RecordEvidence = Callable[[str, Path, str], str]


class RunScopeFactory:
    """一次执行的运行范围组装：解析插件能力、登记环境并构建插件上下文。"""

    def __init__(
        self,
        services: ExecutorServices,
        registry: PluginRegistry,
        data_root: Path,
        cancel: CancelToken,
    ) -> None:
        self._resources = services.resources
        self._environments = services.environments
        self._evidence = services.evidence
        self._events = services.events
        self._clock = services.clock
        self._log = services.log
        self._registry = registry
        self._data_root = data_root
        self._cancel = cancel

    def begin(self, run: JobRunRecord, template: JobTemplateRow) -> RunScope:
        """组装一次执行用的插件、环境引用与运行上下文。"""
        provider = self._registry.environment(template.plugin_id)
        script = self._registry.script(template.plugin_id)
        verifier = self._registry.verifier(template.plugin_id)
        environment_id = template.environment_id
        self._resources.ensure(
            environment_id,
            ResourceSpec(resource_type="environment", name=environment_id),
        )
        self._environments.register(
            environment_id,
            provider_id=template.plugin_id,
            name=environment_id,
        )
        context = self._build_context(run, template)
        call = EnvironmentCall(
            environment_id=environment_id,
            provider=provider,
            ref=EnvironmentRef(id=environment_id, provider_id=template.plugin_id),
            ctx=context,
        )
        return RunScope(
            run=run,
            template=template,
            call=call,
            script=script,
            verifier=verifier,
            context=context,
        )

    def _build_context(self, run: JobRunRecord, template: JobTemplateRow) -> RunContext:
        """构建插件上下文：工作与证据目录落数据根，截止时间缺省取默认时限。"""
        work_dir = self._data_root / "work" / run.id
        evidence_dir = self._data_root / "evidence" / run.id
        work_dir.mkdir(parents=True, exist_ok=True)
        evidence_dir.mkdir(parents=True, exist_ok=True)
        now = self._clock.utc_now()
        deadline = run.deadline_at if run.deadline_at is not None else now + _DEFAULT_DEADLINE
        return RunContext(
            run_id=run.id,
            attempt=run.attempt,
            template_id=template.id,
            plugin_id=template.plugin_id,
            environment_id=template.environment_id,
            config=dict(template.config),
            work_dir=work_dir,
            evidence_dir=evidence_dir,
            deadline_at=deadline,
            cancel=self._cancel,
            clock=self._clock,
            secrets={},
            log=self._log,
            emit=self._emitter(run.id),
            record_evidence=self._evidence_recorder(run.id, evidence_dir),
            verification_policy=dict(template.verification_policy),
        )

    def _emitter(self, run_id: str) -> Emit:
        def emit(event: str, payload: Mapping[str, Any]) -> None:
            # 插件只能写 run.signal（docs/03）：与 worker 通道共用同一白名单校验。
            checked = rpc.plugin_event(event)
            rpc.check_plugin_payload(checked, payload)
            self._events.append_run(
                run_id,
                checked,
                dict(payload),
                at=self._clock.utc_now(),
            )

        return emit

    def _evidence_recorder(self, run_id: str, evidence_dir: Path) -> RecordEvidence:
        def record(kind: str, path: Path, summary: str) -> str:
            target = path if path.is_absolute() else evidence_dir / path
            return self._evidence.record(
                run_id,
                EvidenceDraft(kind=EvidenceKind(kind), path=target, summary=summary),
            )

        return record


__all__ = ["RunScopeFactory"]
