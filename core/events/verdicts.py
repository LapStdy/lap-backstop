"""结论服务：核对证据引用与模板要求、落库 Verdict 并回填到运行。"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import replace
from typing import Any
from uuid import uuid4

from contracts import Clock, EventType, EvidenceKind, Verdict, VerdictStatus
from core.jobs.errors import RunNotFoundError
from storage.db.transaction import transaction
from storage.repositories.events import EventLog
from storage.repositories.evidence import EvidenceRepository
from storage.repositories.job_runs import JobRunRepository
from storage.repositories.job_templates import JobTemplateRepository
from storage.repositories.verdicts import VerdictRepository, VerdictRow


class VerdictError(ValueError):
    """结论校验失败，携带错误码与运行收尾原因（run.state.changed.reason）。"""

    code = "E_VERIFY_NO_EVIDENCE"
    reason = "verify_no_evidence"


class MissingEvidenceError(VerdictError):
    """结论引用了不存在的证据，对应错误码 E_VERIFY_NO_EVIDENCE。"""


class VerdictPolicyError(VerdictError):
    """模板的验证策略不完整，对应错误码 E_CONFIG。"""

    code = "E_CONFIG"
    reason = "verify_no_policy"


class VerdictService:
    """结论落库；内核不信任插件给的证据引用与策略标识，先与证据表、模板核对。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock) -> None:
        self._connection = connection
        self._clock = clock
        self._verdicts = VerdictRepository(connection)
        self._runs = JobRunRepository(connection)
        self._templates = JobTemplateRepository(connection)
        self._evidence = EvidenceRepository(connection)
        self._events = EventLog(connection)

    def record(self, run_id: str, verdict: Verdict) -> tuple[str, Verdict]:
        """核对并写入结论，返回结论 id 与内核改写后的结论。

        Raises:
            RunNotFoundError: 运行不存在。
            MissingEvidenceError: 没有证据，或引用了不属于该运行的证据。
            VerdictPolicyError: 模板的验证策略缺少 id，或 required_evidence 不是合法证据种类数组。
        """
        run = self._runs.get(run_id)
        if run is None:
            raise RunNotFoundError(f"JobRun 不存在：{run_id}")
        template = self._templates.get(run.template_id)
        if template is None:
            raise VerdictPolicyError(f"作业模板不存在：{run.template_id}")
        effective = self._checked(run_id, verdict, template.verification_policy)
        verdict_id = uuid4().hex
        now = self._clock.utc_now()
        with transaction(self._connection):
            self._verdicts.insert(
                VerdictRow(
                    id=verdict_id,
                    run_id=run_id,
                    status=effective.status,
                    confidence=effective.confidence,
                    reason=effective.reason,
                    policy_id=effective.policy_id,
                    checks=tuple(
                        {
                            "name": check.name,
                            "status": check.status.value,
                            "detail": check.detail,
                            "evidence_ids": list(check.evidence_ids),
                        }
                        for check in effective.checks
                    ),
                    evidence_ids=effective.evidence_ids,
                    created_at=now,
                )
            )
            self._runs.set_verdict(run_id, verdict_id)
            self._events.append_run(
                run_id,
                EventType.RUN_VERDICT,
                {
                    "run_id": run_id,
                    "status": effective.status.value,
                    "policy_id": effective.policy_id,
                    "reason": effective.reason,
                    "confidence": effective.confidence,
                    "evidence_ids": list(effective.evidence_ids),
                },
                at=now,
            )
        return verdict_id, effective

    def _checked(self, run_id: str, verdict: Verdict, policy: Mapping[str, Any]) -> Verdict:
        """核对证据引用与模板要求，返回回填策略 id 后的结论。"""
        policy_id, required = _verification_policy(policy)
        if not verdict.evidence_ids:
            raise MissingEvidenceError("结论缺少证据")
        known = self._evidence.known_ids(run_id)
        referenced = set(verdict.evidence_ids)
        for check in verdict.checks:
            referenced.update(check.evidence_ids)
        unknown = referenced - known
        if unknown:
            raise MissingEvidenceError(f"结论引用了不存在的证据：{sorted(unknown)}")
        recorded = {row.kind for row in self._evidence.for_run(run_id)}
        missing = tuple(kind for kind in required if kind not in recorded)
        if not missing:
            return replace(verdict, policy_id=policy_id)
        kinds = "、".join(kind.value for kind in missing)
        return replace(
            verdict,
            status=VerdictStatus.UNKNOWN,
            policy_id=policy_id,
            reason=f"{verdict.reason}（缺少证据种类：{kinds}）",
        )


def _verification_policy(policy: Mapping[str, Any]) -> tuple[str, tuple[EvidenceKind, ...]]:
    """读出策略 id 与内核强制核对的证据种类；required_evidence 缺省或为空表示不强制。"""
    policy_id = policy.get("id")
    if not isinstance(policy_id, str) or not policy_id:
        raise VerdictPolicyError("验证策略缺少 id")
    raw = policy.get("required_evidence")
    if raw is None:
        return policy_id, ()
    if not isinstance(raw, (list, tuple)):
        raise VerdictPolicyError(f"验证策略 required_evidence 不是数组：{raw!r}")
    kinds: list[EvidenceKind] = []
    for item in raw:
        try:
            kinds.append(EvidenceKind(str(item)))
        except ValueError as error:
            raise VerdictPolicyError(f"未知证据种类：{item!r}") from error
    return policy_id, tuple(kinds)


__all__ = ["MissingEvidenceError", "VerdictError", "VerdictPolicyError", "VerdictService"]
