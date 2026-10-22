"""事件注册表：事件类型、作用域与载荷形状。

唯一真源是 docs/03 的 SSE 事件一节；内核与客户端都按本模块的取值收发。
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final, TypedDict


class EventType(StrEnum):
    RUN_CREATED = "run.created"
    RUN_QUEUED = "run.queued"
    RUN_STATE_CHANGED = "run.state.changed"
    RUN_PHASE_STARTED = "run.phase.started"
    RUN_PHASE_FINISHED = "run.phase.finished"
    RUN_SIGNAL = "run.signal"
    RUN_VERDICT = "run.verdict"
    RUN_FINISHED = "run.finished"
    RUN_INTERRUPTED = "run.interrupted"
    ENV_STATE_CHANGED = "env.state.changed"
    RESOURCE_LEASED = "resource.leased"
    RESOURCE_RENEWED = "resource.renewed"
    RESOURCE_RELEASED = "resource.released"
    RECOVERY_STARTED = "recovery.started"
    RECOVERY_FINISHED = "recovery.finished"
    EVIDENCE = "evidence"
    NOTIFY_SENT = "notify.sent"
    NOTIFY_FAILED = "notify.failed"
    PLUGIN_LOADED = "plugin.loaded"
    PLUGIN_REJECTED = "plugin.rejected"
    CONFIG_APPLIED = "config.applied"
    CONFIG_ROLLED_BACK = "config.rolled_back"
    DEPRECATION_USED = "deprecation.used"
    ADVISOR_REQUESTED = "advisor.requested"
    ADVISOR_RESPONDED = "advisor.responded"
    ADVISOR_REJECTED = "advisor.rejected"
    ADVISOR_DEGRADED = "advisor.degraded"
    ADVISOR_DISABLED = "advisor.disabled"
    ADVISOR_BUDGET_EXHAUSTED = "advisor.budget_exhausted"
    HOST_STATE = "host.state"
    HOST_EVENT = "host.event"
    CORE_EVENT = "core.event"
    HEARTBEAT = "heartbeat"


RUN_SCOPED_EVENTS: Final[frozenset[EventType]] = frozenset(
    {
        EventType.RUN_CREATED,
        EventType.RUN_QUEUED,
        EventType.RUN_STATE_CHANGED,
        EventType.RUN_PHASE_STARTED,
        EventType.RUN_PHASE_FINISHED,
        EventType.RUN_SIGNAL,
        EventType.RUN_VERDICT,
        EventType.RUN_FINISHED,
        EventType.RUN_INTERRUPTED,
        EventType.RESOURCE_LEASED,
        EventType.RESOURCE_RENEWED,
        EventType.RESOURCE_RELEASED,
        EventType.RECOVERY_STARTED,
        EventType.RECOVERY_FINISHED,
        EventType.EVIDENCE,
        EventType.NOTIFY_SENT,
        EventType.NOTIFY_FAILED,
        EventType.ADVISOR_REQUESTED,
        EventType.ADVISOR_RESPONDED,
        EventType.ADVISOR_REJECTED,
        EventType.ADVISOR_DEGRADED,
        EventType.ADVISOR_DISABLED,
        EventType.ADVISOR_BUDGET_EXHAUSTED,
    }
)

GLOBAL_EVENTS: Final[frozenset[EventType]] = frozenset(EventType) - RUN_SCOPED_EVENTS

RUN_EVENT_ID_PREFIX: Final[str] = "r"
GLOBAL_EVENT_ID_PREFIX: Final[str] = "g"


def run_event_id(run_id: str, seq: int) -> str:
    """运行事件的 SSE id：r:<run_id>:<seq>。"""
    return f"{RUN_EVENT_ID_PREFIX}:{run_id}:{seq}"


def global_event_id(seq: int) -> str:
    """全局事件的 SSE id：g:<global_seq>。"""
    return f"{GLOBAL_EVENT_ID_PREFIX}:{seq}"


def parse_event_id(event_id: str) -> tuple[str, str, int]:
    """把 SSE id 拆成 (scope, ref, seq)；scope 取 r 或 g，r 的 ref 是 run_id。"""
    parts = event_id.split(":")
    if len(parts) == 3 and parts[0] == RUN_EVENT_ID_PREFIX:
        return parts[0], parts[1], int(parts[2])
    if len(parts) == 2 and parts[0] == GLOBAL_EVENT_ID_PREFIX:
        return parts[0], "", int(parts[1])
    raise ValueError(f"非法事件 id：{event_id!r}")


class RunCreatedPayload(TypedDict, total=False):
    run_id: str
    template_id: str
    schedule_id: str | None
    scheduled_at: str
    merged_scheduled_ats: list[str]


class RunQueuedPayload(TypedDict, total=False):
    run_id: str
    priority: int


class RunStateChangedPayload(TypedDict, total=False):
    run_id: str
    from_state: str
    to_state: str
    reason: str


class RunPhaseStartedPayload(TypedDict, total=False):
    run_id: str
    phase: str
    attempt: int


class RunPhaseFinishedPayload(TypedDict, total=False):
    run_id: str
    phase: str
    status: str
    duration_ms: int


class RunSignalPayload(TypedDict, total=False):
    run_id: str
    kind: str
    payload: dict[str, object]


class RunVerdictPayload(TypedDict, total=False):
    run_id: str
    status: str
    policy_id: str
    reason: str
    confidence: float
    evidence_ids: list[str]


class RunFinishedPayload(TypedDict, total=False):
    run_id: str
    status: str
    duration_ms: int


class RunInterruptedPayload(TypedDict, total=False):
    run_id: str
    reason: str


class EnvStateChangedPayload(TypedDict, total=False):
    environment_id: str
    from_state: str
    to_state: str
    probe: dict[str, object]


class ResourceLeasedPayload(TypedDict, total=False):
    resource_id: str
    run_id: str
    expires_at: str


class ResourceRenewedPayload(TypedDict, total=False):
    resource_id: str
    run_id: str
    expires_at: str


class ResourceReleasedPayload(TypedDict, total=False):
    resource_id: str
    run_id: str
    reason: str


class RecoveryStartedPayload(TypedDict, total=False):
    run_id: str
    action: str
    attempt: int


class RecoveryFinishedPayload(TypedDict, total=False):
    run_id: str
    action: str
    result: str


class EvidencePayload(TypedDict, total=False):
    run_id: str
    kind: str
    path: str
    sha256: str


class NotifySentPayload(TypedDict, total=False):
    run_id: str
    channel: str
    result: str


class NotifyFailedPayload(TypedDict, total=False):
    run_id: str
    channel: str
    error: str
    attempts: int


class PluginLoadedPayload(TypedDict, total=False):
    plugin_id: str
    version: str


class PluginRejectedPayload(TypedDict, total=False):
    plugin_id: str
    reason: str


class ConfigAppliedPayload(TypedDict, total=False):
    plugin_id: str
    target: str
    revision: int
    result: str


class ConfigRolledBackPayload(TypedDict, total=False):
    plugin_id: str
    target: str
    from_revision: int
    to_revision: int


class DeprecationUsedPayload(TypedDict, total=False):
    object: str
    field: str
    sunset: str


class AdvisorRequestedPayload(TypedDict, total=False):
    run_id: str
    purpose: str
    budget: dict[str, object]


class AdvisorRespondedPayload(TypedDict, total=False):
    run_id: str
    purpose: str
    status: str
    confidence: float
    duration_ms: int


class AdvisorRejectedPayload(TypedDict, total=False):
    run_id: str
    purpose: str
    reason: str


class AdvisorDegradedPayload(TypedDict, total=False):
    run_id: str
    purpose: str
    reason: str


class AdvisorDisabledPayload(TypedDict, total=False):
    run_id: str
    purpose: str
    reason: str


class AdvisorBudgetExhaustedPayload(TypedDict, total=False):
    run_id: str
    scope: str
    limit: object


class HostStatePayload(TypedDict, total=False):
    state: str
    detail: str


class HostEventPayload(TypedDict, total=False):
    event: str
    detail: str


class CoreEventPayload(TypedDict, total=False):
    event: str
    detail: str


class HeartbeatPayload(TypedDict, total=False):
    ts: str
