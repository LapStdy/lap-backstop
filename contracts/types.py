"""契约数据类型：插件能力、判定、证据与状态枚举。

唯一真源是 docs/05 的类型表；本模块只做无副作用的定义，不依赖 core/、plugins/、web/、storage/。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

# 契约边界的 JSON 载荷：只在跨插件边界出现，内部模块用具体类型（docs/11）。
JSON = Mapping[str, Any]


class EnvironmentState(StrEnum):
    UNKNOWN = "unknown"
    OFF = "off"
    BOOTING = "booting"
    READY = "ready"
    BUSY = "busy"
    DEGRADED = "degraded"
    FAILED = "failed"


class RunState(StrEnum):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    PREPARING = "preparing"
    WAITING = "waiting"
    EXECUTING = "executing"
    VERIFYING = "verifying"
    CLEANING = "cleaning"
    RECOVERING = "recovering"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"
    CANCELLED = "cancelled"


TERMINAL_RUN_STATES = frozenset(
    {RunState.SUCCEEDED, RunState.FAILED, RunState.UNKNOWN, RunState.CANCELLED}
)


class PhaseState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    OK = "ok"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"


class PhaseName(StrEnum):
    """四个执行阶段；顺序即生命周期顺序。"""

    PREPARE = "prepare"
    EXECUTE = "execute"
    VERIFY = "verify"
    CLEANUP = "cleanup"


class LeaseState(StrEnum):
    FREE = "free"
    LEASED = "leased"
    EXPIRING = "expiring"
    RELEASED = "released"


class VerdictStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    UNKNOWN = "unknown"


class CheckStatus(StrEnum):
    OK = "ok"
    FAILED = "failed"
    UNKNOWN = "unknown"


class NotificationStatus(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class EvidenceKind(StrEnum):
    SCREENSHOT = "screenshot"
    LOG = "log"
    PROBE = "probe"
    EXIT = "exit"
    ARTIFACT = "artifact"


class EvidenceSource(StrEnum):
    KERNEL = "kernel"
    PLUGIN = "plugin"
    HOST = "host"


class ScriptState(StrEnum):
    RUNNING = "running"
    EXITED = "exited"
    HUNG = "hung"


class StopMode(StrEnum):
    GRACEFUL = "graceful"
    FORCE = "force"


class RecoveryAction(StrEnum):
    RETRY = "retry"
    RESET_ENVIRONMENT = "reset_environment"
    RESTART_SCRIPT = "restart_script"
    ABORT = "abort"


class ConfigDiffKind(StrEnum):
    ADDED = "added"
    REMOVED = "removed"
    CHANGED = "changed"


class NotificationLevel(StrEnum):
    INFO = "info"
    WARN = "warn"
    ERROR = "error"


class AdvisorStatus(StrEnum):
    OK = "ok"
    ABSTAIN = "abstain"
    ERROR = "error"


class ErrorCategory(StrEnum):
    ENVIRONMENT = "environment"
    SCRIPT = "script"
    CONFIG = "config"
    VERIFY = "verify"
    RESOURCE = "resource"
    SYSTEM = "system"


@dataclass(frozen=True, slots=True)
class EnvironmentRef:
    id: str
    provider_id: str
    config: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ScriptRef:
    id: str
    plugin_id: str
    config: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EnvironmentProbe:
    state: EnvironmentState
    detail: str = ""
    observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class PhaseResult:
    ok: bool
    error_code: str | None = None
    detail: str = ""
    started_at: datetime | None = None
    ended_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ScriptHandle:
    id: str
    pid: int | None = None
    meta: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Signal:
    kind: str
    payload: JSON = field(default_factory=dict)
    at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ScriptObservation:
    state: ScriptState
    signals: tuple[Signal, ...] = ()
    last_signal: Signal | None = None
    progress: float | None = None
    since: datetime | None = None


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    status: CheckStatus
    detail: str = ""
    evidence_ids: tuple[str, ...] = ()
    at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Verdict:
    status: VerdictStatus
    policy_id: str
    confidence: float
    reason: str
    checks: tuple[CheckResult, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Evidence:
    id: str
    kind: EvidenceKind
    source: EvidenceSource
    path: str
    sha256: str
    summary: str = ""
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class VerificationPolicy:
    id: str
    verifier_plugin_id: str
    required_evidence: tuple[EvidenceKind, ...]
    params: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConfigDiff:
    path: str
    desired: object
    actual: object
    kind: ConfigDiffKind


@dataclass(frozen=True, slots=True)
class ApplyResult:
    ok: bool
    error_code: str | None = None
    detail: str = ""
    meta: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    ok: bool
    error_code: str | None = None
    detail: str = ""
    meta: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StopResult:
    ok: bool
    error_code: str | None = None
    detail: str = ""
    meta: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    ok: bool
    error_code: str | None = None
    detail: str = ""
    meta: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Notification:
    level: NotificationLevel
    title: str
    body: str
    evidence_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AdvisorRequest:
    kind: str
    context: JSON = field(default_factory=dict)
    evidence: tuple[str, ...] = ()
    budget: JSON = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AdvisorResponse:
    status: AdvisorStatus
    confidence: float
    result: JSON = field(default_factory=dict)
    usage: JSON = field(default_factory=dict)
