"""六种插件能力协议。

唯一真源是 docs/05 的能力接口一节。内核只按能力路由，不导入插件内部模块。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from contracts.context import RunContext
from contracts.types import (
    AdvisorRequest,
    AdvisorResponse,
    ApplyResult,
    ConfigDiff,
    DeliveryResult,
    EnvironmentProbe,
    EnvironmentRef,
    Notification,
    PhaseResult,
    RecoveryAction,
    RecoveryResult,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    StopMode,
    StopResult,
    Verdict,
)


@runtime_checkable
class EnvironmentProvider(Protocol):
    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe: ...

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult: ...

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult: ...

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult: ...

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult: ...


@runtime_checkable
class ScriptAdapter(Protocol):
    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle: ...

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation: ...

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult: ...

    def recover(
        self, handle: ScriptHandle, ctx: RunContext, action: RecoveryAction
    ) -> RecoveryResult: ...


@runtime_checkable
class Verifier(Protocol):
    def verify(self, ctx: RunContext) -> Verdict: ...


@runtime_checkable
class ConfigProjection(Protocol):
    def diff(
        self, ctx: RunContext, desired: Mapping[str, Any], actual: Mapping[str, Any]
    ) -> list[ConfigDiff]: ...

    def apply(self, ctx: RunContext, desired: Mapping[str, Any]) -> ApplyResult: ...

    def rollback(self, ctx: RunContext, revision: str) -> ApplyResult: ...


@runtime_checkable
class Notifier(Protocol):
    def send(self, ctx: RunContext, message: Notification) -> DeliveryResult: ...


@runtime_checkable
class Advisor(Protocol):
    def advise(self, ctx: RunContext, request: AdvisorRequest) -> AdvisorResponse: ...
