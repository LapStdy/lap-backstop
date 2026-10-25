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
    """环境生命周期：探测、准备、重置、收尾与保温。"""

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        """探测环境当前状态与摘要。"""
        ...

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        """prepare 阶段：把环境带向 ready，结果按 PhaseResult 报告。"""
        ...

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        """reset_environment 恢复动作对应的环境重置。"""
        ...

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        """收尾阶段；``keep_warm`` 为真时转保温（内部调 warm），保温期不持有租约。"""
        ...

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        """保持环境就绪 ``minutes`` 分钟；只管就绪，不管理租约。"""
        ...


@runtime_checkable
class ScriptAdapter(Protocol):
    """脚本生命周期：拉起、观察、停止与恢复。"""

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        """拉起脚本，返回由插件解释、内核不解析的句柄。"""
        ...

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        """轮询观察：返回状态与自上次调用后的 Signal 列表。"""
        ...

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult:
        """按 ``mode``（graceful/force）停止脚本。"""
        ...

    def recover(
        self, handle: ScriptHandle, ctx: RunContext, action: RecoveryAction
    ) -> RecoveryResult:
        """按恢复动作处置脚本。"""
        ...


@runtime_checkable
class Verifier(Protocol):
    """验证器：唯一能产出 Verdict 的能力。"""

    def verify(self, ctx: RunContext) -> Verdict:
        """验证一次运行并产出结论。"""
        ...


@runtime_checkable
class ConfigProjection(Protocol):
    """配置投影：目标侧配置的比对、下发与回滚。"""

    def diff(
        self, ctx: RunContext, desired: Mapping[str, Any], actual: Mapping[str, Any]
    ) -> list[ConfigDiff]:
        """比对期望与实际，产出差异清单。"""
        ...

    def apply(self, ctx: RunContext, desired: Mapping[str, Any]) -> ApplyResult:
        """把期望配置下发到目标侧。"""
        ...

    def rollback(self, ctx: RunContext, revision: str) -> ApplyResult:
        """回滚到指定修订。"""
        ...


@runtime_checkable
class Notifier(Protocol):
    """通知渠道发送。"""

    def send(self, ctx: RunContext, message: Notification) -> DeliveryResult:
        """发送一条通知，结果按 DeliveryResult 报告。"""
        ...


@runtime_checkable
class Advisor(Protocol):
    """AI 顾问：只出建议，不写结论。"""

    def advise(self, ctx: RunContext, request: AdvisorRequest) -> AdvisorResponse:
        """请求一次建议，结果按 AdvisorResponse 报告。"""
        ...
