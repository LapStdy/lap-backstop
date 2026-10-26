"""Supervisor 环境动作到 API 注入协议的适配：PhaseResult 折叠成动作结果。

领域异常（环境不存在、状态拒绝、provider 缺失）不在这里翻译：原样冒泡，
由 API 端点按 HTTP 口径映射 404/409。
"""

from __future__ import annotations

from contracts import PhaseResult
from core.environments.actions import EnvironmentActionResult
from core.supervisor import Supervisor


class SupervisorEnvironmentActions:
    """把 Supervisor 的手动环境动作包成 ``EnvironmentActions``。"""

    def __init__(self, supervisor: Supervisor) -> None:
        self._supervisor = supervisor

    def prepare(self, environment_id: str) -> EnvironmentActionResult:
        """手动预热，结果取 provider 的 PhaseResult 摘要。"""
        return _action_result(self._supervisor.prepare_environment(environment_id))

    def reset(self, environment_id: str) -> EnvironmentActionResult:
        """手动复位，结果取 provider 的 PhaseResult 摘要。"""
        return _action_result(self._supervisor.reset_environment(environment_id))


def _action_result(phase: PhaseResult) -> EnvironmentActionResult:
    return EnvironmentActionResult(ok=phase.ok, error_code=phase.error_code, detail=phase.detail)


__all__ = ["SupervisorEnvironmentActions"]
