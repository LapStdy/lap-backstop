"""环境动作端点：手动预热与复位（docs/03 POST /environments/{id}/prepare、/reset）。

动作经注入的内核实现同步执行，成功与插件报失败都返回 200 与最新环境行；
环境不存在 404，动作未接入或状态不允许 409。
"""

from __future__ import annotations

from collections.abc import Callable

from contracts import ContractError, ErrorCode
from core.api import registry_store
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.environments.actions import EnvironmentActionResult, EnvironmentActions
from core.environments.errors import EnvironmentNotFoundError, EnvironmentStateError
from core.plugins.errors import PluginError

Action = Callable[[EnvironmentActions, str], EnvironmentActionResult]


def _invoke_prepare(actions: EnvironmentActions, environment_id: str) -> EnvironmentActionResult:
    # 不直接传 EnvironmentActions.prepare：3.14 起 Protocol 类方法取出来是空桩，调用返回 None。
    return actions.prepare(environment_id)


def _invoke_reset(actions: EnvironmentActions, environment_id: str) -> EnvironmentActionResult:
    return actions.reset(environment_id)


def post_environment_prepare(ctx: RequestCtx) -> ApiResponse:
    """手动预热：把环境带向 ready；环境 busy 时按 409 拒绝。"""
    return _environment_action(ctx, _invoke_prepare)


def post_environment_reset(ctx: RequestCtx) -> ApiResponse:
    """手动复位：任何状态先回 booting，成功后等探针确认 ready。"""
    return _environment_action(ctx, _invoke_reset)


def _environment_action(ctx: RequestCtx, action: Action) -> ApiResponse:
    """执行动作并回动作后的环境行；领域异常按 404/409 口径映射。

    Raises:
        ContractError: 动作未接入（409）、环境不存在（404）、状态或 provider
            不允许（409），message 保留领域原文。
    """
    environment_id = ctx.params["id"]
    actions = ctx.environment_actions
    if actions is None:
        raise ContractError(ErrorCode.CONFLICT, "环境动作未接入内核")
    try:
        result = action(actions, environment_id)
    except EnvironmentNotFoundError as error:
        raise ContractError(ErrorCode.NOT_FOUND, str(error)) from error
    except (EnvironmentStateError, PluginError) as error:
        raise ContractError(ErrorCode.CONFLICT, str(error)) from error
    row = registry_store.environment_row(ctx.connection, environment_id)
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"环境不存在：{environment_id}")
    return ApiResponse(
        status=200,
        payload={
            "ok": result.ok,
            "error_code": result.error_code,
            "detail": result.detail,
            "environment": row,
        },
    )


__all__ = ["post_environment_prepare", "post_environment_reset"]
