"""系统端点：health、status 快照、settings 读写与环境/插件只读（docs/03 端点表）。"""

from __future__ import annotations

from contracts import ContractError, ErrorCode
from core.api import store
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.hostops.serve import executable_version
from storage.db.transaction import transaction

# 能力清单列出本构建已落地的端点组；SSE 与导出不在本卡。
CAPABILITIES: tuple[str, ...] = (
    "health",
    "status",
    "settings",
    "schedules",
    "job-templates",
    "runs",
    "environments",
    "plugins",
)


def health(ctx: RequestCtx) -> ApiResponse:
    """服务存活与自身信息；server_id 首次访问时落库并保持不变。"""
    server_id = store.ensure_server_id(ctx.connection, at=ctx.clock.utc_now())
    return ApiResponse(
        status=200,
        payload={
            "status": "ok",
            "version": executable_version(),
            "server_id": server_id,
            "capabilities": list(CAPABILITIES),
        },
    )


def get_status(ctx: RequestCtx) -> ApiResponse:
    """当前运行、队列、环境状态与最近结论（docs/03 /status）。"""
    connection = ctx.connection
    return ApiResponse(
        status=200,
        payload={
            "active_runs": store.active_run_rows(connection),
            "queue": store.queued_run_rows(connection),
            "environments": store.environment_rows(connection),
            "recent_verdicts": store.recent_verdict_rows(connection),
        },
    )


def get_settings(ctx: RequestCtx) -> ApiResponse:
    """全部配置项，value 按落库 JSON 原样返回。"""
    return ApiResponse(status=200, payload={"settings": store.all_settings(ctx.connection)})


def put_settings(ctx: RequestCtx) -> ApiResponse:
    """整批写入配置项并回读全量，写后即可见。"""
    if not isinstance(ctx.body, dict):
        raise ContractError(ErrorCode.BAD_REQUEST, "settings 请求体必须是 JSON 对象")
    with transaction(ctx.connection):
        store.put_settings(ctx.connection, ctx.body, at=ctx.clock.utc_now())
    return get_settings(ctx)


def list_environments(ctx: RequestCtx) -> ApiResponse:
    """环境列表。"""
    return ApiResponse(status=200, payload={"environments": store.environment_rows(ctx.connection)})


def get_environment(ctx: RequestCtx) -> ApiResponse:
    """环境详情与状态。"""
    row = store.environment_row(ctx.connection, ctx.params["id"])
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"环境不存在：{ctx.params['id']}")
    return ApiResponse(status=200, payload={"environment": row})


def list_plugins(ctx: RequestCtx) -> ApiResponse:
    """插件列表。"""
    return ApiResponse(status=200, payload={"plugins": store.plugin_rows(ctx.connection)})


def get_plugin(ctx: RequestCtx) -> ApiResponse:
    """插件详情。"""
    row = store.plugin_row(ctx.connection, ctx.params["id"])
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"插件不存在：{ctx.params['id']}")
    return ApiResponse(status=200, payload={"plugin": row})


__all__ = [
    "CAPABILITIES",
    "get_environment",
    "get_plugin",
    "get_settings",
    "get_status",
    "health",
    "list_environments",
    "list_plugins",
    "put_settings",
]
