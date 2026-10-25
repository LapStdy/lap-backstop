"""系统端点：health 存活检查与 settings 读写（docs/03 端点表）。"""

from __future__ import annotations

from contracts import ContractError, ErrorCode
from core.api import store
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.hostops.serve import executable_version
from storage.db.transaction import transaction

# 能力清单列出本构建已落地的端点组；SSE 与导出不在本卡。
CAPABILITIES: tuple[str, ...] = ("health", "settings")


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


__all__ = ["CAPABILITIES", "get_settings", "health", "put_settings"]
