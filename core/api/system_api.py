"""系统端点：health 存活检查（docs/03：存活、版本、server_id、能力）。"""

from __future__ import annotations

from core.api import store
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.hostops.serve import executable_version

# 能力清单列出本构建已落地的端点组；SSE 与导出不在本卡。
CAPABILITIES: tuple[str, ...] = ("health",)


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


__all__ = ["CAPABILITIES", "health"]
