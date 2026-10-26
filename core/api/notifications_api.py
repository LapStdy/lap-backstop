"""通知端点：投递记录列表（GET /notifications）与测试投递（POST /notifications/test）。"""

from __future__ import annotations

from typing import Final

from contracts import ContractError, ErrorCode
from core.api import notifications_store, store
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.notify.selftest import build_kernel_delivery_context, deliver_test_notification

DEFAULT_LIMIT: Final[int] = 100
MAX_LIMIT: Final[int] = 500


def list_notifications(ctx: RequestCtx) -> ApiResponse:
    """通知记录列表；过滤与截断口径见 docs/03 端点表，端点只读、不写事件。"""
    return ApiResponse(
        status=200,
        payload={
            "notifications": notifications_store.notification_rows(ctx.connection, _filters(ctx))
        },
    )


def _filters(ctx: RequestCtx) -> notifications_store.NotificationFilters:
    """解析过滤参数；认识的参数按 docs/03 口径收敛，不认识的忽略。"""
    query = ctx.query
    return notifications_store.NotificationFilters(
        run_id=query.get("run_id"),
        status=query.get("status"),
        limit=_limit(query.get("limit")),
    )


def _limit(raw: str | None) -> int:
    """limit 收敛到 1..500：缺省与非整数按默认，小于 1 按 1，大于上限按上限。"""
    if raw is None:
        return DEFAULT_LIMIT
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_LIMIT
    return max(1, min(value, MAX_LIMIT))


def post_notification_test(ctx: RequestCtx) -> ApiResponse:
    """测试通知渠道：单次真实投递，结果结构化返回。

    投递失败不算端点失败：渠道 4xx/5xx/超时映射为 ok=false 与 E_NOTIFY，
    是否重试由调用方决定；测试投递不写 notify.sent / notify.failed 事件，
    渠道凭据不进响应。配置未启用或非法按 E_CONFIG 返回 400 与可操作提示。

    Raises:
        ContractError: 配置未启用或非法，错误码 E_CONFIG，HTTP 400。
    """
    started = ctx.clock.monotonic()
    try:
        delivery = deliver_test_notification(
            store.all_settings(ctx.connection),
            build_kernel_delivery_context(ctx.clock, data_root=ctx.data_root),
        )
    except ContractError as error:
        # 配置类失败对外的码是 E_CONFIG（docs/03 HTTP 映射）；渠道内码只用于内核簿记。
        raise ContractError(ErrorCode.CONFIG, error.message) from error
    elapsed_ms = int((ctx.clock.monotonic() - started) * 1000)
    result = delivery.result
    payload: dict[str, object] = {
        "ok": result.ok,
        "channel": delivery.channel,
        "error_code": result.error_code,
        "elapsed_ms": elapsed_ms,
        "detail": result.detail,
    }
    return ApiResponse(status=200, payload=payload)


__all__ = ["list_notifications", "post_notification_test"]
