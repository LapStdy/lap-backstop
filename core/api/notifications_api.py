"""通知端点：按当前配置做一次真实测试投递（docs/03 POST /notifications/test）。"""

from __future__ import annotations

from contracts import ContractError, ErrorCode
from core.api import store
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.notify.selftest import build_kernel_delivery_context, deliver_test_notification


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


__all__ = ["post_notification_test"]
