"""插件配置端点：schema 与当前配置的读改（docs/03 HTTP API）。

schema 由插件 manifest 声明、加载时解析入库，端点只读；config 的 payload
结构同样由插件 schema 决定，内核只校验 JSON 对象形态，未声明的字段原样保存。
"""

from __future__ import annotations

from contracts import ContractError, ErrorCode
from core.api import registry_store
from core.api.ctx import RequestCtx, body_object
from core.api.wire import ApiResponse, check_if_match, content_etag
from storage.db.transaction import transaction
from storage.repositories.plugin_config import PluginConfigRepository, PluginConfigRow


def get_plugin_schema(ctx: RequestCtx) -> ApiResponse:
    """插件配置 JSON Schema；契约以 plugins.config_schema 为准。"""
    row = registry_store.plugin_row(ctx.connection, ctx.params["id"])
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"插件不存在：{ctx.params['id']}")
    return ApiResponse(status=200, payload={"schema": row["config_schema"]})


def get_plugin_config(ctx: RequestCtx) -> ApiResponse:
    """当前插件配置与修订号；应答带内容 ETag。"""
    row = _require_config(ctx)
    payload = _payload(row)
    return ApiResponse(status=200, payload=payload, etag=content_etag(payload))


def put_plugin_config(ctx: RequestCtx) -> ApiResponse:
    """整份覆盖当前配置；带 If-Match 时校验修订版本，成功返回新修订号。

    Raises:
        ContractError: 插件不存在（E_NOT_FOUND）、If-Match 不符（E_CONFLICT）、
            请求体不是 JSON 对象（E_BAD_REQUEST）。
    """
    current = _require_config(ctx)
    check_if_match(ctx.headers.get("if-match"), content_etag(_payload(current)))
    config = body_object(ctx)
    with transaction(ctx.connection):
        saved = PluginConfigRepository(ctx.connection).save(
            ctx.params["id"], config, at=ctx.clock.utc_now()
        )
    if saved is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"插件不存在：{ctx.params['id']}")
    payload = _payload(saved)
    return ApiResponse(status=200, payload=payload, etag=content_etag(payload))


def _require_config(ctx: RequestCtx) -> PluginConfigRow:
    row = PluginConfigRepository(ctx.connection).get(ctx.params["id"])
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"插件不存在：{ctx.params['id']}")
    return row


def _payload(row: PluginConfigRow) -> dict[str, object]:
    return {"config": dict(row.config), "revision": row.revision}


__all__ = ["get_plugin_config", "get_plugin_schema", "put_plugin_config"]
