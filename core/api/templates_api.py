"""作业模板端点：CRUD（docs/03 端点表与 job_templates 字段表）。

payload 字段（config、两个 policy）的结构由插件 schema 决定，
本层只校验 JSON 对象形态，不解释内容。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from contracts import ContractError, ErrorCode
from core.api import catalog_store
from core.api.catalog_store import TemplateJson
from core.api.ctx import RequestCtx, body_object
from core.api.fields import require_bool, require_object, require_text
from core.api.wire import ApiResponse, check_if_match, content_etag
from storage.db.transaction import transaction

_FIELDS = frozenset(
    {
        "name",
        "plugin_id",
        "environment_id",
        "config",
        "config_schema_version",
        "verification_policy",
        "recovery_policy",
        "enabled",
    }
)
_REQUIRED = ("name", "plugin_id", "environment_id")


def list_templates(ctx: RequestCtx) -> ApiResponse:
    """模板列表，按创建时间排序。"""
    rows = catalog_store.template_rows(ctx.connection)
    return ApiResponse(status=200, payload={"job_templates": rows})


def get_template(ctx: RequestCtx) -> ApiResponse:
    """模板详情；应答带内容 ETag。"""
    row = _require_template_row(ctx)
    return ApiResponse(status=200, payload={"job_template": row}, etag=content_etag(row))


def create_template(ctx: RequestCtx) -> ApiResponse:
    """新建模板；字段缺省取空 payload 与启用态。"""
    values = template_values(body_object(ctx), existing=None)
    template_id = uuid4().hex
    with transaction(ctx.connection):
        catalog_store.insert_template(
            ctx.connection, {**values, "id": template_id}, at=ctx.clock.utc_now()
        )
    row = catalog_store.template_row(ctx.connection, template_id)
    if row is None:
        raise ContractError(ErrorCode.INTERNAL, "模板写入后读不到")
    return ApiResponse(status=201, payload={"job_template": row}, etag=content_etag(row))


def put_template(ctx: RequestCtx) -> ApiResponse:
    """整行替换修改；带 If-Match 时校验当前 ETag。"""
    row = _require_template_row(ctx)
    check_if_match(ctx.headers.get("if-match"), content_etag(row))
    values = template_values(body_object(ctx), existing=row)
    with transaction(ctx.connection):
        updated = catalog_store.update_template(
            ctx.connection,
            ctx.params["id"],
            values,
            at=ctx.clock.utc_now(),
        )
    if not updated:
        raise ContractError(ErrorCode.NOT_FOUND, f"模板不存在：{ctx.params['id']}")
    updated_row = _require_template_row(ctx)
    return ApiResponse(
        status=200,
        payload={"job_template": updated_row},
        etag=content_etag(updated_row),
    )


def delete_template(ctx: RequestCtx) -> ApiResponse:
    """删除模板；仍被调度或运行引用时按 E_CONFLICT 拒绝。"""
    _require_template_row(ctx)
    try:
        with transaction(ctx.connection):
            removed = catalog_store.delete_template(ctx.connection, ctx.params["id"])
    except sqlite3.IntegrityError as error:
        raise ContractError(ErrorCode.CONFLICT, "模板仍被调度或运行记录引用，不能删除") from error
    if not removed:
        raise ContractError(ErrorCode.NOT_FOUND, f"模板不存在：{ctx.params['id']}")
    return ApiResponse(status=204, payload=None)


def template_values(
    body: Mapping[str, Any],
    existing: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """把请求体并进默认值或既有行，校验成完整列值。"""
    merged = _default_values() if existing is None else dict(existing)
    for key, value in body.items():
        if key not in _FIELDS:
            raise ContractError(ErrorCode.BAD_REQUEST, f"未知字段：{key}")
        merged[key] = value
    if existing is None:
        missing = [key for key in _REQUIRED if key not in merged]
        if missing:
            raise ContractError(ErrorCode.BAD_REQUEST, f"缺少必填字段：{', '.join(missing)}")
    _normalize(merged)
    return merged


def _default_values() -> dict[str, Any]:
    return {
        "config": {},
        "config_schema_version": "1",
        "verification_policy": {},
        "recovery_policy": {},
        "enabled": True,
    }


def _normalize(merged: dict[str, Any]) -> None:
    for key in ("name", "plugin_id", "environment_id", "config_schema_version"):
        merged[key] = require_text(merged, key)
    for key in ("config", "verification_policy", "recovery_policy"):
        merged[key] = dict(require_object(merged, key))
    merged["enabled"] = require_bool(merged, "enabled")


def _require_template_row(ctx: RequestCtx) -> TemplateJson:
    row = catalog_store.template_row(ctx.connection, ctx.params["id"])
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"模板不存在：{ctx.params['id']}")
    return row


__all__ = [
    "create_template",
    "delete_template",
    "get_template",
    "list_templates",
    "put_template",
    "template_values",
]
