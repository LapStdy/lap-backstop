"""作业模板端点：CRUD（docs/03 端点表与 job_templates 字段表）。

config 与 recovery_policy 的结构由插件 schema 决定，本层只校验 JSON
对象形态，不解释内容；verification_policy 是内核定义的类型（ADR 0003），
创建时生成缺省策略，请求携带时按形状校验。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from contracts import ContractError, ErrorCode, EvidenceKind
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
_POLICY_FIELDS = frozenset({"id", "verifier_plugin_id", "required_evidence", "params"})
_EVIDENCE_KINDS = frozenset(kind.value for kind in EvidenceKind)


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
    if "verification_policy" in body:
        merged["verification_policy"] = _policy_values(body["verification_policy"])
    elif existing is None:
        merged["verification_policy"] = _default_policy(merged["plugin_id"])
    return merged


def _default_values() -> dict[str, Any]:
    return {
        "config": {},
        "config_schema_version": "1",
        "recovery_policy": {},
        "enabled": True,
    }


def _normalize(merged: dict[str, Any]) -> None:
    for key in ("name", "plugin_id", "environment_id", "config_schema_version"):
        merged[key] = require_text(merged, key)
    for key in ("config", "recovery_policy"):
        merged[key] = dict(require_object(merged, key))
    merged["enabled"] = require_bool(merged, "enabled")


def _default_policy(plugin_id: str) -> dict[str, Any]:
    """缺省验证策略：id 由模板创建生成；证据种类不强制，验证器随模板插件解析。"""
    return {
        "id": uuid4().hex,
        "verifier_plugin_id": plugin_id,
        "required_evidence": [],
        "params": {},
    }


def _policy_values(value: object) -> dict[str, Any]:
    """校验请求携带的验证策略，按 ADR 0003 的形状归一成完整策略。

    Raises:
        ContractError: 不是 JSON 对象、含未知字段，或必填字段缺失、类型不符。
    """
    if not isinstance(value, dict):
        raise ContractError(ErrorCode.BAD_REQUEST, "字段 verification_policy 必须是 JSON 对象")
    unknown = sorted(set(value) - _POLICY_FIELDS)
    if unknown:
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            f"verification_policy 含未知字段：{', '.join(unknown)}",
        )
    return {
        "id": _require_policy_text(value, "id"),
        "verifier_plugin_id": _require_policy_text(value, "verifier_plugin_id"),
        "required_evidence": _require_policy_evidence(value),
        "params": _require_policy_params(value),
    }


def _require_policy_text(policy: Mapping[str, Any], field: str) -> str:
    value = policy.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            f"verification_policy.{field} 必须是非空字符串",
        )
    return value


def _require_policy_evidence(policy: Mapping[str, Any]) -> list[str]:
    value = policy.get("required_evidence")
    kinds = "、".join(sorted(_EVIDENCE_KINDS))
    if not isinstance(value, list) or not value:
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            f"verification_policy.required_evidence 必须是数组，取值必须是 {kinds} 之一，至少一项",
        )
    unknown = [item for item in value if not isinstance(item, str) or item not in _EVIDENCE_KINDS]
    if unknown:
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            "verification_policy.required_evidence 含未知证据种类："
            + f"{', '.join(repr(item) for item in unknown)}；取值必须是 {kinds} 之一",
        )
    return list(value)


def _require_policy_params(policy: Mapping[str, Any]) -> dict[str, Any]:
    value = policy.get("params")
    if not isinstance(value, dict):
        raise ContractError(ErrorCode.BAD_REQUEST, "verification_policy.params 必须是 JSON 对象")
    return dict(value)


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
