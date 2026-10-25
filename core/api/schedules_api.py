"""调度端点：列表、详情、新建（docs/03 端点表与 schedules 字段表）。

字段校验的规则：可选字段缺省取 config/defaults.toml 的 [scheduler] 基线；
``window_minutes <= deadline_minutes`` 违反按 E_CONFIG 拒绝（docs/03 保存校验）；
带 Idempotency-Key 的新建以 key 决定 id，重复提交命中同一行不再新建。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from contracts import ContractError, ErrorCode
from core.api import store
from core.api.ctx import RequestCtx, body_object
from core.api.wire import ApiResponse, content_etag
from storage.db.transaction import transaction
from storage.defaults import default

KINDS = frozenset({"cron", "at", "interval"})
CATCH_UPS = frozenset({"run_once", "skip", "window"})
_FIELDS = frozenset(
    {
        "name",
        "kind",
        "expr",
        "at",
        "timezone",
        "catch_up",
        "window_minutes",
        "coalesce",
        "priority",
        "deadline_minutes",
        "preheat_minutes",
        "keep_warm_minutes",
        "job_template_id",
        "enabled",
    }
)
_REQUIRED = ("name", "kind", "job_template_id")
_INT_MINUTES = (
    "window_minutes",
    "deadline_minutes",
    "preheat_minutes",
    "keep_warm_minutes",
)


def list_schedules(ctx: RequestCtx) -> ApiResponse:
    """调度列表，按创建时间排序。"""
    return ApiResponse(status=200, payload={"schedules": store.schedule_rows(ctx.connection)})


def get_schedule(ctx: RequestCtx) -> ApiResponse:
    """调度详情；应答带内容 ETag，供 PUT 的 If-Match 使用。"""
    row = _require_schedule(ctx)
    return ApiResponse(status=200, payload={"schedule": row}, etag=content_etag(row))


def create_schedule(ctx: RequestCtx) -> ApiResponse:
    """新建调度；带 Idempotency-Key 时按 key 幂等。"""
    values = schedule_values(body_object(ctx), existing=None)
    _require_template(ctx, str(values["job_template_id"]))
    schedule_id = _new_schedule_id(ctx.headers.get("idempotency-key"))
    with transaction(ctx.connection):
        created = store.insert_schedule(
            ctx.connection,
            {**values, "id": schedule_id},
            at=ctx.clock.utc_now(),
        )
    row = store.schedule_row(ctx.connection, schedule_id)
    if row is None:
        raise ContractError(ErrorCode.INTERNAL, "调度写入后读不到")
    return ApiResponse(
        status=201 if created else 200,
        payload={"schedule": row},
        etag=content_etag(row),
    )


def put_schedule(ctx: RequestCtx) -> ApiResponse:
    """整行替换修改；带 If-Match 时校验当前 ETag，不符按 E_CONFLICT 拒绝。"""
    row = _require_schedule(ctx)
    _check_if_match(ctx.headers.get("if-match"), content_etag(row))
    values = schedule_values(body_object(ctx), existing=row)
    _require_template(ctx, str(values["job_template_id"]))
    with transaction(ctx.connection):
        updated = store.update_schedule(
            ctx.connection,
            ctx.params["id"],
            values,
            at=ctx.clock.utc_now(),
        )
    if not updated:
        raise ContractError(ErrorCode.NOT_FOUND, f"调度不存在：{ctx.params['id']}")
    updated_row = _require_schedule(ctx)
    return ApiResponse(
        status=200,
        payload={"schedule": updated_row},
        etag=content_etag(updated_row),
    )


def delete_schedule(ctx: RequestCtx) -> ApiResponse:
    """删除调度；仍被运行记录引用时按 E_CONFLICT 拒绝。"""
    _require_schedule(ctx)
    try:
        with transaction(ctx.connection):
            removed = store.delete_schedule(ctx.connection, ctx.params["id"])
    except sqlite3.IntegrityError as error:
        raise ContractError(ErrorCode.CONFLICT, "调度仍被运行记录引用，不能删除") from error
    if not removed:
        raise ContractError(ErrorCode.NOT_FOUND, f"调度不存在：{ctx.params['id']}")
    return ApiResponse(status=204, payload=None)


def schedule_values(body: dict[str, Any], existing: dict[str, Any] | None) -> dict[str, Any]:
    """把请求体并进默认值或既有行，校验并规范化成完整列值。

    Args:
        body: 请求体对象；只接受 ``_FIELDS`` 内的字段。
        existing: 修改场景的当前行；None 表示新建。

    Raises:
        ContractError: 字段类型或取值非法（E_BAD_REQUEST）或保存校验不过（E_CONFIG）。
    """
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
        "timezone": default("scheduler", "timezone"),
        "catch_up": default("scheduler", "catch_up"),
        "window_minutes": default("scheduler", "window_minutes"),
        "coalesce": default("scheduler", "coalesce"),
        "priority": default("scheduler", "priority"),
        "deadline_minutes": default("scheduler", "deadline_minutes"),
        "preheat_minutes": default("scheduler", "preheat_minutes"),
        "keep_warm_minutes": default("scheduler", "keep_warm_minutes"),
        "enabled": True,
    }


def _normalize(merged: dict[str, Any]) -> None:
    """逐字段收窄类型并按 kind 规范 expr 与 at。"""
    merged["name"] = _text(merged, "name")
    merged["job_template_id"] = _text(merged, "job_template_id")
    kind = _enum(merged, "kind", KINDS)
    _enum(merged, "catch_up", CATCH_UPS)
    for key in _INT_MINUTES:
        _non_negative_int(merged, key)
    _integer(merged, "priority")
    for key in ("coalesce", "enabled"):
        _boolean(merged, key)
    _timezone(merged)
    if kind == "cron":
        merged["expr"] = _text(merged, "expr")
    elif kind == "interval":
        merged["expr"] = str(_positive_int(merged, "expr"))
    else:
        merged["at"] = _timestamp(merged, "at")
    if int(merged["window_minutes"]) > int(merged["deadline_minutes"]):
        raise ContractError(ErrorCode.CONFIG, "window_minutes 不能超过 deadline_minutes")


def _require_schedule(ctx: RequestCtx) -> dict[str, Any]:
    row = store.schedule_row(ctx.connection, ctx.params["id"])
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"调度不存在：{ctx.params['id']}")
    return row


def _require_template(ctx: RequestCtx, template_id: str) -> None:
    if not store.job_template_exists(ctx.connection, template_id):
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            f"job_template_id 不存在：{template_id}",
        )


def _new_schedule_id(idempotency_key: str | None) -> str:
    """幂等键决定 id：同 key 重放命中同一行；无 key 每次新 id。"""
    if idempotency_key is None or not idempotency_key.strip():
        return uuid4().hex
    return uuid5(NAMESPACE_URL, f"schedule:{idempotency_key}").hex


def _check_if_match(header: str | None, current: str) -> None:
    """If-Match 校验：``*`` 匹配任意存在；否则必须等于当前 ETag。"""
    if header is None or header == "*" or header == current:
        return
    raise ContractError(ErrorCode.CONFLICT, "If-Match 与当前版本不符，先重读再改")


def _text(merged: dict[str, Any], key: str) -> str:
    value = merged.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是非空字符串")
    return value


def _enum(merged: dict[str, Any], key: str, allowed: frozenset[str]) -> str:
    value = merged.get(key)
    if value not in allowed:
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            f"字段 {key} 取值必须是 {'、'.join(sorted(allowed))} 之一",
        )
    return str(value)


def _integer(merged: dict[str, Any], key: str) -> int:
    value = merged.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是整数")
    merged[key] = value
    return value


def _non_negative_int(merged: dict[str, Any], key: str) -> int:
    value = _integer(merged, key)
    if value < 0:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 不能是负数")
    return value


def _positive_int(merged: dict[str, Any], key: str) -> int:
    value = _integer(merged, key)
    if value <= 0:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是正整数")
    return value


def _boolean(merged: dict[str, Any], key: str) -> bool:
    value = merged.get(key)
    if not isinstance(value, bool):
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是布尔值")
    merged[key] = value
    return value


def _timezone(merged: dict[str, Any]) -> None:
    name = _text(merged, "timezone")
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as error:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 timezone 不是合法时区：{name}") from error


def _timestamp(merged: dict[str, Any], key: str) -> str:
    value = merged.get(key)
    if not isinstance(value, str):
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是 RFC3339 时间")
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 不是合法时间：{value}") from error
    if moment.tzinfo is None:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须带时区")
    return moment.isoformat()


__all__ = [
    "create_schedule",
    "delete_schedule",
    "get_schedule",
    "list_schedules",
    "put_schedule",
    "schedule_values",
]
