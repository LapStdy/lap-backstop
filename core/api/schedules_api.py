"""调度端点：CRUD 与手动触发（docs/03 端点表、schedules 与 request_idempotency 字段表）。

字段校验的规则：可选字段缺省取 config/defaults.toml 的 [scheduler] 基线；
``window_minutes <= deadline_minutes`` 违反按 E_CONFIG 拒绝（docs/03 保存校验）；
带 Idempotency-Key 的新建以 key 决定 id，重复提交命中同一行不再新建。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from contracts import Clock, ContractError, ErrorCode
from core.api import catalog_store, run_store
from core.api.catalog_store import ScheduleJson
from core.api.ctx import RequestCtx, body_object
from core.api.fields import (
    require_bool,
    require_enum,
    require_int,
    require_text,
    require_timestamp,
    require_timezone,
)
from core.api.runs_api import run_json
from core.api.wire import ApiResponse, check_if_match, content_etag
from core.jobs.run_service import JobRunService
from storage.db.transaction import transaction
from storage.defaults import default
from storage.repositories.job_runs import JobRunRecord, JobRunRepository, NewRun
from storage.repositories.job_templates import JobTemplateRepository

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
    rows = catalog_store.schedule_rows(ctx.connection)
    return ApiResponse(status=200, payload={"schedules": rows})


def run_now(ctx: RequestCtx) -> ApiResponse:
    """手动立即跑一次；必须带 Idempotency-Key，同一键重复提交返回同一运行。"""
    row = _require_schedule(ctx)
    key = ctx.headers.get("idempotency-key", "").strip()
    if not key:
        raise ContractError(ErrorCode.BAD_REQUEST, "缺少 Idempotency-Key 请求头")
    if not JobTemplateRepository(ctx.connection).get(str(row["job_template_id"])):
        raise ContractError(ErrorCode.NOT_FOUND, f"作业模板不存在：{row['job_template_id']}")
    with transaction(ctx.connection):
        record, created = _manual_run(ctx.connection, ctx.clock, row, key)
    return ApiResponse(
        status=201 if created else 200,
        payload={"run": run_json(record)},
    )


def _manual_run(
    connection: sqlite3.Connection,
    clock: Clock,
    row: Mapping[str, Any],
    key: str,
) -> tuple[JobRunRecord, bool]:
    """事务内建手动运行并登记幂等键；键已命中时返回既有运行。"""
    hit = run_store.run_now_run_id(connection, key)
    if hit is not None:
        record = JobRunRepository(connection).get(hit)
        if record is not None:
            return record, False
    now = clock.utc_now()
    record, created = JobRunService(connection, clock).create_scheduled(
        NewRun(
            template_id=str(row["job_template_id"]),
            # docs/03 job_runs：手动运行的 schedule_id 为空
            schedule_id=None,
            scheduled_at=now,
            created_at=now,
            idempotency_key=f"manual:{key}",
            priority=int(row["priority"]),
            deadline_at=now + timedelta(minutes=int(row["deadline_minutes"])),
            detail={"trigger": "manual"},
        )
    )
    run_store.record_run_now(connection, key, record.id, at=now)
    return record, created


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
        created = catalog_store.insert_schedule(
            ctx.connection,
            {**values, "id": schedule_id},
            at=ctx.clock.utc_now(),
        )
    row = catalog_store.schedule_row(ctx.connection, schedule_id)
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
    check_if_match(ctx.headers.get("if-match"), content_etag(row))
    values = schedule_values(body_object(ctx), existing=row)
    _require_template(ctx, str(values["job_template_id"]))
    with transaction(ctx.connection):
        updated = catalog_store.update_schedule(
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
            removed = catalog_store.delete_schedule(ctx.connection, ctx.params["id"])
    except sqlite3.IntegrityError as error:
        raise ContractError(ErrorCode.CONFLICT, "调度仍被运行记录引用，不能删除") from error
    if not removed:
        raise ContractError(ErrorCode.NOT_FOUND, f"调度不存在：{ctx.params['id']}")
    return ApiResponse(status=204, payload=None)


def schedule_values(
    body: Mapping[str, Any],
    existing: Mapping[str, Any] | None,
) -> dict[str, Any]:
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
    merged["name"] = require_text(merged, "name")
    merged["job_template_id"] = require_text(merged, "job_template_id")
    kind = require_enum(merged, "kind", KINDS)
    merged["catch_up"] = require_enum(merged, "catch_up", CATCH_UPS)
    for key in _INT_MINUTES:
        merged[key] = require_int(merged, key, minimum=0)
    merged["priority"] = require_int(merged, "priority")
    for key in ("coalesce", "enabled"):
        merged[key] = require_bool(merged, key)
    merged["timezone"] = require_timezone(merged, "timezone")
    if kind == "cron":
        merged["expr"] = require_text(merged, "expr")
    elif kind == "interval":
        merged["expr"] = str(_interval_minutes(merged))
    # 字段表把 at 定义为带时区的 RFC3339 文本：kind=at 必填；其余 kind 给了非空值也照此校验。
    if kind == "at" or merged.get("at") is not None:
        merged["at"] = require_timestamp(merged, "at")
    if merged["window_minutes"] > merged["deadline_minutes"]:
        raise ContractError(ErrorCode.CONFIG, "window_minutes 不能超过 deadline_minutes")


def _interval_minutes(merged: dict[str, Any]) -> int:
    """interval 的 expr 存分钟数；请求体给整数，库里读出是数字文本，两者都收。"""
    value = merged.get("expr")
    if isinstance(value, bool):
        minutes: int | None = None
    elif isinstance(value, int):
        minutes = value
    elif isinstance(value, str) and value.strip().isdigit():
        minutes = int(value)
    else:
        minutes = None
    if minutes is None or minutes <= 0:
        raise ContractError(ErrorCode.BAD_REQUEST, "interval 调度的 expr 必须是正整数分钟数")
    return minutes


def _require_schedule(ctx: RequestCtx) -> ScheduleJson:
    row = catalog_store.schedule_row(ctx.connection, ctx.params["id"])
    if row is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"调度不存在：{ctx.params['id']}")
    return row


def _require_template(ctx: RequestCtx, template_id: str) -> None:
    if not catalog_store.job_template_exists(ctx.connection, template_id):
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            f"job_template_id 不存在：{template_id}",
        )


def _new_schedule_id(idempotency_key: str | None) -> str:
    """幂等键决定 id：同 key 重放命中同一行；无 key 每次新 id。"""
    if idempotency_key is None or not idempotency_key.strip():
        return uuid4().hex
    return uuid5(NAMESPACE_URL, f"schedule:{idempotency_key}").hex


__all__ = [
    "create_schedule",
    "delete_schedule",
    "get_schedule",
    "list_schedules",
    "put_schedule",
    "run_now",
    "schedule_values",
]
