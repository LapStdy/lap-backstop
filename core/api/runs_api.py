"""运行端点：列表、详情与叫停（docs/03 端点表与 job_runs 字段表）。

状态迁移与事件写入复用 core/jobs 的 JobRunService；本层只做过滤解析与 JSON 形态。
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from contracts import ContractError, ErrorCode, RunState
from core.api import store
from core.api.ctx import RequestCtx
from core.api.fields import require_timestamp
from core.api.wire import ApiResponse
from core.jobs.errors import IllegalRunTransitionError, RunNotFoundError
from core.jobs.run_service import JobRunService
from storage.repositories.evidence import EvidenceRepository, EvidenceRow
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.run_phases import RunPhaseRepository, RunPhaseRow
from storage.repositories.verdicts import VerdictRepository, VerdictRow

MAX_PAGE_SIZE = 500


def list_runs(ctx: RequestCtx) -> ApiResponse:
    """运行列表；支持 state、template_id、since、until 与 limit。"""
    filters = _filters(ctx)
    return ApiResponse(status=200, payload={"runs": store.run_rows(ctx.connection, filters)})


def get_run(ctx: RequestCtx) -> ApiResponse:
    """运行详情：运行本身加阶段、证据与结论。"""
    record = _require_run(ctx)
    connection = ctx.connection
    return ApiResponse(
        status=200,
        payload={
            "run": run_json(record),
            "phases": [_phase_json(p) for p in RunPhaseRepository(connection).for_run(record.id)],
            "evidence": [
                _evidence_json(e) for e in EvidenceRepository(connection).for_run(record.id)
            ],
            "verdicts": [
                _verdict_json(v) for v in VerdictRepository(connection).for_run(record.id)
            ],
        },
    )


def stop_run(ctx: RequestCtx) -> ApiResponse:
    """叫停运行：排队或未开始的立即取消，其余在阶段边界生效。"""
    reason = _stop_reason(ctx)
    service = JobRunService(ctx.connection, ctx.clock)
    try:
        record = service.request_cancel(ctx.params["id"], reason)
    except RunNotFoundError as error:
        raise ContractError(ErrorCode.NOT_FOUND, f"运行不存在：{ctx.params['id']}") from error
    except IllegalRunTransitionError as error:
        raise ContractError(ErrorCode.CONFLICT, str(error)) from error
    return ApiResponse(status=200, payload={"run": run_json(record)})


def run_json(record: JobRunRecord) -> dict[str, Any]:
    """JobRunRecord 的 JSON 形态。"""
    return {
        "id": record.id,
        "template_id": record.template_id,
        "schedule_id": record.schedule_id,
        "scheduled_at": _iso(record.scheduled_at),
        "created_at": record.created_at.isoformat(),
        "idempotency_key": record.idempotency_key,
        "state": record.state.value,
        "attempt": record.attempt,
        "priority": record.priority,
        "deadline_at": _iso(record.deadline_at),
        "started_at": _iso(record.started_at),
        "finished_at": _iso(record.finished_at),
        "verdict_id": record.verdict_id,
        "error_code": record.error_code,
        "detail": dict(record.detail),
    }


def _filters(ctx: RequestCtx) -> store.RunFilters:
    query = ctx.query
    state = query.get("state")
    if state is not None and state not in {item.value for item in RunState}:
        raise ContractError(ErrorCode.BAD_REQUEST, f"state 取值非法：{state}")
    limit = _page_size(query.get("limit"))
    return store.RunFilters(
        state=state,
        template_id=query.get("template_id"),
        since=_optional_timestamp(query, "since"),
        until=_optional_timestamp(query, "until"),
        limit=limit,
    )


def _page_size(raw: str | None) -> int:
    if raw is None:
        return 100
    try:
        limit = int(raw)
    except ValueError as error:
        raise ContractError(ErrorCode.BAD_REQUEST, f"limit 必须是整数：{raw}") from error
    if limit < 1 or limit > MAX_PAGE_SIZE:
        raise ContractError(ErrorCode.BAD_REQUEST, f"limit 必须在 1 到 {MAX_PAGE_SIZE} 之间")
    return limit


def _optional_timestamp(query: Mapping[str, str], key: str) -> str | None:
    return None if query.get(key) is None else require_timestamp(query, key)


def _stop_reason(ctx: RequestCtx) -> str:
    """可选的 JSON 对象体里带 reason；空体或空对象都按默认原因处理。"""
    if ctx.body is None:
        return "user_cancel"
    if not isinstance(ctx.body, dict):
        raise ContractError(ErrorCode.BAD_REQUEST, "请求体必须是 JSON 对象")
    value = ctx.body.get("reason")
    return "user_cancel" if not isinstance(value, str) or not value else value


def _require_run(ctx: RequestCtx) -> JobRunRecord:
    service = JobRunService(ctx.connection, ctx.clock)
    try:
        return service.get(ctx.params["id"])
    except RunNotFoundError as error:
        raise ContractError(ErrorCode.NOT_FOUND, f"运行不存在：{ctx.params['id']}") from error


def _iso(moment: datetime | None) -> str | None:
    return None if moment is None else moment.isoformat()


def _phase_json(phase: RunPhaseRow) -> dict[str, Any]:
    return {
        "id": phase.id,
        "phase": phase.phase,
        "attempt": phase.attempt,
        "state": phase.state.value,
        "started_at": _iso(phase.started_at),
        "finished_at": _iso(phase.finished_at),
        "timeout_seconds": phase.timeout_seconds,
        "detail": dict(phase.detail),
    }


def _evidence_json(item: EvidenceRow) -> dict[str, Any]:
    return {
        "id": item.id,
        "kind": item.kind.value,
        "source": item.source.value,
        "path": item.path,
        "sha256": item.sha256,
        "summary": item.summary,
        "created_at": item.created_at.isoformat(),
    }


def _verdict_json(item: VerdictRow) -> dict[str, Any]:
    return {
        "id": item.id,
        "status": item.status.value,
        "confidence": item.confidence,
        "reason": item.reason,
        "policy_id": item.policy_id,
        "checks": [dict(check) for check in item.checks],
        "evidence_ids": list(item.evidence_ids),
        "created_at": item.created_at.isoformat(),
    }


__all__ = ["get_run", "list_runs", "run_json", "stop_run"]
