"""运行端点：列表、详情、叫停与证据文件（docs/03 端点表与 job_runs 字段表）。

状态迁移与事件写入复用 core/jobs 的 JobRunService；本层只做过滤解析与 JSON 形态。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Final, Protocol

from contracts import Clock, ContractError, ErrorCode, RunState
from core.api import run_store
from core.api.ctx import RequestCtx
from core.api.fields import require_timestamp
from core.api.run_store import RunJson
from core.api.wire import ApiResponse, FileBody
from core.events.evidence import EvidenceService
from core.jobs.errors import IllegalRunTransitionError, RunNotFoundError
from core.jobs.run_service import JobRunService
from storage.repositories.evidence import EvidenceRepository, EvidenceRow
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.run_phases import RunPhaseRepository, RunPhaseRow
from storage.repositories.verdicts import VerdictRepository, VerdictRow

MAX_PAGE_SIZE = 500

_CONTENT_TYPES: Final[Mapping[str, str]] = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".json": "application/json",
    ".log": "text/plain; charset=utf-8",
    ".txt": "text/plain; charset=utf-8",
    ".zip": "application/zip",
}
DEFAULT_CONTENT_TYPE: Final = "application/octet-stream"
# 410 是保留策略的结果，不产生错误码（docs/03），只带可读说明。
EVIDENCE_GONE_MESSAGE: Final = "证据文件已按保留策略清理，记录仍可查询"


class RunStopper(Protocol):
    """运行动作的注入面：HTTP 请求线程经此触达执行器，不认识 Supervisor。

    Raises:
        RunNotFoundError: 运行不存在。
        IllegalRunTransitionError: 运行已到终态，不能再取消。
    """

    def stop(self, run_id: str, reason: str = "user_cancel") -> JobRunRecord:
        """叫停运行并返回叫停后的运行行。"""
        ...


def list_runs(ctx: RequestCtx) -> ApiResponse:
    """运行列表；支持 state、template_id、since、until 与 limit。"""
    filters = _filters(ctx)
    return ApiResponse(status=200, payload={"runs": run_store.run_rows(ctx.connection, filters)})


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


def stop_run(ctx: RequestCtx, *, stopper: RunStopper | None = None) -> ApiResponse:
    """叫停运行：排队或未开始的立即取消，其余在阶段边界生效。

    内核实现由 serve 注入；未接入时 409，避免绕过执行器只写一条取消事件。

    Raises:
        ContractError: 叫停未接入（409）、运行不存在（404）、状态机拒绝（409）。
    """
    reason = _stop_reason(ctx)
    if stopper is None:
        raise ContractError(ErrorCode.CONFLICT, "运行叫停未接入内核")
    try:
        record = stopper.stop(ctx.params["id"], reason)
    except RunNotFoundError as error:
        raise ContractError(ErrorCode.NOT_FOUND, f"运行不存在：{ctx.params['id']}") from error
    except IllegalRunTransitionError as error:
        raise ContractError(ErrorCode.CONFLICT, str(error)) from error
    return ApiResponse(status=200, payload={"run": run_json(record)})


def get_evidence_file(ctx: RequestCtx) -> ApiResponse:
    """按证据 id 取回证据文件字节；只认本运行的证据，不跨运行泄露。

    记录不存在、不属于该运行或路径越界都按 E_NOT_FOUND 返回 404，不回显路径；
    记录在而文件已被保留策略删除时返回 410 与可读说明（docs/03：不产生错误码）。

    Raises:
        ContractError: 运行或证据不存在，错误码 E_NOT_FOUND。
    """
    _require_run(ctx)
    service = EvidenceService(ctx.connection, ctx.data_root / "evidence", ctx.clock)
    try:
        found = service.open_file(ctx.params["id"], ctx.params["name"])
    except FileNotFoundError:
        return ApiResponse(
            status=410,
            payload={"error": {"message": EVIDENCE_GONE_MESSAGE}},
        )
    except ValueError:
        # 越界路径按不存在处理：不回显解析后的位置，与记录缺失同口径
        found = None
    if found is None:
        raise ContractError(ErrorCode.NOT_FOUND, f"证据不存在：{ctx.params['name']}")
    return ApiResponse(
        status=200,
        payload=None,
        file=FileBody(
            content_type=_content_type(found.path),
            length=found.size,
            stream=found.stream,
        ),
    )


def _content_type(path: str) -> str:
    return _CONTENT_TYPES.get(PurePosixPath(path).suffix.lower(), DEFAULT_CONTENT_TYPE)


def run_json(record: JobRunRecord) -> RunJson:
    """运行行（docs/03 job_runs 字段表）的 JSON 形态；详情与列表共用同一形态。"""
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


def _filters(ctx: RequestCtx) -> run_store.RunFilters:
    query = ctx.query
    state = query.get("state")
    if state is not None and state not in {item.value for item in RunState}:
        raise ContractError(ErrorCode.BAD_REQUEST, f"state 取值非法：{state}")
    limit = _page_size(query.get("limit"))
    return run_store.RunFilters(
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
    if isinstance(value, str) and value:
        return value
    return "user_cancel"


def require_run(connection: sqlite3.Connection, clock: Clock, run_id: str) -> JobRunRecord:
    """取运行，不存在按 404 口径拒绝；普通端点与流式端点共用这一份判定。

    Raises:
        ContractError: 运行不存在，错误码 E_NOT_FOUND。
    """
    service = JobRunService(connection, clock)
    try:
        return service.get(run_id)
    except RunNotFoundError as error:
        raise ContractError(ErrorCode.NOT_FOUND, f"运行不存在：{run_id}") from error


def _require_run(ctx: RequestCtx) -> JobRunRecord:
    return require_run(ctx.connection, ctx.clock, ctx.params["id"])


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


__all__ = [
    "RunStopper",
    "get_evidence_file",
    "get_run",
    "list_runs",
    "require_run",
    "run_json",
    "stop_run",
]
