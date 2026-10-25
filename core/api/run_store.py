"""运行与快照数据访问：运行列表、在途/队列查询、最近结论与手动触发幂等键。

查询结果直接映射成端点应答的 snake_case JSON 结构；状态迁移走 core/jobs。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict

from storage.repositories.iso_time import format_at
from storage.repositories.sql import optional_str


def run_now_run_id(connection: sqlite3.Connection, key: str) -> str | None:
    """按 Idempotency-Key 查 run_now 作用域命中的运行；未命中返回 None。"""
    row = connection.execute(
        "SELECT run_id FROM request_idempotency WHERE key = ? AND scope = 'run_now'",
        (key,),
    ).fetchone()
    return None if row is None else str(row["run_id"])


def record_run_now(
    connection: sqlite3.Connection,
    key: str,
    run_id: str,
    *,
    at: datetime,
) -> None:
    """登记幂等键与命中的运行；键已存在时保留原映射。"""
    connection.execute(
        "INSERT INTO request_idempotency (key, scope, run_id, created_at) "
        "VALUES (?, 'run_now', ?, ?) ON CONFLICT (key) DO NOTHING",
        (key, run_id, format_at(at)),
    )


class RunJson(TypedDict):
    """job_runs 一行的应答形态（docs/03 字段表）。"""

    id: str
    template_id: str
    schedule_id: str | None
    scheduled_at: str | None
    created_at: str
    idempotency_key: str
    state: str
    attempt: int
    priority: int
    deadline_at: str | None
    started_at: str | None
    finished_at: str | None
    verdict_id: str | None
    error_code: str | None
    detail: Mapping[str, Any]


class VerdictJson(TypedDict):
    """verdicts 的摘要形态，/status 的最近结论一栏。"""

    id: str
    run_id: str
    status: str
    confidence: float
    reason: str
    created_at: str


_ACTIVE_STATES = (
    "scheduled",
    "queued",
    "preparing",
    "waiting",
    "executing",
    "verifying",
    "cleaning",
    "recovering",
)


def active_run_rows(connection: sqlite3.Connection) -> list[RunJson]:
    """全部非终态运行，按创建时间升序；供 /status 的当前运行一栏。"""
    marks = ", ".join("?" for _ in _ACTIVE_STATES)
    rows = connection.execute(
        f"SELECT {_RUN_COLUMNS} FROM job_runs WHERE state IN ({marks}) ORDER BY created_at, id",
        _ACTIVE_STATES,
    ).fetchall()
    return [_run_json(row) for row in rows]


def queued_run_rows(connection: sqlite3.Connection) -> list[RunJson]:
    """排队中的运行，按出队顺序（docs/03：priority 降序、时间升序）。"""
    rows = connection.execute(
        f"SELECT {_RUN_COLUMNS} FROM job_runs WHERE state = 'queued' "
        "ORDER BY priority DESC, scheduled_at ASC, created_at ASC"
    ).fetchall()
    return [_run_json(row) for row in rows]


def recent_verdict_rows(connection: sqlite3.Connection, limit: int = 10) -> list[VerdictJson]:
    """最近的结论摘要，新的在前。"""
    rows = connection.execute(
        "SELECT id, run_id, status, confidence, reason, created_at FROM verdicts "
        "ORDER BY created_at DESC, id LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "id": str(row["id"]),
            "run_id": str(row["run_id"]),
            "status": str(row["status"]),
            "confidence": float(row["confidence"]),
            "reason": str(row["reason"]),
            "created_at": str(row["created_at"]),
        }
        for row in rows
    ]


_RUN_COLUMNS = (
    "id, template_id, schedule_id, scheduled_at, created_at, idempotency_key, "
    "state, attempt, priority, deadline_at, started_at, finished_at, verdict_id, "
    "error_code, detail"
)


@dataclass(frozen=True, slots=True)
class RunFilters:
    """GET /runs 的过滤条件；时间按 created_at 过滤（docs/03：状态、时间、模板）。"""

    state: str | None = None
    template_id: str | None = None
    since: str | None = None
    until: str | None = None
    limit: int = 100


def run_rows(connection: sqlite3.Connection, filters: RunFilters) -> list[RunJson]:
    """按过滤条件取运行列表，新的在前。"""
    clauses: list[str] = []
    params: list[Any] = []
    if filters.state is not None:
        clauses.append("state = ?")
        params.append(filters.state)
    if filters.template_id is not None:
        clauses.append("template_id = ?")
        params.append(filters.template_id)
    if filters.since is not None:
        clauses.append("created_at >= ?")
        params.append(filters.since)
    if filters.until is not None:
        clauses.append("created_at <= ?")
        params.append(filters.until)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = connection.execute(
        f"SELECT {_RUN_COLUMNS} FROM job_runs{where} ORDER BY created_at DESC, id LIMIT ?",
        (*params, filters.limit),
    ).fetchall()
    return [_run_json(row) for row in rows]


def _run_json(row: sqlite3.Row) -> RunJson:
    return {
        "id": str(row["id"]),
        "template_id": str(row["template_id"]),
        "schedule_id": optional_str(row["schedule_id"]),
        "scheduled_at": optional_str(row["scheduled_at"]),
        "created_at": str(row["created_at"]),
        "idempotency_key": str(row["idempotency_key"]),
        "state": str(row["state"]),
        "attempt": int(row["attempt"]),
        "priority": int(row["priority"]),
        "deadline_at": optional_str(row["deadline_at"]),
        "started_at": optional_str(row["started_at"]),
        "finished_at": optional_str(row["finished_at"]),
        "verdict_id": optional_str(row["verdict_id"]),
        "error_code": optional_str(row["error_code"]),
        "detail": json.loads(str(row["detail"])),
    }


__all__ = [
    "RunFilters",
    "RunJson",
    "VerdictJson",
    "active_run_rows",
    "queued_run_rows",
    "recent_verdict_rows",
    "record_run_now",
    "run_now_run_id",
    "run_rows",
]
