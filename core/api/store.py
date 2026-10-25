"""配置与服务身份数据访问：settings 读写、鉴权令牌与服务实例标识。

SQL 结果直接映射成端点应答的 snake_case JSON 结构。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

from storage.repositories.iso_time import format_at
from storage.repositories.sql import optional_str

TOKEN_KEY = "api.token"
SERVER_ID_KEY = "server.id"


def all_settings(connection: sqlite3.Connection) -> dict[str, Any]:
    """全部配置项；value 按落库的 JSON 原样解析。"""
    rows = connection.execute("SELECT key, value FROM settings ORDER BY key").fetchall()
    return {str(row["key"]): json.loads(str(row["value"])) for row in rows}


def put_settings(
    connection: sqlite3.Connection,
    values: Mapping[str, Any],
    *,
    at: datetime,
) -> None:
    """整批写入配置项；事务边界由调用方包裹。"""
    moment = format_at(at)
    for key, value in values.items():
        connection.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value, "
            "updated_at = excluded.updated_at",
            (key, json.dumps(value, ensure_ascii=False, sort_keys=True), moment),
        )


def bearer_token(connection: sqlite3.Connection) -> str | None:
    """settings 里的固定 Bearer token；未配置或不是非空字符串视为未配置。"""
    row = connection.execute(
        "SELECT value FROM settings WHERE key = ?",
        (TOKEN_KEY,),
    ).fetchone()
    if row is None:
        return None
    token = json.loads(str(row["value"]))
    return token if isinstance(token, str) and token else None


def ensure_server_id(connection: sqlite3.Connection, *, at: datetime) -> str:
    """取服务实例标识；首次访问生成并落库，进程重启后保持不变。"""
    row = connection.execute(
        "SELECT value FROM settings WHERE key = ?",
        (SERVER_ID_KEY,),
    ).fetchone()
    if row is not None:
        value = json.loads(str(row["value"]))
        if isinstance(value, str) and value:
            return value
    server_id = uuid4().hex
    connection.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        (SERVER_ID_KEY, json.dumps(server_id), format_at(at)),
    )
    return server_id


__all__ = [
    "SERVER_ID_KEY",
    "TOKEN_KEY",
    "all_settings",
    "bearer_token",
    "ensure_server_id",
    "put_settings",
]


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


def active_run_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """全部非终态运行，按创建时间升序；供 /status 的当前运行一栏。"""
    marks = ", ".join("?" for _ in _ACTIVE_STATES)
    rows = connection.execute(
        f"SELECT {_RUN_COLUMNS} FROM job_runs WHERE state IN ({marks}) ORDER BY created_at, id",
        _ACTIVE_STATES,
    ).fetchall()
    return [_run_json(row) for row in rows]


def queued_run_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """排队中的运行，按出队顺序（docs/03：priority 降序、时间升序）。"""
    rows = connection.execute(
        f"SELECT {_RUN_COLUMNS} FROM job_runs WHERE state = 'queued' "
        "ORDER BY priority DESC, scheduled_at ASC, created_at ASC"
    ).fetchall()
    return [_run_json(row) for row in rows]


def recent_verdict_rows(connection: sqlite3.Connection, limit: int = 10) -> list[dict[str, Any]]:
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


_ENVIRONMENT_QUERY = (
    "SELECT id, provider_id, name, state, last_probe_at, warm_until, detail FROM environments"
)


def environment_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """环境列表；状态语义由 core/environments 维护，这里只读。"""
    rows = connection.execute(f"{_ENVIRONMENT_QUERY} ORDER BY id").fetchall()
    return [_environment_json(row) for row in rows]


def environment_row(connection: sqlite3.Connection, environment_id: str) -> dict[str, Any] | None:
    """按 id 取环境；不存在返回 None。"""
    row = connection.execute(
        f"{_ENVIRONMENT_QUERY} WHERE id = ?",
        (environment_id,),
    ).fetchone()
    return None if row is None else _environment_json(row)


def _environment_json(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "provider_id": str(row["provider_id"]),
        "name": str(row["name"]),
        "state": str(row["state"]),
        "last_probe_at": optional_str(row["last_probe_at"]),
        "warm_until": optional_str(row["warm_until"]),
        "detail": json.loads(str(row["detail"])),
    }


def plugin_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """插件列表，按 id 排序；manifest 由内核加载时写入这张表。"""
    rows = connection.execute(f"{_PLUGIN_QUERY} ORDER BY id").fetchall()
    return [_plugin_json(row) for row in rows]


_PLUGIN_QUERY = (
    "SELECT id, name, version, api_version, capabilities, config_schema, "
    "error_map, requirements, enabled, installed_at FROM plugins"
)


def plugin_row(connection: sqlite3.Connection, plugin_id: str) -> dict[str, Any] | None:
    """按 id 取插件；不存在返回 None。"""
    row = connection.execute(f"{_PLUGIN_QUERY} WHERE id = ?", (plugin_id,)).fetchone()
    return None if row is None else _plugin_json(row)


def _plugin_json(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "name": str(row["name"]),
        "version": str(row["version"]),
        "api_version": int(row["api_version"]),
        "capabilities": json.loads(str(row["capabilities"])),
        "config_schema": json.loads(str(row["config_schema"])),
        "error_map": json.loads(str(row["error_map"])),
        "requirements": json.loads(str(row["requirements"])),
        "enabled": bool(row["enabled"]),
        "installed_at": str(row["installed_at"]),
    }


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


def run_rows(connection: sqlite3.Connection, filters: RunFilters) -> list[dict[str, Any]]:
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


def _run_json(row: sqlite3.Row) -> dict[str, Any]:
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
