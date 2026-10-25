"""API 层数据访问：settings 读写、鉴权令牌、服务实例标识与调度持久化。

本卡不改 storage/；API 层专用的 SQL 集中在这里，查询结果直接映射成
snake_case 的 JSON 结构。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
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
    "delete_schedule",
    "ensure_server_id",
    "insert_schedule",
    "job_template_exists",
    "put_settings",
    "schedule_row",
    "schedule_rows",
    "update_schedule",
]


_SCHEDULE_COLUMNS = (
    "id, name, kind, expr, at, timezone, catch_up, window_minutes, coalesce, "
    "priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
    "job_template_id, enabled, next_run_at, created_at, updated_at"
)


def job_template_exists(connection: sqlite3.Connection, template_id: str) -> bool:
    """模板 id 是否存在；新建调度时校验外键引用。"""
    row = connection.execute(
        "SELECT 1 FROM job_templates WHERE id = ?",
        (template_id,),
    ).fetchone()
    return row is not None


def schedule_row(connection: sqlite3.Connection, schedule_id: str) -> dict[str, Any] | None:
    """按 id 取调度的 JSON 形态；不存在返回 None。"""
    row = connection.execute(
        f"SELECT {_SCHEDULE_COLUMNS} FROM schedules WHERE id = ?",
        (schedule_id,),
    ).fetchone()
    return None if row is None else _schedule_json(row)


def schedule_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """全部调度，按创建时间排序。"""
    rows = connection.execute(
        f"SELECT {_SCHEDULE_COLUMNS} FROM schedules ORDER BY created_at, id"
    ).fetchall()
    return [_schedule_json(row) for row in rows]


def insert_schedule(
    connection: sqlite3.Connection,
    values: Mapping[str, Any],
    *,
    at: datetime,
) -> bool:
    """写入一条调度；id 冲突（幂等键命中）时保留原行并返回 False。"""
    moment = format_at(at)
    cursor = connection.execute(
        "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, "
        "window_minutes, coalesce, priority, deadline_minutes, preheat_minutes, "
        "keep_warm_minutes, job_template_id, enabled, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (id) DO NOTHING",
        (values["id"], *_schedule_params(values), moment, moment),
    )
    return cursor.rowcount == 1


def update_schedule(
    connection: sqlite3.Connection,
    schedule_id: str,
    values: Mapping[str, Any],
    *,
    at: datetime,
) -> bool:
    """整行覆盖更新；行不存在返回 False。"""
    cursor = connection.execute(
        "UPDATE schedules SET name = ?, kind = ?, expr = ?, at = ?, timezone = ?, "
        "catch_up = ?, window_minutes = ?, coalesce = ?, priority = ?, "
        "deadline_minutes = ?, preheat_minutes = ?, keep_warm_minutes = ?, "
        "job_template_id = ?, enabled = ?, updated_at = ? WHERE id = ?",
        (*_schedule_params(values), format_at(at), schedule_id),
    )
    return cursor.rowcount == 1


def delete_schedule(connection: sqlite3.Connection, schedule_id: str) -> bool:
    """删除调度；行不存在返回 False，仍被运行引用时抛 IntegrityError。"""
    cursor = connection.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
    return cursor.rowcount == 1


def _schedule_params(values: Mapping[str, Any]) -> tuple[Any, ...]:
    """JSON 形态到列值的转换：布尔落 0/1，其余原样。"""
    return (
        values["name"],
        values["kind"],
        values.get("expr"),
        values.get("at"),
        values["timezone"],
        values["catch_up"],
        int(values["window_minutes"]),
        int(values["coalesce"]),
        int(values["priority"]),
        int(values["deadline_minutes"]),
        int(values["preheat_minutes"]),
        int(values["keep_warm_minutes"]),
        values["job_template_id"],
        int(values["enabled"]),
    )


def _schedule_json(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "name": str(row["name"]),
        "kind": str(row["kind"]),
        "expr": optional_str(row["expr"]),
        "at": optional_str(row["at"]),
        "timezone": str(row["timezone"]),
        "catch_up": str(row["catch_up"]),
        "window_minutes": int(row["window_minutes"]),
        "coalesce": bool(row["coalesce"]),
        "priority": int(row["priority"]),
        "deadline_minutes": int(row["deadline_minutes"]),
        "preheat_minutes": int(row["preheat_minutes"]),
        "keep_warm_minutes": int(row["keep_warm_minutes"]),
        "job_template_id": str(row["job_template_id"]),
        "enabled": bool(row["enabled"]),
        "next_run_at": optional_str(row["next_run_at"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }
