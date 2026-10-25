"""配置与服务身份数据访问：settings 读写、鉴权令牌与服务实例标识。

SQL 结果直接映射成端点应答的 snake_case JSON 结构。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import uuid4

from storage.repositories.iso_time import format_at

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
