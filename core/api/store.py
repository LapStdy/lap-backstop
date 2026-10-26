"""配置与服务身份数据访问：settings 读写、鉴权令牌与服务实例标识。

SQL 结果直接映射成端点应答的 snake_case JSON 结构。
"""

from __future__ import annotations

import json
import secrets
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import uuid4

from storage.repositories.iso_time import format_at

TOKEN_KEY = "api.token"
SERVER_ID_KEY = "server.id"
# 32 字节熵的 URL-safe 随机值（43 字符）：离线穷举不可行（docs/08 鉴权）。
TOKEN_BYTES = 32


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
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True)
        _upsert_setting(connection, key, payload, moment)


def _upsert_setting(connection: sqlite3.Connection, key: str, value_json: str, moment: str) -> None:
    """写入或覆盖一个配置项；settings 表以 key 为主键。"""
    connection.execute(
        "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value, "
        "updated_at = excluded.updated_at",
        (key, value_json, moment),
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
    _upsert_setting(connection, SERVER_ID_KEY, json.dumps(server_id), format_at(at))
    return server_id


def ensure_token(connection: sqlite3.Connection, *, at: datetime) -> str:
    """取 Bearer token；未配置时生成强随机值落库，已有配置原样返回。

    与 ensure_server_id 同一幂等口径：settings 里已有可用 token（非空字符串）
    不覆盖。生成值只经返回值交给调用方，不进日志、不进事件。
    """
    existing = bearer_token(connection)
    if existing is not None:
        return existing
    token = secrets.token_urlsafe(TOKEN_BYTES)
    put_settings(connection, {TOKEN_KEY: token}, at=at)
    return token


__all__ = [
    "SERVER_ID_KEY",
    "TOKEN_BYTES",
    "TOKEN_KEY",
    "all_settings",
    "bearer_token",
    "ensure_server_id",
    "ensure_token",
    "put_settings",
]
