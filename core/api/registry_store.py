"""环境与插件数据访问：只读列表与详情。

行由内核（插件加载器与环境服务）写入；本层只读并映射成 JSON 结构。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any, TypedDict

from storage.repositories.sql import optional_str


class EnvironmentJson(TypedDict):
    """environments 一行的应答形态（docs/03 字段表）。"""

    id: str
    provider_id: str
    name: str
    state: str
    last_probe_at: str | None
    warm_until: str | None
    detail: Mapping[str, Any]


class PluginJson(TypedDict):
    """plugins 一行的应答形态（docs/03 字段表）。"""

    id: str
    name: str
    version: str
    api_version: int
    capabilities: list[Any]
    config_schema: Mapping[str, Any]
    error_map: Mapping[str, Any]
    requirements: Mapping[str, Any]
    enabled: bool
    installed_at: str


_ENVIRONMENT_QUERY = (
    "SELECT id, provider_id, name, state, last_probe_at, warm_until, detail FROM environments"
)


def environment_rows(connection: sqlite3.Connection) -> list[EnvironmentJson]:
    """环境列表；状态语义由 core/environments 维护，这里只读。"""
    rows = connection.execute(f"{_ENVIRONMENT_QUERY} ORDER BY id").fetchall()
    return [_environment_json(row) for row in rows]


def environment_row(connection: sqlite3.Connection, environment_id: str) -> EnvironmentJson | None:
    """按 id 取环境；不存在返回 None。"""
    row = connection.execute(
        f"{_ENVIRONMENT_QUERY} WHERE id = ?",
        (environment_id,),
    ).fetchone()
    return None if row is None else _environment_json(row)


def _environment_json(row: sqlite3.Row) -> EnvironmentJson:
    return {
        "id": str(row["id"]),
        "provider_id": str(row["provider_id"]),
        "name": str(row["name"]),
        "state": str(row["state"]),
        "last_probe_at": optional_str(row["last_probe_at"]),
        "warm_until": optional_str(row["warm_until"]),
        "detail": json.loads(str(row["detail"])),
    }


def plugin_rows(connection: sqlite3.Connection) -> list[PluginJson]:
    """插件列表，按 id 排序；manifest 由内核加载时写入这张表。"""
    rows = connection.execute(f"{_PLUGIN_QUERY} ORDER BY id").fetchall()
    return [_plugin_json(row) for row in rows]


_PLUGIN_QUERY = (
    "SELECT id, name, version, api_version, capabilities, config_schema, "
    "error_map, requirements, enabled, installed_at FROM plugins"
)


def plugin_row(connection: sqlite3.Connection, plugin_id: str) -> PluginJson | None:
    """按 id 取插件；不存在返回 None。"""
    row = connection.execute(f"{_PLUGIN_QUERY} WHERE id = ?", (plugin_id,)).fetchone()
    return None if row is None else _plugin_json(row)


def _plugin_json(row: sqlite3.Row) -> PluginJson:
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


__all__ = [
    "EnvironmentJson",
    "PluginJson",
    "environment_row",
    "environment_rows",
    "plugin_row",
    "plugin_rows",
]
