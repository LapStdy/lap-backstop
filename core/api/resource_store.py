"""资源与租约数据访问：只读列表。

行由租约管理器（core/resources）写入；本层只读并映射成 JSON 结构。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any, TypedDict

from storage.repositories.sql import optional_str


class ResourceJson(TypedDict):
    """resources 一行的应答形态（docs/03 字段表）。"""

    id: str
    type: str
    name: str
    capacity: int
    detail: Mapping[str, Any]


class LeaseJson(TypedDict):
    """resource_leases 一行的应答形态（docs/03 字段表）。"""

    id: str
    resource_id: str
    run_id: str
    owner: str
    state: str
    acquired_at: str
    expires_at: str
    renewed_at: str | None


_RESOURCE_QUERY = "SELECT id, type, name, capacity, detail FROM resources"


def resource_rows(connection: sqlite3.Connection) -> list[ResourceJson]:
    """资源列表，按 id 排序；未持有租约的资源同样列出，是否空闲由租约行推断。"""
    rows = connection.execute(f"{_RESOURCE_QUERY} ORDER BY id").fetchall()
    return [_resource_json(row) for row in rows]


_LEASE_QUERY = (
    "SELECT id, resource_id, run_id, owner, state, acquired_at, expires_at, renewed_at "
    "FROM resource_leases"
)


def lease_rows(connection: sqlite3.Connection) -> list[LeaseJson]:
    """租约列表，按到期时间与 id 排序；表里有就列，不按状态筛选。"""
    rows = connection.execute(f"{_LEASE_QUERY} ORDER BY expires_at, id").fetchall()
    return [_lease_json(row) for row in rows]


def _resource_json(row: sqlite3.Row) -> ResourceJson:
    return {
        "id": str(row["id"]),
        "type": str(row["type"]),
        "name": str(row["name"]),
        "capacity": int(row["capacity"]),
        "detail": json.loads(str(row["detail"])),
    }


def _lease_json(row: sqlite3.Row) -> LeaseJson:
    return {
        "id": str(row["id"]),
        "resource_id": str(row["resource_id"]),
        "run_id": str(row["run_id"]),
        "owner": str(row["owner"]),
        "state": str(row["state"]),
        "acquired_at": str(row["acquired_at"]),
        "expires_at": str(row["expires_at"]),
        "renewed_at": optional_str(row["renewed_at"]),
    }


__all__ = ["LeaseJson", "ResourceJson", "lease_rows", "resource_rows"]
