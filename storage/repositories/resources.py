"""资源仓储：注册资源与容量。environment 类型容量固定为 1。"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass

from storage.repositories.json_columns import dump_json, load_json_object

RESOURCE_TYPES = frozenset({"environment", "account", "gpu", "custom"})
_COLUMNS = "id, type, name, capacity, detail"


@dataclass(frozen=True, slots=True)
class ResourceRow:
    """resources 的一行。"""

    id: str
    type: str
    name: str
    capacity: int
    detail: Mapping[str, object]


class ResourceRepository:
    """resources 表的读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def ensure(
        self,
        resource_id: str,
        *,
        resource_type: str,
        name: str,
        capacity: int = 1,
        detail: Mapping[str, object] | None = None,
    ) -> ResourceRow:
        """注册资源；已存在时原样返回。

        environment 类型的容量固定为 1，其余类型容量必须为正数。
        """
        if resource_type not in RESOURCE_TYPES:
            raise ValueError(f"未知资源类型：{resource_type!r}")
        effective = 1 if resource_type == "environment" else capacity
        if effective < 1:
            raise ValueError("资源容量必须为正整数")
        if self.get(resource_id) is None:
            self._connection.execute(
                "INSERT INTO resources (id, type, name, capacity, detail) VALUES (?, ?, ?, ?, ?)",
                (
                    resource_id,
                    resource_type,
                    name,
                    effective,
                    dump_json(detail or {}),
                ),
            )
        return self._require(resource_id)

    def get(self, resource_id: str) -> ResourceRow | None:
        """按 id 取资源；不存在返回 None。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM resources WHERE id = ?",
            (resource_id,),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    def list_all(self) -> tuple[ResourceRow, ...]:
        """全部资源，按 id 排序。"""
        rows = self._connection.execute(f"SELECT {_COLUMNS} FROM resources ORDER BY id").fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def _require(self, resource_id: str) -> ResourceRow:
        row = self.get(resource_id)
        if row is None:
            raise sqlite3.DatabaseError(f"资源不存在：{resource_id}")
        return row


def _row_to_record(row: sqlite3.Row) -> ResourceRow:
    return ResourceRow(
        id=str(row["id"]),
        type=str(row["type"]),
        name=str(row["name"]),
        capacity=int(row["capacity"]),
        detail=load_json_object(row["detail"], "资源 detail"),
    )


__all__ = ["RESOURCE_TYPES", "ResourceRepository", "ResourceRow"]
