"""环境仓储：注册、状态迁移、探针时间与保温到期。"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from contracts import EnvironmentState
from storage.repositories.iso_time import format_at, parse_optional_at
from storage.repositories.json_columns import dump_json, load_json_object
from storage.repositories.sql import optional_str

_COLUMNS = "id, provider_id, name, state, last_probe_at, warm_until, detail"


@dataclass(frozen=True, slots=True)
class EnvironmentRow:
    """environments 的一行。"""

    id: str
    provider_id: str
    name: str
    state: EnvironmentState
    last_probe_at: datetime | None
    warm_until: datetime | None
    detail: Mapping[str, Any]


class EnvironmentRepository:
    """environments 表的读写；状态语义由 core/environments 负责。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def ensure(
        self,
        environment_id: str,
        *,
        provider_id: str,
        name: str,
    ) -> EnvironmentRow:
        """注册环境；已存在时原样返回，不覆盖当前状态。"""
        if self.get(environment_id) is None:
            self._connection.execute(
                "INSERT INTO environments (id, provider_id, name, state, detail) "
                "VALUES (?, ?, ?, ?, '{}')",
                (environment_id, provider_id, name, EnvironmentState.UNKNOWN.value),
            )
        return self._require(environment_id)

    def get(self, environment_id: str) -> EnvironmentRow | None:
        """按 id 取环境；不存在返回 None。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM environments WHERE id = ?",
            (environment_id,),
        ).fetchone()
        return None if row is None else _row_to_row(row)

    def list_all(self) -> tuple[EnvironmentRow, ...]:
        """全部环境，按 id 排序。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM environments ORDER BY id"
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def set_state(
        self,
        environment_id: str,
        state: EnvironmentState,
        *,
        at: datetime,
        detail: Mapping[str, Any],
    ) -> None:
        """写入新状态、最近探测时间与探针摘要。"""
        self._connection.execute(
            "UPDATE environments SET state = ?, last_probe_at = ?, detail = ? WHERE id = ?",
            (state.value, format_at(at), dump_json(detail), environment_id),
        )

    def touch(
        self,
        environment_id: str,
        *,
        at: datetime,
        detail: Mapping[str, Any],
    ) -> None:
        """状态未变时只刷新探测时间与摘要。"""
        self._connection.execute(
            "UPDATE environments SET last_probe_at = ?, detail = ? WHERE id = ?",
            (format_at(at), dump_json(detail), environment_id),
        )

    def set_warm_until(
        self,
        environment_id: str,
        warm_until: datetime | None,
    ) -> None:
        """写入或清空保温到期时间。"""
        self._connection.execute(
            "UPDATE environments SET warm_until = ? WHERE id = ?",
            (None if warm_until is None else format_at(warm_until), environment_id),
        )

    def warm_expired(self, now: datetime) -> tuple[EnvironmentRow, ...]:
        """保温已到期、仍处于 ready 的环境。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM environments "
            "WHERE state = ? AND warm_until IS NOT NULL AND warm_until <= ? "
            "ORDER BY warm_until",
            (EnvironmentState.READY.value, format_at(now)),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def _require(self, environment_id: str) -> EnvironmentRow:
        row = self.get(environment_id)
        if row is None:
            raise sqlite3.DatabaseError(f"环境不存在：{environment_id}")
        return row


def _row_to_row(row: sqlite3.Row) -> EnvironmentRow:
    return EnvironmentRow(
        id=str(row["id"]),
        provider_id=str(row["provider_id"]),
        name=str(row["name"]),
        state=EnvironmentState(str(row["state"])),
        last_probe_at=parse_optional_at(optional_str(row["last_probe_at"])),
        warm_until=parse_optional_at(optional_str(row["warm_until"])),
        detail=load_json_object(row["detail"], "环境 detail"),
    )


__all__ = ["EnvironmentRepository", "EnvironmentRow"]
