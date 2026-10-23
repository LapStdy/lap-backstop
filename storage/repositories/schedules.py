"""调度仓储：读取启用调度、推进持久化的 next_run_at。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime

from storage.repositories.iso_time import format_at, parse_at, parse_optional_at
from storage.repositories.sql import optional_str

_COLUMNS = (
    "id, name, kind, expr, at, timezone, catch_up, window_minutes, coalesce, "
    "priority, deadline_minutes, preheat_minutes, keep_warm_minutes, "
    "job_template_id, enabled, next_run_at, created_at"
)


@dataclass(frozen=True, slots=True)
class ScheduleRow:
    """schedules 的一行；时间字段一律带时区的 UTC。"""

    id: str
    name: str
    kind: str
    expr: str | None
    at: datetime | None
    timezone: str
    catch_up: str
    window_minutes: int
    coalesce: bool
    priority: int
    deadline_minutes: int
    preheat_minutes: int
    keep_warm_minutes: int
    job_template_id: str
    enabled: bool
    next_run_at: datetime | None
    created_at: datetime


class ScheduleRepository:
    """schedules 表的读写；业务语义由 core/scheduler 负责。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def enabled(self) -> tuple[ScheduleRow, ...]:
        """所有启用的调度，按 next_run_at 升序（空值在前）。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM schedules WHERE enabled = 1 "
            "ORDER BY next_run_at IS NOT NULL, next_run_at",
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def get(self, schedule_id: str) -> ScheduleRow | None:
        """按 id 取调度；不存在返回 None。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM schedules WHERE id = ?",
            (schedule_id,),
        ).fetchone()
        return None if row is None else _row_to_row(row)

    def set_next_run(
        self,
        schedule_id: str,
        next_run_at: datetime | None,
        *,
        updated_at: datetime,
    ) -> None:
        """推进或清空下次发生点。"""
        self._connection.execute(
            "UPDATE schedules SET next_run_at = ?, updated_at = ? WHERE id = ?",
            (
                None if next_run_at is None else format_at(next_run_at),
                format_at(updated_at),
                schedule_id,
            ),
        )


def _row_to_row(row: sqlite3.Row) -> ScheduleRow:
    return ScheduleRow(
        id=str(row["id"]),
        name=str(row["name"]),
        kind=str(row["kind"]),
        expr=optional_str(row["expr"]),
        at=parse_optional_at(optional_str(row["at"])),
        timezone=str(row["timezone"]),
        catch_up=str(row["catch_up"]),
        window_minutes=int(row["window_minutes"]),
        coalesce=bool(row["coalesce"]),
        priority=int(row["priority"]),
        deadline_minutes=int(row["deadline_minutes"]),
        preheat_minutes=int(row["preheat_minutes"]),
        keep_warm_minutes=int(row["keep_warm_minutes"]),
        job_template_id=str(row["job_template_id"]),
        enabled=bool(row["enabled"]),
        next_run_at=parse_optional_at(optional_str(row["next_run_at"])),
        created_at=parse_at(str(row["created_at"])),
    )
