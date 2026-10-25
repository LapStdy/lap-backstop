"""目录资源持久化：调度与作业模板的行级读写。

查询与写入的 JSON 形态由 docs/03 的 schedules 与 job_templates 字段表决定；
payload 字段（config、策略）按插件 schema 原样序列化，本层不解释。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from typing import Any, TypedDict

from storage.repositories.iso_time import format_at
from storage.repositories.sql import optional_str


class TemplateJson(TypedDict):
    """job_templates 一行的应答形态（docs/03 字段表）。"""

    id: str
    name: str
    plugin_id: str
    environment_id: str
    config: Mapping[str, Any]
    config_schema_version: str
    verification_policy: Mapping[str, Any]
    recovery_policy: Mapping[str, Any]
    enabled: bool
    created_at: str
    updated_at: str


_TEMPLATE_COLUMNS = (
    "id, name, plugin_id, environment_id, config, config_schema_version, "
    "verification_policy, recovery_policy, enabled, created_at, updated_at"
)


def template_row(connection: sqlite3.Connection, template_id: str) -> TemplateJson | None:
    """按 id 取模板的 JSON 形态；不存在返回 None。"""
    row = connection.execute(
        f"SELECT {_TEMPLATE_COLUMNS} FROM job_templates WHERE id = ?",
        (template_id,),
    ).fetchone()
    return None if row is None else _template_json(row)


def template_rows(connection: sqlite3.Connection) -> list[TemplateJson]:
    """全部模板，按创建时间排序。"""
    rows = connection.execute(
        f"SELECT {_TEMPLATE_COLUMNS} FROM job_templates ORDER BY created_at, id"
    ).fetchall()
    return [_template_json(row) for row in rows]


def insert_template(
    connection: sqlite3.Connection,
    values: Mapping[str, Any],
    *,
    at: datetime,
) -> None:
    """写入一条模板；id 由调用方生成。"""
    moment = format_at(at)
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (values["id"], *_template_params(values), moment, moment),
    )


def update_template(
    connection: sqlite3.Connection,
    template_id: str,
    values: Mapping[str, Any],
    *,
    at: datetime,
) -> bool:
    """整行覆盖更新；行不存在返回 False。"""
    cursor = connection.execute(
        "UPDATE job_templates SET name = ?, plugin_id = ?, environment_id = ?, "
        "config = ?, config_schema_version = ?, verification_policy = ?, "
        "recovery_policy = ?, enabled = ?, updated_at = ? WHERE id = ?",
        (*_template_params(values), format_at(at), template_id),
    )
    return cursor.rowcount == 1


def delete_template(connection: sqlite3.Connection, template_id: str) -> bool:
    """删除模板；行不存在返回 False，仍被调度或运行引用时抛 IntegrityError。"""
    cursor = connection.execute("DELETE FROM job_templates WHERE id = ?", (template_id,))
    return cursor.rowcount == 1


def _template_params(values: Mapping[str, Any]) -> tuple[Any, ...]:
    """JSON 形态到列值的转换；payload 字段序列化成 JSON 文本。"""
    return (
        values["name"],
        values["plugin_id"],
        values["environment_id"],
        _dump_payload(values.get("config")),
        values["config_schema_version"],
        _dump_payload(values.get("verification_policy")),
        _dump_payload(values.get("recovery_policy")),
        int(values["enabled"]),
    )


def _dump_payload(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _template_json(row: sqlite3.Row) -> TemplateJson:
    return {
        "id": str(row["id"]),
        "name": str(row["name"]),
        "plugin_id": str(row["plugin_id"]),
        "environment_id": str(row["environment_id"]),
        "config": json.loads(str(row["config"])),
        "config_schema_version": str(row["config_schema_version"]),
        "verification_policy": json.loads(str(row["verification_policy"])),
        "recovery_policy": json.loads(str(row["recovery_policy"])),
        "enabled": bool(row["enabled"]),
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


class ScheduleJson(TypedDict):
    """schedules 一行的应答形态（docs/03 字段表）。"""

    id: str
    name: str
    kind: str
    expr: str | None
    at: str | None
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
    next_run_at: str | None
    created_at: str
    updated_at: str


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


def schedule_row(connection: sqlite3.Connection, schedule_id: str) -> ScheduleJson | None:
    """按 id 取调度的 JSON 形态；不存在返回 None。"""
    row = connection.execute(
        f"SELECT {_SCHEDULE_COLUMNS} FROM schedules WHERE id = ?",
        (schedule_id,),
    ).fetchone()
    return None if row is None else _schedule_json(row)


def schedule_rows(connection: sqlite3.Connection) -> list[ScheduleJson]:
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


def _schedule_json(row: sqlite3.Row) -> ScheduleJson:
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


__all__ = [
    "ScheduleJson",
    "TemplateJson",
    "delete_schedule",
    "delete_template",
    "insert_schedule",
    "insert_template",
    "job_template_exists",
    "schedule_row",
    "schedule_rows",
    "template_row",
    "template_rows",
    "update_schedule",
    "update_template",
]
