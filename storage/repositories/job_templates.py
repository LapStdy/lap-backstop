"""作业模板仓储：插件配置、验证策略与恢复策略。"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from storage.repositories.json_columns import load_json_object

_COLUMNS = (
    "id, name, plugin_id, environment_id, config, config_schema_version, "
    "verification_policy, recovery_policy, enabled"
)


@dataclass(frozen=True, slots=True)
class JobTemplateRow:
    """job_templates 的一行；两个策略 payload 保持插件定义的结构。"""

    id: str
    name: str
    plugin_id: str
    environment_id: str
    config: Mapping[str, Any]
    config_schema_version: str
    verification_policy: Mapping[str, Any]
    recovery_policy: Mapping[str, Any]
    enabled: bool


class JobTemplateRepository:
    """job_templates 表的只读访问。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get(self, template_id: str) -> JobTemplateRow | None:
        """按 id 取模板；不存在返回 None。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM job_templates WHERE id = ?",
            (template_id,),
        ).fetchone()
        return None if row is None else _row_to_row(row)

    def list_all(self) -> tuple[JobTemplateRow, ...]:
        """全部模板，按 id 排序。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM job_templates ORDER BY id"
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)


def _row_to_row(row: sqlite3.Row) -> JobTemplateRow:
    return JobTemplateRow(
        id=str(row["id"]),
        name=str(row["name"]),
        plugin_id=str(row["plugin_id"]),
        environment_id=str(row["environment_id"]),
        config=load_json_object(row["config"], "模板 config"),
        config_schema_version=str(row["config_schema_version"]),
        verification_policy=load_json_object(
            row["verification_policy"], "模板 verification_policy"
        ),
        recovery_policy=load_json_object(row["recovery_policy"], "模板 recovery_policy"),
        enabled=bool(row["enabled"]),
    )


__all__ = ["JobTemplateRepository", "JobTemplateRow"]
