"""插件配置仓储：当前 payload、自增修订号与每次保存的变更账。

当前配置存 `plugins.config`，修订号存 `plugins.config_revision`；每次保存把
相对上一版的差异记一行 `config_revisions`，供复盘「谁在哪一刻改了什么」。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from storage.repositories.iso_time import format_at
from storage.repositories.json_columns import dump_json, load_json_object

_ROW_QUERY = "SELECT id, config, config_revision FROM plugins WHERE id = ?"


@dataclass(frozen=True, slots=True)
class PluginConfigRow:
    """一个插件的当前配置：payload 原样保存，revision 每次保存自增。"""

    plugin_id: str
    config: Mapping[str, object]
    revision: int


class PluginConfigRepository:
    """plugins 表的配置读写；写入方是配置端点，读方是端点与后续投影。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get(self, plugin_id: str) -> PluginConfigRow | None:
        """按 id 取当前配置；插件不存在返回 None。"""
        row = self._connection.execute(_ROW_QUERY, (plugin_id,)).fetchone()
        return None if row is None else _row_to_record(row)

    def save(
        self,
        plugin_id: str,
        config: Mapping[str, object],
        *,
        at: datetime,
    ) -> PluginConfigRow | None:
        """覆盖当前配置：修订号 +1 并记一条变更账。

        Returns:
            保存后的配置行；插件不存在返回 None。

        Raises:
            sqlite3.Error: 更新或写变更账失败，由调用方的事务回滚。
        """
        current = self.get(plugin_id)
        if current is None:
            return None
        revision = current.revision + 1
        self._connection.execute(
            "UPDATE plugins SET config = ?, config_revision = ? WHERE id = ?",
            (dump_json(config), revision, plugin_id),
        )
        self._connection.execute(
            "INSERT INTO config_revisions (id, plugin_id, target, revision, diff, applied_at, "
            "result) VALUES (?, ?, 'plugin', ?, ?, ?, 'applied')",
            (
                uuid4().hex,
                plugin_id,
                revision,
                dump_json(_diff(current.config, config)),
                format_at(at),
            ),
        )
        return PluginConfigRow(plugin_id=plugin_id, config=dict(config), revision=revision)


def _diff(before: Mapping[str, object], after: Mapping[str, object]) -> dict[str, object]:
    """两版配置的差异：新增键、删除键与改值键各带两侧的值。"""
    added = {key: after[key] for key in sorted(set(after) - set(before))}
    removed = {key: before[key] for key in sorted(set(before) - set(after))}
    changed = {
        key: {"from": before[key], "to": after[key]}
        for key in sorted(set(before) & set(after))
        if before[key] != after[key]
    }
    return {"added": added, "removed": removed, "changed": changed}


def _row_to_record(row: sqlite3.Row) -> PluginConfigRow:
    return PluginConfigRow(
        plugin_id=str(row["id"]),
        config=load_json_object(row["config"], "插件 config"),
        revision=int(row["config_revision"]),
    )


__all__ = ["PluginConfigRepository", "PluginConfigRow"]
