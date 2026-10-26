"""配置导出的数据收集：settings 全量键值（不含密钥键）与 plugins 概要。

JSON 文件导出（storage.db.backup）与 HTTP zip 导出（core.api.export_api）
共用这一份收集器，两条路径的内容口径不会漂移；密钥键不出库门
（docs/03：备份不包含 token 与密钥）。
"""

from __future__ import annotations

import sqlite3
from typing import TypedDict

# docs/03「不包含 token 与密钥」：这些键是凭据，任何配置导出都不得带走。
SECRET_SETTING_KEYS: frozenset[str] = frozenset({"api.token", "notify.token", "notify.webhook"})


class PluginSummaryRow(TypedDict):
    """plugins 表一行的导出概要形态。"""

    id: str
    version: str
    enabled: bool


def collect_export_data(
    connection: sqlite3.Connection,
) -> tuple[dict[str, str], list[PluginSummaryRow]]:
    """收集 settings 全量键值（不含密钥键）与 plugins 概要，均按 key 排序。"""
    values = {
        str(row[0]): str(row[1])
        for row in connection.execute("SELECT key, value FROM settings ORDER BY key")
        if str(row[0]) not in SECRET_SETTING_KEYS
    }
    plugins: list[PluginSummaryRow] = [
        {"id": str(row[0]), "version": str(row[1]), "enabled": bool(row[2])}
        for row in connection.execute("SELECT id, version, enabled FROM plugins ORDER BY id")
    ]
    return values, plugins


__all__ = ["SECRET_SETTING_KEYS", "PluginSummaryRow", "collect_export_data"]
