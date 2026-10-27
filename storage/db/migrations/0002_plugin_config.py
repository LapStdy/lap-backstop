"""迁移 0002：plugins 表加当前配置与修订号。

插件配置原本只有 `config_revisions` 记变更，没有存「当前 payload」的地方；
本迁移把当前配置与自增修订号落到 plugins 行上，回滚走 pre-migrate 快照。
"""

from __future__ import annotations

import sqlite3

from storage.db.migrations.runner import execute_statements

_UP = """
ALTER TABLE plugins ADD COLUMN config TEXT NOT NULL DEFAULT '{}';
ALTER TABLE plugins ADD COLUMN config_revision INTEGER NOT NULL DEFAULT 0;
"""


def up(connection: sqlite3.Connection) -> None:
    """给 plugins 加 config 与 config_revision 两列；旧行取空配置与修订 0。"""
    execute_statements(connection, _UP)


__all__ = ["up"]
