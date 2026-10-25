"""数据库完整性检查：迁移与备份共用的 ``PRAGMA integrity_check`` 封装。"""

from __future__ import annotations

import sqlite3


def verify_integrity(connection: sqlite3.Connection) -> None:
    """跑 ``PRAGMA integrity_check``，结果不是 ``ok`` 时抛错。"""
    rows = connection.execute("PRAGMA integrity_check").fetchall()
    results = [str(row[0]) for row in rows]
    if results != ["ok"]:
        raise sqlite3.DatabaseError("integrity_check 未通过：" + "；".join(results))
