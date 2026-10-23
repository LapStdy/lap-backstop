"""SQLite 连接引导。

WAL、外键、忙等超时是数据层不变量，集中在这里设定，调用方不必每次重复。
连接是 autocommit 模式，事务边界由调用方用 BEGIN / COMMIT 显式控制。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

BUSY_TIMEOUT_MS = 5000


def open_database(path: Path) -> sqlite3.Connection:
    """打开数据库并设好运行期 PRAGMA。

    Args:
        path: 数据库文件路径；父目录不存在时自动创建。

    Returns:
        autocommit 模式的连接；行按 ``sqlite3.Row`` 返回。

    Raises:
        sqlite3.Error: 文件不可创建、目录不可写或磁盘故障。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    connection.execute("PRAGMA synchronous = NORMAL")
    return connection
