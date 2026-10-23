"""单写连接上的显式事务。

状态变更与它的事件必须整块落盘或整块回滚；嵌套调用复用外层事务，不重复 BEGIN。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, suppress


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """在连接上开一个 IMMEDIATE 事务；已在事务中时直接复用。

    Raises:
        sqlite3.Error: 开启、提交或回滚失败。
    """
    if connection.in_transaction:
        yield connection
        return
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        with suppress(sqlite3.Error):
            connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")
