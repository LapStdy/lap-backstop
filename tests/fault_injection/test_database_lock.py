"""故障注入：另一连接持写锁时，写入等到 busy_timeout 后得到明确错误。

判据是 docs/12 故障注入表「数据库锁」的「请求等到 busy_timeout 后返回明确
错误，不静默丢写」：锁竞争下的写既不假装成功，也不吞掉错误；锁释放后同一写入照常落库。
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from storage import open_database

# 5 秒的默认等待对测试太长；缩短的是等满时长，行为语义（等到 busy_timeout 后明确报错）不变。
_SHORT_BUSY_TIMEOUT_MS = 300


def test_locked_write_times_out_explicitly_and_writes_after_release(tmp_path: Path) -> None:
    holder = open_database(tmp_path / "state.db")
    writer = open_database(tmp_path / "state.db")
    try:
        writer.execute(f"PRAGMA busy_timeout = {_SHORT_BUSY_TIMEOUT_MS}")
        holder.execute("CREATE TABLE lock_probe (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
        holder.execute("BEGIN IMMEDIATE")
        holder.execute("INSERT INTO lock_probe (note) VALUES ('held')")

        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            writer.execute("INSERT INTO lock_probe (note) VALUES ('blocked')")
        assert time.monotonic() - started >= _SHORT_BUSY_TIMEOUT_MS / 1000
        blocked = writer.execute("SELECT COUNT(*) FROM lock_probe").fetchone()
        assert blocked is not None
        assert blocked[0] == 0

        holder.execute("COMMIT")
        writer.execute("INSERT INTO lock_probe (note) VALUES ('after')")
        notes = [str(row[0]) for row in writer.execute("SELECT note FROM lock_probe")]
        assert notes == ["held", "after"]
    finally:
        holder.close()
        writer.close()
