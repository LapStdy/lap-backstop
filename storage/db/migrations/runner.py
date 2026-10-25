"""只前滚迁移的执行器。

回滚走快照而不是反向迁移：反向迁移本身容易写错，恢复路径必须简单（docs/03）。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from contracts.context import Clock
from storage.db.transaction import transaction

UpFunction = Callable[[sqlite3.Connection], None]


class MigrationError(RuntimeError):
    """迁移链不连续、清单非法或读取版本失败。"""


@dataclass(frozen=True, slots=True)
class Migration:
    """一份迁移：版本号从 1 起连续递增，``up`` 在事务内执行。"""

    version: int
    name: str
    up: UpFunction


def current_version(connection: sqlite3.Connection) -> int:
    """读取数据库当前版本，对应 SQLite 的 ``user_version``。"""
    row = connection.execute("PRAGMA user_version").fetchone()
    if row is None:
        raise MigrationError("无法读取 user_version")
    return int(row[0])


def execute_statements(connection: sqlite3.Connection, script: str) -> None:
    """按分号拆分 SQL 并逐条执行。

    ``executescript`` 会先提交调用方已开启的事务，迁移中途断开会留下半套结构；
    这里保持单事务，失败可整体回滚。
    """
    for statement in script.split(";"):
        if statement.strip():
            connection.execute(statement)


def migrate(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration] | None = None,
    *,
    backups_dir: Path | None = None,
    clock: Clock | None = None,
) -> int:
    """把数据库前滚到最新版本，返回最终版本号。

    Args:
        connection: 已打开并设好 PRAGMA 的连接。
        migrations: 迁移清单；省略时用仓库内置清单。
        backups_dir: 给定且存在待执行迁移时，先强制生成 pre-migrate 快照；
            数据库已是最新则不生成。
        clock: 快照的时间来源；给了 backups_dir 就必须给。

    Raises:
        MigrationError: 版本不连续、数据库版本高于代码支持的版本，
            或给了 backups_dir 却没给 clock。
        sqlite3.Error: 某份迁移的 SQL 执行失败；该份迁移整体回滚。
        BackupError: 迁移前快照生成失败；此时不执行任何迁移。
    """
    ordered, applied = _prepare_and_snapshot(connection, migrations, backups_dir, clock)
    for entry in ordered:
        if entry.version <= applied:
            continue
        with transaction(connection):
            entry.up(connection)
            connection.execute(f"PRAGMA user_version = {entry.version}")
        applied = entry.version
    return applied


def _prepare_and_snapshot(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration] | None,
    backups_dir: Path | None,
    clock: Clock | None,
) -> tuple[tuple[Migration, ...], int]:
    """迁移前置：清单排序与连续性校验、版本检查与 pre-migrate 快照。"""
    if migrations is None:
        from storage.db.migrations import MIGRATIONS

        ordered: tuple[Migration, ...] = MIGRATIONS
    else:
        ordered = tuple(migrations)
    ordered = tuple(sorted(ordered, key=lambda entry: entry.version))
    _require_contiguous(ordered)
    applied = current_version(connection)
    if applied > ordered[-1].version:
        raise MigrationError(f"数据库版本 {applied} 高于代码支持的最高版本 {ordered[-1].version}")
    if backups_dir is not None and applied < ordered[-1].version:
        if clock is None:
            raise MigrationError("启用迁移前快照必须同时提供 clock")
        # 延迟导入：backup 依赖本模块的 verify_integrity，顶层互导成环。
        from storage.db.backup import create_pre_migrate_snapshot

        create_pre_migrate_snapshot(
            connection, backups_dir, version=ordered[-1].version, clock=clock
        )
    return ordered, applied


def verify_integrity(connection: sqlite3.Connection) -> None:
    """跑 ``PRAGMA integrity_check``，结果不是 ``ok`` 时抛错。"""
    rows = connection.execute("PRAGMA integrity_check").fetchall()
    results = [str(row[0]) for row in rows]
    if results != ["ok"]:
        raise sqlite3.DatabaseError("integrity_check 未通过：" + "；".join(results))


def _require_contiguous(migrations: Sequence[Migration]) -> None:
    if not migrations:
        raise MigrationError("迁移清单为空")
    expected = 1
    for entry in migrations:
        if entry.version != expected:
            raise MigrationError(
                f"迁移版本应从 {expected} 开始且连续，实际是 {entry.version}（{entry.name}）"
            )
        expected += 1


def load_up(module: object) -> UpFunction:
    """取出迁移模块的 ``up`` 函数并收窄类型。"""
    up = getattr(module, "up", None)
    if not callable(up):
        raise MigrationError("迁移模块缺少可调用的 up 函数")
    return cast(UpFunction, up)
