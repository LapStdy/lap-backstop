"""SQLite 数据层：连接引导、只前滚迁移与快照备份。

依赖方向由 ADR 0001 固定：storage 只依赖 contracts 与标准库。
"""

from __future__ import annotations

from storage.db.backup import (
    BackupError,
    SnapshotKind,
    SnapshotResult,
    create_snapshot,
    verify_snapshot,
)
from storage.db.connection import BUSY_TIMEOUT_MS, open_database
from storage.db.migrations.runner import (
    Migration,
    MigrationError,
    current_version,
    execute_statements,
    migrate,
    verify_integrity,
)

__all__ = [
    "BUSY_TIMEOUT_MS",
    "BackupError",
    "Migration",
    "MigrationError",
    "SnapshotKind",
    "SnapshotResult",
    "create_snapshot",
    "current_version",
    "execute_statements",
    "migrate",
    "open_database",
    "verify_integrity",
    "verify_snapshot",
]
