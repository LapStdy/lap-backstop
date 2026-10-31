"""SQLite 数据层：连接引导、只前滚迁移、快照与配置备份。

依赖方向由 ADR 0001 固定：storage 只依赖 contracts 与标准库。
"""

from __future__ import annotations

from storage.db.backup import (
    BackupError,
    ExportResult,
    PruneResult,
    SnapshotKind,
    SnapshotResult,
    create_pre_migrate_snapshot,
    create_pre_upgrade_snapshot,
    create_snapshot,
    export_configs,
    prune_backups,
    verify_snapshot,
)
from storage.db.connection import BUSY_TIMEOUT_MS, open_database
from storage.db.integrity import verify_integrity
from storage.db.migrations.runner import (
    Migration,
    MigrationError,
    current_version,
    execute_statements,
    migrate,
)

__all__ = [
    "BUSY_TIMEOUT_MS",
    "BackupError",
    "ExportResult",
    "Migration",
    "MigrationError",
    "PruneResult",
    "SnapshotKind",
    "SnapshotResult",
    "create_pre_migrate_snapshot",
    "create_pre_upgrade_snapshot",
    "create_snapshot",
    "current_version",
    "execute_statements",
    "export_configs",
    "migrate",
    "open_database",
    "prune_backups",
    "verify_integrity",
    "verify_snapshot",
]
