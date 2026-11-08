"""备份目录的配置导出与按份数修剪：导出成对 JSON、快照与导出按保留表最旧优先删除。

修剪只消费 manifest，不产生快照；快照的生成与校验留在 ``storage.db.backup``。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from contracts.context import Clock
from storage.db.backup import (
    _MANIFEST_SUFFIX,
    TIMESTAMP_FORMAT,
    BackupError,
    SnapshotResult,
    _discard,
    _load_json,
    _result_from_manifest,
    _write_json_atomic,
)
from storage.repositories.config_export import collect_export_data
from storage.repositories.iso_time import format_at

# 导出文件名规则 {prefix}-{stamp}.json；构造与修剪必须共用同一形状。
_EXPORT_PREFIXES: tuple[str, ...] = ("settings", "plugins")


@dataclass(frozen=True, slots=True)
class ExportResult:
    """一次配置导出生成的两个 JSON 文件。"""

    settings_path: Path
    plugins_path: Path
    created_at: datetime
    settings_count: int
    plugins_count: int


def export_configs(
    connection: sqlite3.Connection,
    exports_dir: Path,
    *,
    clock: Clock,
) -> ExportResult:
    """把平台配置导出为带时间戳的 JSON：settings 全量、plugins 概要。

    docs/08 的备份内容：settings 键值全量、plugins 只含 id、版本与启用
    状态；收集口径见 storage.repositories.config_export，密钥键不入导出。

    Args:
        connection: 已打开的源库连接。
        exports_dir: 导出目录，不存在时创建。
        clock: 时间唯一来源；文件名时间戳取自它。

    Returns:
        ExportResult: 两个导出文件与各自条数。

    Raises:
        BackupError: 同秒同名导出已存在或目录不可写。
    """
    exports_dir.mkdir(parents=True, exist_ok=True)
    created_at = clock.utc_now().astimezone(UTC)
    stamp = created_at.strftime(TIMESTAMP_FORMAT)
    settings_path = _export_path(exports_dir, "settings", stamp)
    plugins_path = _export_path(exports_dir, "plugins", stamp)
    if settings_path.exists() or plugins_path.exists():
        raise BackupError(f"同名导出已存在，换一个时刻再导出：settings-{stamp}.json")
    values, plugins = collect_export_data(connection)
    try:
        _write_json_atomic(settings_path, {"created_at": format_at(created_at), "values": values})
        _write_json_atomic(plugins_path, {"created_at": format_at(created_at), "plugins": plugins})
    except OSError as error:
        # 导出必须成对；缺一半的恢复材料比没有更误导。
        _discard(settings_path)
        _discard(plugins_path)
        raise BackupError(f"配置导出失败：{error}") from error
    return ExportResult(
        settings_path=settings_path,
        plugins_path=plugins_path,
        created_at=created_at,
        settings_count=len(values),
        plugins_count=len(plugins),
    )


@dataclass(frozen=True, slots=True)
class PruneResult:
    """一次修剪删除的文件；快照与 manifest 成对计入。"""

    deleted: tuple[Path, ...]


def prune_backups(
    backups_dir: Path,
    *,
    daily_keep: int = 14,
    weekly_keep: int = 8,
    export_keep: int = 20,
) -> PruneResult:
    """按 manifest 的 kind 修剪备份，最旧优先删除（docs/03 保留表）。

    默认保留 14 份日备、8 份周备、20 组配置导出。pre-migrate 快照与没有
    manifest 的文件不参与修剪：分不了类的一律不动，去留由升级流程决定。

    Args:
        backups_dir: 备份目录；快照与配置导出同目录。
        daily_keep: 日备保留份数。
        weekly_keep: 周备保留份数。
        export_keep: 配置导出保留组数（一组含 settings 与 plugins）。

    Returns:
        PruneResult: 实际删除的文件清单。

    Raises:
        BackupError: manifest 损坏或不可读——宁可停下也不猜着删。
    """
    if not backups_dir.exists():
        return PruneResult(deleted=())
    deleted = _prune_snapshots(backups_dir, daily_keep=daily_keep, weekly_keep=weekly_keep)
    deleted += _prune_exports(backups_dir, export_keep=export_keep)
    return PruneResult(deleted=tuple(deleted))


def _prune_snapshots(backups_dir: Path, *, daily_keep: int, weekly_keep: int) -> list[Path]:
    """快照按类别各保留最新 N 份；manifest 是分类唯一依据。"""
    keeps: dict[str, int] = {"daily": daily_keep, "weekly": weekly_keep}
    groups: dict[str, list[SnapshotResult]] = {"daily": [], "weekly": []}
    for manifest_path in sorted(backups_dir.glob(f"*{_MANIFEST_SUFFIX}")):
        snapshot_path = manifest_path.with_name(
            manifest_path.name.removesuffix(_MANIFEST_SUFFIX) + ".sqlite"
        )
        result = _result_from_manifest(snapshot_path, manifest_path, _load_json(manifest_path))
        if result.kind in groups:
            groups[result.kind].append(result)
    deleted: list[Path] = []
    for kind, results in groups.items():
        results.sort(key=lambda item: item.created_at)
        for victim in results[: max(len(results) - keeps[kind], 0)]:
            victim.path.unlink()
            victim.manifest_path.unlink()
            deleted.extend((victim.path, victim.manifest_path))
    return deleted


def _export_path(exports_dir: Path, prefix: str, stamp: str) -> Path:
    """导出文件路径；命名规则单点，修剪按同一形状反解。"""
    return exports_dir / f"{prefix}-{stamp}.json"


def _prune_exports(exports_dir: Path, *, export_keep: int) -> list[Path]:
    """配置导出按时间戳分组修剪，一组含 settings 与 plugins 两个文件。"""
    generations: dict[str, list[Path]] = {}
    for prefix in _EXPORT_PREFIXES:
        for path in sorted(exports_dir.glob(f"{prefix}-*.json")):
            stamp = path.name.removesuffix(".json").removeprefix(f"{prefix}-")
            generations.setdefault(stamp, []).append(path)
    deleted: list[Path] = []
    for stamp in sorted(generations)[: max(len(generations) - export_keep, 0)]:
        for path in generations[stamp]:
            path.unlink()
            deleted.append(path)
    return deleted
