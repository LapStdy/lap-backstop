"""数据库快照：VACUUM INTO 在线快照、manifest 清单与完整性校验。

回滚与恢复只走快照、不走反向迁移（docs/03），因此快照必须自带可核对的凭据：
SHA-256、行数与 schema 版本写入 manifest，未通过 integrity_check 的文件不允许
留在备份目录（docs/08「备份与恢复」）。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

from contracts.context import Clock
from storage.db.integrity import verify_integrity
from storage.repositories.config_export import collect_export_data
from storage.repositories.iso_time import format_at, parse_at

SnapshotKind = Literal["daily", "weekly", "pre-migrate"]

_MANIFEST_SUFFIX = ".manifest.json"
# 文件名时间戳形状：快照、pre-migrate 与配置导出统一走这一份，别处不再复制。
TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"
_KINDS: tuple[SnapshotKind, ...] = ("daily", "weekly", "pre-migrate")
# 导出文件名规则 {prefix}-{stamp}.json；构造与修剪必须共用同一形状。
_EXPORT_PREFIXES: tuple[str, ...] = ("settings", "plugins")


class BackupError(RuntimeError):
    """快照生成或校验失败；备份目录不允许留下未经校验的快照。"""


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    """一份已通过完整性校验的快照及其 manifest 内容。"""

    path: Path
    manifest_path: Path
    kind: SnapshotKind
    created_at: datetime
    schema_version: int
    row_counts: Mapping[str, int]
    sha256: str
    size_bytes: int


def create_snapshot(
    connection: sqlite3.Connection,
    backups_dir: Path,
    *,
    clock: Clock,
    kind: SnapshotKind = "daily",
) -> SnapshotResult:
    """生成在线一致快照并写 manifest，返回快照与清单内容。

    VACUUM INTO 读源库的已提交状态，不阻塞写入；快照先落临时名，通过
    integrity_check 后才替换成正式名，备份目录里不会出现未校验的快照。

    Args:
        connection: 已打开并设好 PRAGMA 的源库连接。
        backups_dir: 备份目录，不存在时创建。
        clock: 时间唯一来源；文件名时间戳与 manifest 时间取自它。
        kind: 快照类别，写进 manifest 供修剪分类。

    Returns:
        SnapshotResult: 含路径、SHA-256、各表行数与 schema 版本。

    Raises:
        BackupError: 同秒同名快照已存在、快照不可读、integrity_check
            未通过或备份目录不可写。
    """
    created_at = clock.utc_now().astimezone(UTC)
    spec = _SnapshotSpec(
        stem=f"db-{created_at.strftime(TIMESTAMP_FORMAT)}",
        kind=kind,
        created_at=created_at,
    )
    return _write_verified_snapshot(connection, backups_dir, spec)


def create_pre_migrate_snapshot(
    connection: sqlite3.Connection,
    backups_dir: Path,
    *,
    version: int,
    clock: Clock,
) -> SnapshotResult:
    """生成 schema 迁移前的强制快照（docs/08），文件名携带目标版本号。

    Args:
        connection: 已打开并设好 PRAGMA 的源库连接。
        backups_dir: 备份目录，不存在时创建。
        version: 即将迁移到的目标 schema 版本。
        clock: 时间唯一来源；文件名时间戳与 manifest 时间取自它。

    Returns:
        SnapshotResult: kind 为 ``pre-migrate``，不参与例行修剪。

    Raises:
        BackupError: 同秒同名快照已存在、快照不可读、integrity_check
            未通过或备份目录不可写。
    """
    created_at = clock.utc_now().astimezone(UTC)
    spec = _SnapshotSpec(
        stem=f"pre-migrate-{version}-{created_at.strftime(TIMESTAMP_FORMAT)}",
        kind="pre-migrate",
        created_at=created_at,
    )
    return _write_verified_snapshot(connection, backups_dir, spec)


@dataclass(frozen=True, slots=True)
class _SnapshotSpec:
    """一次快照的命名要素：文件名主干、类别与时刻。"""

    stem: str
    kind: SnapshotKind
    created_at: datetime


def _write_verified_snapshot(
    connection: sqlite3.Connection,
    backups_dir: Path,
    spec: _SnapshotSpec,
) -> SnapshotResult:
    """快照主体：临时名落盘，过 integrity_check 才转正名并写 manifest。"""
    backups_dir.mkdir(parents=True, exist_ok=True)
    final_path = backups_dir / f"{spec.stem}.sqlite"
    staging_path = backups_dir / f"{spec.stem}.sqlite.tmp"
    if final_path.exists():
        raise BackupError(f"同名快照已存在，换一个时刻再生成：{final_path.name}")
    staging_path.unlink(missing_ok=True)
    try:
        connection.execute("VACUUM INTO ?", (_uri(staging_path),))
        _verify_snapshot_file(staging_path)
        os.replace(staging_path, final_path)
    except (sqlite3.Error, OSError) as error:
        _discard(staging_path)
        raise BackupError(f"快照生成失败：{error}") from error
    except BackupError:
        _discard(staging_path)
        raise
    return _describe_with_manifest(final_path, kind=spec.kind, created_at=spec.created_at)


def verify_snapshot(snapshot_path: Path) -> SnapshotResult:
    """按 manifest 校验快照：SHA-256 一致且 integrity_check 通过。

    Args:
        snapshot_path: 快照文件路径；manifest 取同名 ``.manifest.json``。

    Returns:
        SnapshotResult: manifest 记录的快照信息。

    Raises:
        BackupError: 快照或 manifest 缺失、字段不完整、SHA-256 不符
            或完整性未通过。
    """
    manifest_path = snapshot_path.with_name(snapshot_path.stem + _MANIFEST_SUFFIX)
    result = _result_from_manifest(snapshot_path, manifest_path, _load_json(manifest_path))
    if _sha256_of(snapshot_path) != result.sha256:
        raise BackupError(f"快照与 manifest 的 SHA-256 不符：{snapshot_path.name}")
    _verify_snapshot_file(snapshot_path)
    return result


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


def _describe_with_manifest(
    final_path: Path,
    *,
    kind: SnapshotKind,
    created_at: datetime,
) -> SnapshotResult:
    """读取已落位的快照生成描述信息并写 manifest；读不回来就删掉快照。"""
    try:
        result = _describe_snapshot(final_path, kind=kind, created_at=created_at)
        payload: dict[str, object] = {
            "file": final_path.name,
            "kind": result.kind,
            "created_at": format_at(result.created_at),
            "schema_version": result.schema_version,
            "sha256": result.sha256,
            "size_bytes": result.size_bytes,
            "row_counts": dict(result.row_counts),
        }
        _write_json_atomic(final_path.with_name(final_path.stem + _MANIFEST_SUFFIX), payload)
        return result
    except (sqlite3.Error, OSError, ValueError) as error:
        # 读不回凭据的快照等于不可恢复，宁可不留。
        _discard(final_path)
        raise BackupError(f"快照描述或 manifest 写入失败：{error}") from error


def _describe_snapshot(
    path: Path,
    *,
    kind: SnapshotKind,
    created_at: datetime,
) -> SnapshotResult:
    """打开只读连接统计 schema 版本与各表行数，并计算文件摘要。"""
    snapshot = _open_readonly(path)
    try:
        version_row = snapshot.execute("PRAGMA user_version").fetchone()
        if version_row is None:
            raise sqlite3.DatabaseError("无法读取快照的 user_version")
        counts = _row_counts(snapshot)
    finally:
        snapshot.close()
    return SnapshotResult(
        path=path,
        manifest_path=path.with_name(path.stem + _MANIFEST_SUFFIX),
        kind=kind,
        created_at=created_at,
        schema_version=int(version_row[0]),
        row_counts=counts,
        sha256=_sha256_of(path),
        size_bytes=path.stat().st_size,
    )


def _result_from_manifest(
    snapshot_path: Path,
    manifest_path: Path,
    payload: Mapping[str, object],
) -> SnapshotResult:
    """把 manifest 载荷收窄成 SnapshotResult；字段缺失或不合法即拒绝。"""
    for key in (
        "file",
        "kind",
        "created_at",
        "schema_version",
        "sha256",
        "size_bytes",
        "row_counts",
    ):
        if key not in payload:
            raise BackupError(f"manifest 缺字段 {key}（{manifest_path.name}）")
    kind = payload["kind"]
    if kind not in _KINDS:
        raise BackupError(f"manifest 的 kind 非法：{kind!r}（{manifest_path.name}）")
    if payload["file"] != snapshot_path.name:
        raise BackupError(f"manifest 属于别的快照：{payload['file']!r}（{manifest_path.name}）")
    schema_version = payload["schema_version"]
    sha256 = payload["sha256"]
    size_bytes = payload["size_bytes"]
    created_at = payload["created_at"]
    if not isinstance(schema_version, int) or not isinstance(size_bytes, int):
        raise BackupError(f"manifest 的数值字段非法（{manifest_path.name}）")
    if not isinstance(sha256, str) or not isinstance(created_at, str):
        raise BackupError(f"manifest 的文本字段非法（{manifest_path.name}）")
    raw_counts = payload["row_counts"]
    if not isinstance(raw_counts, dict) or not all(
        isinstance(name, str) and isinstance(count, int) for name, count in raw_counts.items()
    ):
        raise BackupError(f"manifest 的 row_counts 非法（{manifest_path.name}）")
    try:
        moment = parse_at(created_at)
    except ValueError as error:
        raise BackupError(f"manifest 的 created_at 非法（{manifest_path.name}）") from error
    return SnapshotResult(
        path=snapshot_path,
        manifest_path=manifest_path,
        kind=cast(SnapshotKind, kind),
        created_at=moment,
        schema_version=schema_version,
        row_counts=dict(raw_counts),
        sha256=sha256,
        size_bytes=size_bytes,
    )


def _verify_snapshot_file(path: Path) -> None:
    """对快照文件跑 integrity_check；失败按 BackupError 处置。"""
    snapshot = _open_readonly(path)
    try:
        verify_integrity(snapshot)
    except sqlite3.DatabaseError as error:
        raise BackupError(f"integrity_check 未通过：{path.name}（{error}）") from error
    finally:
        snapshot.close()


def _row_counts(connection: sqlite3.Connection) -> dict[str, int]:
    """统计每张用户表的行数；清单必须可对照源库核数。"""
    tables = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        " ORDER BY name"
    ).fetchall()
    counts: dict[str, int] = {}
    for row in tables:
        name = str(row[0])
        counts[name] = int(connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0])
    return counts


def _open_readonly(path: Path) -> sqlite3.Connection:
    """以只读 URI 打开快照，避免校验动作改动被校对象。"""
    return sqlite3.connect(f"file:{_uri(path)}?mode=ro", uri=True)


def _sha256_of(path: Path) -> str:
    """计算文件 SHA-256 摘要。"""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    """临时文件写入后原子替换，manifest 不留半截。"""
    staging = path.with_name(path.name + ".tmp")
    with staging.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(staging, path)


def _load_json(path: Path) -> Mapping[str, object]:
    """读 manifest JSON；缺失或不是对象按 BackupError 处置。"""
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise BackupError(f"manifest 不可读：{path.name}（{error}）") from error
    if not isinstance(payload, dict):
        raise BackupError(f"manifest 不是 JSON 对象：{path.name}")
    return payload


def _discard(path: Path) -> None:
    """清理失败路径上的残留文件；删不掉不再连带抛错。"""
    with suppress(OSError):
        path.unlink(missing_ok=True)


def _uri(path: Path) -> str:
    """SQLite 文件 URI 用正斜杠的绝对路径，Windows 盘符也接受。"""
    return path.resolve().as_posix()
