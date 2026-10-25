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
from storage.db.migrations.runner import verify_integrity
from storage.repositories.iso_time import format_at, parse_at

SnapshotKind = Literal["daily", "weekly"]

_MANIFEST_SUFFIX = ".manifest.json"
_SNAPSHOT_TIMESTAMP = "%Y%m%d-%H%M%S"
_KINDS: tuple[SnapshotKind, ...] = ("daily", "weekly")


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
    backups_dir.mkdir(parents=True, exist_ok=True)
    created_at = clock.utc_now().astimezone(UTC)
    stem = f"db-{created_at.strftime(_SNAPSHOT_TIMESTAMP)}"
    final_path = backups_dir / f"{stem}.sqlite"
    staging_path = backups_dir / f"{stem}.sqlite.tmp"
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
    return _describe_with_manifest(final_path, kind=kind, created_at=created_at)


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
