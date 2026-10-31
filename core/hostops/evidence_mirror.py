"""证据增量镜像：把证据目录按增量规则搬进备份目标（docs/08「备份与恢复」）。

镜像只增不删：目标里已有且内容一致的文件跳过，新增或变更的文件先写临时名
再原子替换，正式名不出现半截文件。任一份搬运失败立即停止并把原因交给调用方
落事件；已落盘的都是完整文件，重跑同一命令即续做。

体积策略：证据根总量超过阈值时只搬 verdict.evidence_ids 引用到的证据，
其余文件留在证据根、不进镜像；阈值取 defaults.toml 的 [backup] 段。
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final

from contracts import EventType
from storage.defaults import default
from storage.repositories.events import EventLog

EVIDENCE_MIRROR_FINISHED_EVENT: Final[str] = "host.evidence_mirror_finished"
EVIDENCE_MIRROR_FAILED_EVENT: Final[str] = "host.evidence_mirror_failed"
# 临时名后缀：写入与清扫共用同一形状，孤儿临时文件按它识别。
STAGING_SUFFIX: Final[str] = ".mirror-tmp"
_GIB: Final[int] = 1024**3

Copier = Callable[[Path, Path], None]


@dataclass(frozen=True, slots=True)
class MirrorPlan:
    """一轮镜像的搬运清单：源根、相对路径集合与按体积策略跳过的份数。"""

    source_root: Path
    relatives: tuple[str, ...]
    skipped_by_policy: int


@dataclass(frozen=True, slots=True)
class MirrorOutcome:
    """一轮镜像的结果统计；detail 的键名是事件契约，不随实现调整。"""

    copied_new: int
    copied_changed: int
    skipped_unchanged: int
    skipped_by_policy: int
    bytes_copied: int

    def detail(self) -> dict[str, object]:
        """事件载荷里的 detail；字段顺序与人读顺序一致。"""
        return {
            "copied_new": self.copied_new,
            "copied_changed": self.copied_changed,
            "skipped_unchanged": self.skipped_unchanged,
            "skipped_by_policy": self.skipped_by_policy,
            "bytes_copied": self.bytes_copied,
        }


class EvidenceMirrorError(RuntimeError):
    """镜像中途失败；已落盘文件保持完整，重跑同一命令续做。"""


def mirror_threshold_bytes() -> int:
    """镜像体积阈值：defaults.toml [backup] 段的 GB 值换算成字节。

    Returns:
        int: 阈值字节数；键缺失由 default 抛 KeyError，说明代码与
            defaults.toml 不一致。
    """
    return int(default("backup", "evidence_mirror_threshold_gb")) * _GIB


def plan_evidence(
    connection: sqlite3.Connection,
    source_root: Path,
    *,
    threshold_bytes: int,
) -> MirrorPlan:
    """决定这轮搬哪些文件：先盘点证据根，超阈值时收敛到 verdict 引用的证据。

    证据目录不存在按空清单处理；盘点里任何一份文件取不到体积就放弃本轮，
    宁可不搬也不猜总量（docs/08：镜像失败不破坏已有备份）。

    Args:
        connection: 已打开的平台库连接，用于反查 verdict 引用到的证据路径。
        source_root: 证据根目录（``data\\evidence``）。
        threshold_bytes: 证据根总量阈值；超过时只搬被引用的证据。

    Returns:
        MirrorPlan: 相对路径按字典序排列的搬运清单与按策略跳过的份数。

    Raises:
        OSError: 证据目录不可读或某份文件取不到体积。
        sqlite3.Error: verdict 引用反查失败。
    """
    entries = _inventory(source_root)
    if not entries:
        return MirrorPlan(source_root=source_root, relatives=(), skipped_by_policy=0)
    total_bytes = sum(size for _, size in entries)
    if total_bytes <= threshold_bytes:
        return MirrorPlan(
            source_root=source_root,
            relatives=tuple(relative for relative, _ in entries),
            skipped_by_policy=0,
        )
    referenced = _referenced_paths(connection)
    relatives = tuple(relative for relative, _ in entries if relative in referenced)
    return MirrorPlan(
        source_root=source_root,
        relatives=relatives,
        skipped_by_policy=len(entries) - len(relatives),
    )


def copy_file(source: Path, target: Path) -> None:
    """单份搬运：临时名落盘后原子替换，镜像里不出现半截文件。

    Args:
        source: 源文件，须已存在。
        target: 镜像目标路径，父目录按需创建。

    Raises:
        OSError: 源不可读、目标不可写或替换失败；临时名可能残留，
            由下一轮 mirror_evidence 开头清扫。
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(target.name + STAGING_SUFFIX)
    shutil.copyfile(source, staging)
    os.replace(staging, target)


def mirror_evidence(
    plan: MirrorPlan,
    target_root: Path,
    *,
    copier: Copier = copy_file,
) -> MirrorOutcome:
    """把清单里的文件搬进镜像目标：一致的跳过，新增或变更的逐份原子替换。

    Args:
        plan: plan_evidence 产出的搬运清单。
        target_root: 镜像目标目录（``data\\backups\\evidence``），逐份按需创建。
        copier: 单份搬运动作；缺省临时名加原子替换，测试替换成计数或注错件。

    Returns:
        MirrorOutcome: 新增、变更、跳过计数与搬运字节。

    Raises:
        EvidenceMirrorError: 盘点后文件消失、比对或搬运失败；原因带文件相对
            路径，已完成文件保持完整，重跑同一命令续做。
    """
    _sweep_staging(target_root)
    copied_new = 0
    copied_changed = 0
    skipped_unchanged = 0
    bytes_copied = 0
    for relative in plan.relatives:
        source = plan.source_root / relative
        target = target_root / relative
        existed = target.is_file()
        try:
            size = source.stat().st_size
            if existed and target.stat().st_size == size and _same_bytes(source, target):
                skipped_unchanged += 1
                continue
            copier(source, target)
        except OSError as error:
            raise EvidenceMirrorError(f"{relative}：{error}") from error
        bytes_copied += size
        if existed:
            copied_changed += 1
        else:
            copied_new += 1
    return MirrorOutcome(
        copied_new=copied_new,
        copied_changed=copied_changed,
        skipped_unchanged=skipped_unchanged,
        skipped_by_policy=plan.skipped_by_policy,
        bytes_copied=bytes_copied,
    )


def record_finished(
    connection: sqlite3.Connection,
    outcome: MirrorOutcome,
    *,
    at: datetime,
) -> None:
    """镜像完成写全局事件，detail 是各类计数（docs/03 host.event 表）。

    Args:
        connection: 已打开的平台库连接。
        outcome: 本次镜像的结果统计。
        at: 事件落库时刻。

    Raises:
        sqlite3.Error: 事件落库失败，由调用方决定是否吞掉。
    """
    EventLog(connection).append_global(
        EventType.HOST_EVENT,
        {"event": EVIDENCE_MIRROR_FINISHED_EVENT, "detail": outcome.detail()},
        at=at,
    )


def record_failure(
    connection: sqlite3.Connection,
    reason: str,
    *,
    at: datetime,
) -> None:
    """镜像失败写全局事件，detail 是失败原因；已有备份不受影响。

    Args:
        connection: 已打开的平台库连接。
        reason: 失败原因，人可读且带出错文件路径。
        at: 事件落库时刻。

    Raises:
        sqlite3.Error: 事件落库失败；库本身故障时事件通道不可用，
            调用方（CLI）只记日志不重试。
    """
    EventLog(connection).append_global(
        EventType.HOST_EVENT,
        {"event": EVIDENCE_MIRROR_FAILED_EVENT, "detail": reason},
        at=at,
    )


def _sweep_staging(target_root: Path) -> None:
    """清掉上一轮强杀残留的临时文件；它们按构造不完整，没有保留价值。

    Raises:
        OSError: 残留文件删不掉；交由本轮按失败处置，不静默带病继续。
    """
    if not target_root.is_dir():
        return
    for staging in target_root.rglob(f"*{STAGING_SUFFIX}"):
        staging.unlink()


def _inventory(source_root: Path) -> tuple[tuple[str, int], ...]:
    """证据根下全部文件的 (POSIX 相对路径, 字节) 清单，按相对路径排序。"""
    if not source_root.is_dir():
        return ()
    entries = [
        (path.relative_to(source_root).as_posix(), path.stat().st_size)
        for path in source_root.rglob("*")
        if path.is_file()
    ]
    return tuple(sorted(entries))


def _referenced_paths(connection: sqlite3.Connection) -> frozenset[str]:
    """verdicts.evidence_ids 引用到的证据相对路径集合，逐条 JSON 展开。"""
    rows = connection.execute(
        "SELECT DISTINCT e.path FROM evidence e WHERE EXISTS ("
        "SELECT 1 FROM verdicts v, json_each(v.evidence_ids) AS ref WHERE ref.value = e.id)"
    ).fetchall()
    return frozenset(str(row[0]) for row in rows)


def _same_bytes(source: Path, target: Path) -> bool:
    """同体积文件的等价判定：SHA-256 一致才算跳过。"""
    return _sha256(source) == _sha256(target)


def _sha256(path: Path) -> str:
    """计算文件 SHA-256 摘要。"""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


__all__ = [
    "EVIDENCE_MIRROR_FAILED_EVENT",
    "EVIDENCE_MIRROR_FINISHED_EVENT",
    "STAGING_SUFFIX",
    "Copier",
    "EvidenceMirrorError",
    "MirrorOutcome",
    "MirrorPlan",
    "copy_file",
    "mirror_evidence",
    "mirror_threshold_bytes",
    "plan_evidence",
    "record_failure",
    "record_finished",
]
