r"""app 版本目录：布局推导、升级目标校验、current 联接与保留清理。

布局契约见 docs/08「路径布局」与「升级与回滚」：版本目录在
app\releases\<version>\，app\current 是指向它的目录联接，data\ 与 plugins\ 不随
切换移动。联接只是名字，重建幂等；保留清理按目录修改时间从最旧删，正在使用
与回滚目标等受保护目录永不删。
"""

from __future__ import annotations

import _winapi
import os
import re
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from core.hostops.doctor import DATABASE_FILENAME
from core.hostops.upgrade_state import UpgradeError

APP_DIRNAME = "app"
CURRENT_DIRNAME = "current"
RELEASES_DIRNAME = "releases"
CORE_EXE_NAME = "lap-backstop.exe"
# 升级成功后版本目录的保留份数（docs/08 版本保留）。
KEEP_RELEASES = 3
# 版本目录名形状与 scripts/build_app.py 一致：目录名直接充当版本号。
VERSION_PATTERN = re.compile(r"[0-9A-Za-z._-]+")


@dataclass(frozen=True, slots=True)
class UpgradeLayout:
    """一次升级涉及的固定路径：数据根、部署根（app\\）、版本库与平台库。"""

    data_root: Path
    app_dir: Path
    releases: Path
    database: Path


@dataclass(frozen=True, slots=True)
class UpgradePlan:
    """一次升级的目标与回滚基线，由入口校验后确定。"""

    target_version: str
    previous_dir: Path

    @property
    def previous_version(self) -> str:
        """回滚目标版本号，即切换前的版本目录名。"""
        return self.previous_dir.name


def layout(data_root: Path) -> UpgradeLayout:
    """由数据根推出升级涉及的固定路径（docs/08 路径布局：app 与 data 同级）。"""
    app_dir = data_root.parent / APP_DIRNAME
    return UpgradeLayout(
        data_root=data_root,
        app_dir=app_dir,
        releases=app_dir / RELEASES_DIRNAME,
        database=data_root / DATABASE_FILENAME,
    )


def plan_upgrade(app: UpgradeLayout, target: Path) -> UpgradePlan:
    """校验新版本目录与回滚基线，产出升级计划。

    Raises:
        UpgradeError: 目标不在版本库下、产物不完整，或 current 联接不可用。
    """
    version = _validate_target(app.releases, target)
    return UpgradePlan(target_version=version, previous_dir=read_current(app.app_dir))


def read_current(app_dir: Path) -> Path:
    """读 app\\current 联接指向的版本目录，作为回滚基线。

    Raises:
        UpgradeError: 联接不存在、被普通目录占用或指向缺失的版本目录。
    """
    link = app_dir / CURRENT_DIRNAME
    if not os.path.lexists(link):
        raise UpgradeError(f"{link} 不存在：先用 scripts/build_app.py 建立基线版本")
    if not link.is_junction():
        raise UpgradeError(f"{link} 不是目录联接，拒绝在其上升级")
    target = link.resolve()
    if not target.is_dir():
        raise UpgradeError(f"{link} 指向的版本目录缺失：{target}")
    return target


def switch_current(app_dir: Path, target: Path) -> None:
    """把 app\\current 重建为指向 target 的目录联接；重复执行结果一致。

    联接只是名字，不承载版本内容；删除只摘联接本身，版本目录不受影响。

    Raises:
        UpgradeError: app\\current 被普通目录或文件占用，或联接创建失败。
    """
    link = app_dir / CURRENT_DIRNAME
    if os.path.lexists(link) and not (link.is_junction() or link.is_symlink()):
        raise UpgradeError(f"{link} 不是目录联接，拒绝覆盖；请人工确认后删除再升级")
    if os.path.lexists(link):
        os.rmdir(link)
    try:
        _winapi.CreateJunction(str(target), str(link))
    except OSError as error:
        raise UpgradeError(f"无法把 {link} 联接到 {target}：{error}") from error


def prune_releases(
    releases_dir: Path,
    *,
    protected: Iterable[Path],
    keep: int = KEEP_RELEASES,
) -> tuple[str, ...]:
    """版本目录保留清理：最多保留 keep 个，超出删最旧；受保护目录永不删。

    「最旧」按目录修改时间计；正在使用的当前版本与回滚目标由调用方放进
    protected，即使挤掉保留名额也不删。重复执行天然幂等：第二次没有可删对象。

    Returns:
        实际删除的版本目录名，按从旧到新。

    Raises:
        UpgradeError: 某个版本目录删不掉（可能正被运行中的 exe 占用）。
    """
    versions = _version_dirs(releases_dir)
    protected_paths = {_norm_name(path) for path in protected}
    ranked = sorted(versions, key=_mtime_of)
    stale_count = max(len(ranked) - keep, 0)
    victims = [
        directory
        for index, directory in enumerate(ranked)
        if index < stale_count and _norm_name(directory) not in protected_paths
    ]
    for directory in victims:
        try:
            shutil.rmtree(directory)
        except OSError as error:
            raise UpgradeError(f"无法删除旧版本目录 {directory.name}：{error}") from error
    return tuple(directory.name for directory in victims)


def _validate_target(releases: Path, target: Path) -> str:
    """校验新版本目录：必须在版本库下、是目录、名字合法且含入口 exe。

    Returns:
        版本目录名，直接充当版本号。

    Raises:
        UpgradeError: 位置、形态、名字或入口不满足。
    """
    resolved = target.resolve()
    if not same_path(resolved.parent, releases):
        raise UpgradeError(f"新版本目录必须先放到版本库下再升级：{releases}")
    version = resolved.name
    if VERSION_PATTERN.fullmatch(version) is None:
        raise UpgradeError(f"版本目录名含非法字符：{version!r}")
    if not resolved.is_dir():
        raise UpgradeError(f"新版本目录不存在：{resolved}")
    if not (resolved / CORE_EXE_NAME).is_file():
        raise UpgradeError(f"版本目录缺少入口 {CORE_EXE_NAME}，产物不完整：{version}")
    return version


def _version_dirs(releases_dir: Path) -> tuple[Path, ...]:
    """版本库下的目录清单；文件与联接不算版本，版本库不存在按空处理。"""
    if not releases_dir.is_dir():
        return ()
    return tuple(
        path
        for path in releases_dir.iterdir()
        if path.is_dir() and not path.is_junction() and not path.is_symlink()
    )


def _mtime_of(directory: Path) -> float:
    """目录修改时间；取不到按 0（最旧优先删）。"""
    try:
        return directory.stat().st_mtime
    except OSError:
        return 0.0


def _norm_name(path: Path) -> str:
    """Windows 大小写不敏感的路径归一，用于受保护目录与同路径比对。"""
    return os.path.normcase(str(path.resolve()))


def same_path(left: Path, right: Path) -> bool:
    """两个路径是否指同一位置（Windows 大小写不敏感）。"""
    return _norm_name(left) == _norm_name(right)


__all__ = [
    "APP_DIRNAME",
    "CORE_EXE_NAME",
    "CURRENT_DIRNAME",
    "KEEP_RELEASES",
    "RELEASES_DIRNAME",
    "VERSION_PATTERN",
    "UpgradeLayout",
    "UpgradePlan",
    "layout",
    "plan_upgrade",
    "prune_releases",
    "read_current",
    "same_path",
    "switch_current",
]
