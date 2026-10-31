"""版本目录：保留清理与 current 联接重建（docs/08 升级与回滚）。"""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.hostops.releases import CORE_EXE_NAME, CURRENT_DIRNAME, prune_releases, switch_current
from core.hostops.upgrade_state import UpgradeError

NOW = datetime(2026, 10, 30, 3, 0, tzinfo=UTC)


def _make_versions(releases: Path, names: tuple[str, ...]) -> None:
    for index, name in enumerate(names):
        directory = releases / name
        directory.mkdir(parents=True)
        moment = NOW.timestamp() + index
        os.utime(directory, (moment, moment))


def _make_version(root: Path, name: str) -> Path:
    """造一个含入口 exe 的版本目录，供联接指向与内容断言共用。"""
    version = root / "releases" / name
    version.mkdir(parents=True)
    (version / CORE_EXE_NAME).write_bytes(b"MZ")
    return version


def test_prune_releases_keeps_three_and_protects_rollback_target(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    _make_versions(releases, ("0.9.0", "1.0.0", "1.1.0", "1.2.0"))

    removed = prune_releases(releases, protected=(releases / "1.2.0", releases / "1.1.0"))
    repeated = prune_releases(releases, protected=(releases / "1.2.0", releases / "1.1.0"))

    assert removed == ("0.9.0",)
    assert repeated == (), "重复执行不得再删任何目录"
    assert not (releases / "0.9.0").exists()
    for survivor in ("1.0.0", "1.1.0", "1.2.0"):
        assert (releases / survivor).is_dir()


def test_prune_releases_never_deletes_protected_beyond_quota(tmp_path: Path) -> None:
    releases = tmp_path / "releases"
    _make_versions(releases, ("0.9.0", "1.0.0", "1.1.0", "1.2.0"))

    removed = prune_releases(releases, protected=(releases / "0.9.0",), keep=2)

    assert removed == ("1.0.0",), "最旧的受保护目录被跳过，次旧的被删"
    assert (releases / "0.9.0").is_dir()


def test_switch_current_missing_target_fails_and_leaves_no_link(tmp_path: Path) -> None:
    app = tmp_path / "app"
    app.mkdir()
    target = tmp_path / "releases" / "9.9.9"

    with pytest.raises(UpgradeError) as raised:
        switch_current(app, target)

    assert str(target) in str(raised.value), "报错要指出联不上的目标路径"
    assert not os.path.lexists(app / CURRENT_DIRNAME), "创建失败不得留下半成品联接"


def test_switch_current_repoints_existing_link_and_keeps_old_version(tmp_path: Path) -> None:
    app = tmp_path / "app"
    app.mkdir()
    old = _make_version(tmp_path, "1.0.0")
    switch_current(app, old)
    new = _make_version(tmp_path, "1.1.0")

    switch_current(app, new)

    current = app / CURRENT_DIRNAME
    assert os.path.realpath(current) == os.path.realpath(new), "联接要改指新版本"
    assert (current / CORE_EXE_NAME).is_file(), "经联接读到的是新版本内容"
    assert (old / CORE_EXE_NAME).is_file(), "旧版本目录不随联接重建受损"


def test_switch_current_rebuilds_link_whose_target_vanished(tmp_path: Path) -> None:
    app = tmp_path / "app"
    app.mkdir()
    vanished = _make_version(tmp_path, "1.0.0")
    switch_current(app, vanished)
    shutil.rmtree(vanished)
    fresh = _make_version(tmp_path, "1.1.0")

    switch_current(app, fresh)

    current = app / CURRENT_DIRNAME
    assert current.is_junction(), "悬空联接被重建为可用联接"
    assert os.path.realpath(current) == os.path.realpath(fresh), "重建后联接改指新版本目录"
    assert (current / CORE_EXE_NAME).is_file(), "经重建的联接能读到新版本内容"


def test_switch_current_refuses_real_directory_occupant(tmp_path: Path) -> None:
    app = tmp_path / "app"
    app.mkdir()
    current = app / CURRENT_DIRNAME
    current.mkdir(parents=True)
    keeper = current / "human.txt"
    keeper.write_text("人工存放的内容", encoding="utf-8")

    with pytest.raises(UpgradeError) as raised:
        switch_current(app, _make_version(tmp_path, "1.1.0"))

    assert str(raised.value).endswith("请人工确认后删除再升级"), "占用拒绝按升级口径报错"
    assert keeper.is_file(), "拒绝覆盖时人工内容原样保留"
