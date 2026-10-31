"""版本目录保留清理：配额、受保护目录与幂等（docs/08 升级与回滚）。"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

from core.hostops.releases import prune_releases

NOW = datetime(2026, 10, 30, 3, 0, tzinfo=UTC)


def _make_versions(releases: Path, names: tuple[str, ...]) -> None:
    for index, name in enumerate(names):
        directory = releases / name
        directory.mkdir(parents=True)
        moment = NOW.timestamp() + index
        os.utime(directory, (moment, moment))


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
