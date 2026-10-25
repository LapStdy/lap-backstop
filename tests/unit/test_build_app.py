r"""build_app 布局用例：版本目录、current 联接、幂等与 data/plugins 隔离。

判据来自 docs/08「路径布局」「升级与回滚」：exe 直接位于版本目录根，
app\current 是指向最新版本目录的联接，构建只写 app\ 与 build\。PyInstaller
真实打包不进单测，runner 注入按命令清单造出 onedir 假产物。
"""

from __future__ import annotations

import importlib.util
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "build_app.py"


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location("build_app_under_test", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # importlib 加载的模块先注册进 sys.modules，避免机制按名回查时找不到。
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_app = _load_script()


def make_repo(tmp_path: Path) -> Path:
    """最小仓库骨架：pyproject、defaults.toml 与 data、plugins 哨兵。"""
    repo = tmp_path / "repo"
    (repo / "config").mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "lap-backstop"\nversion = "1.2.3"\n', encoding="utf-8"
    )
    (repo / "config" / "defaults.toml").write_text("[host]\n", encoding="utf-8")
    for dirname in ("data", "plugins"):
        (repo / dirname).mkdir()
        (repo / dirname / "sentinel.txt").write_text("keep", encoding="utf-8")
    return repo


def make_runner(calls: list[list[str]]) -> Any:
    """假 PyInstaller：记录命令，并按 --distpath 与 --name 造出 onedir 假产物。"""

    def run(command: Sequence[str]) -> None:
        parts = list(command)
        calls.append(parts)
        dist = Path(flag_value(parts, "--distpath"))
        name = flag_value(parts, "--name")
        bundle = dist / name
        (bundle / "_internal").mkdir(parents=True)
        (bundle / "_internal" / "runtime.dat").write_bytes(b"stub")
        (bundle / f"{name}.exe").write_bytes(b"MZ")

    return run


def make_options(
    repo: Path, runner: Any, version: str | None = None, is_dry_run: bool = False
) -> Any:
    return build_app.BuildOptions(
        repo_root=repo,
        version=build_app.resolve_version(repo, version),
        runner=runner,
        is_dry_run=is_dry_run,
    )


def flag_value(command: list[str], flag: str) -> str:
    """取命令清单里某个旗标的值，供断言与假产物定位共用。"""
    return str(command[command.index(flag) + 1])


def realpath(path: Path) -> Path:
    return Path(os.path.realpath(path))


def test_resolve_version_reads_pyproject_and_prefers_override(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    assert build_app.resolve_version(repo, None) == "1.2.3"
    assert build_app.resolve_version(repo, "9.9.9") == "9.9.9"


@pytest.mark.parametrize("bad", ["..", "a/b", "a\\b", "a b", ""])
def test_resolve_version_rejects_names_unsafe_for_paths(tmp_path: Path, bad: str) -> None:
    with pytest.raises(build_app.BuildError):
        build_app.resolve_version(make_repo(tmp_path), bad)


def test_build_produces_version_dir_exe_and_current_junction(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    version_dir = build_app.build(make_options(repo, make_runner([])))
    assert version_dir == repo / "app" / "releases" / "1.2.3"
    assert (version_dir / "lap-backstop.exe").is_file()
    assert (version_dir / "_internal").is_dir()
    current = repo / "app" / "current"
    assert current.is_junction()
    assert realpath(current) == realpath(version_dir)
    assert (current / "lap-backstop.exe").is_file()


def test_build_twice_cleans_version_dir_and_keeps_junction_consistent(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    version_dir = build_app.build(make_options(repo, make_runner([])))
    stale = version_dir / "_internal" / "stale.dat"
    stale.write_bytes(b"old")
    assert build_app.build(make_options(repo, make_runner([]))) == version_dir
    assert not stale.exists()
    current = repo / "app" / "current"
    assert realpath(current) == realpath(version_dir)
    assert (current / "lap-backstop.exe").is_file()


def test_build_new_version_retargets_current_and_keeps_old_dir(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    old = build_app.build(make_options(repo, make_runner([])))
    new = build_app.build(make_options(repo, make_runner([]), version="2.0.0"))
    assert (old / "lap-backstop.exe").is_file()
    assert realpath(repo / "app" / "current") == realpath(new)


def test_build_leaves_data_and_plugins_untouched(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    version_dir = build_app.build(make_options(repo, make_runner([])))
    for dirname in ("data", "plugins"):
        assert (repo / dirname / "sentinel.txt").read_text(encoding="utf-8") == "keep"
    names = {entry.name for entry in version_dir.iterdir()}
    assert "data" not in names
    assert "plugins" not in names


def test_pyinstaller_command_carries_entry_name_and_defaults(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    calls: list[list[str]] = []
    build_app.build(make_options(repo, make_runner(calls)))
    command = calls[0]
    assert command[command.index("--name") + 1] == "lap-backstop"
    add_data = command[command.index("--add-data") + 1]
    assert add_data.startswith(str(repo / "config" / "defaults.toml"))
    assert add_data.endswith("config")
    assert command[command.index("--paths") + 1] == str(repo)
    assert command[-1].endswith(".py")


def test_pyinstaller_command_lists_dynamic_migration_imports(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    migrations = repo / "storage" / "db" / "migrations"
    migrations.mkdir(parents=True)
    for stem in ("0001_initial", "0002_add_verdicts"):
        (migrations / f"{stem}.py").write_text("def up(connection):\n", encoding="utf-8")
    calls: list[list[str]] = []
    build_app.build(make_options(repo, make_runner(calls)))
    flags = [part for part in calls[0] if part.startswith("--hidden-import=")]
    assert flags == [
        "--hidden-import=storage.db.migrations.0001_initial",
        "--hidden-import=storage.db.migrations.0002_add_verdicts",
    ]


def test_build_without_bundle_fails_before_linking_current(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)

    def empty_runner(command: Sequence[str]) -> None:
        return None

    with pytest.raises(build_app.BuildError):
        build_app.build(make_options(repo, empty_runner))
    assert not (repo / "app" / "current").exists()


def test_dry_run_prints_plan_without_touching_disk(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    repo = make_repo(tmp_path)
    build_app.build(make_options(repo, make_runner([]), is_dry_run=True))
    assert not (repo / "app").exists()
    assert not (repo / "build" / "pyinstaller").exists()
    output = capsys.readouterr().out
    assert "PyInstaller" in output
    assert "1.2.3" in output


def test_build_refuses_current_occupied_by_real_directory(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    current = repo / "app" / "current"
    current.mkdir(parents=True)
    keeper = current / "human.txt"
    keeper.write_text("人工存放的内容", encoding="utf-8")
    with pytest.raises(build_app.BuildError):
        build_app.build(make_options(repo, make_runner([])))
    assert keeper.is_file()
