r"""打包件构建器：产出 app\releases\<version>\ 并把 app\current 联接到最新版本。

布局契约见 docs/08「路径布局」与「升级与回滚」：版本目录自包含 PyInstaller
onedir 产物，exe 直接位于版本目录根；data\ 与 plugins\ 不进版本目录，也不随
构建切换。app\current 是指向最新版本目录的目录联接，重复构建保持一致。
"""

from __future__ import annotations

import _winapi
import argparse
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tomllib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

APP_DIRNAME = "app"
RELEASES_DIRNAME = "releases"
CURRENT_DIRNAME = "current"
BUNDLE_NAME = "lap-backstop"
EXE_NAME = f"{BUNDLE_NAME}.exe"
DEFAULTS_TOML = Path("config") / "defaults.toml"
MIGRATIONS_PACKAGE = "storage.db.migrations"
MIGRATIONS_DIR = Path(*MIGRATIONS_PACKAGE.split("."))
VERSION_PATTERN = re.compile(r"[0-9A-Za-z._-]+")
BUILD_TIMEOUT_SECONDS = 3600

PyInstallerRunner = Callable[[Sequence[str]], None]

_LAUNCHER_SOURCE = """\
import sys

from core.__main__ import main

if __name__ == "__main__":
    sys.exit(main())
"""


class BuildError(RuntimeError):
    """打包或布局操作失败，消息给出可直接执行的处理办法。"""


@dataclass(frozen=True, slots=True)
class BuildOptions:
    """一次构建的全部输入；runner 可注入，单测据此跳过真实 PyInstaller。"""

    repo_root: Path
    version: str
    runner: PyInstallerRunner
    is_dry_run: bool = False


def resolve_version(repo_root: Path, override: str | None) -> str:
    """版本号：--version 优先，否则取 pyproject.toml 的 project.version。

    版本号用作版本目录名，路径保留名与分隔符一律拒绝。

    Raises:
        BuildError: pyproject.toml 缺失、字段缺失，或版本号含非法字符。
    """
    version = override if override is not None else _read_version(repo_root)
    if version in (".", "..") or VERSION_PATTERN.fullmatch(version) is None:
        raise BuildError(f"版本号含非法字符或为路径保留名：{version!r}")
    return version


def build_pyinstaller_command(repo_root: Path, work_root: Path) -> list[str]:
    r"""PyInstaller 命令：onedir 产物落在 work_root\dist\lap-backstop\。

    storage.defaults 按 __file__ 相对定位 defaults.toml，冻结后对应
    _MEIPASS\config\，add-data 的目标目录必须与之对齐，否则 exe 启动即抛错。
    迁移模块由 import_module 按编号拼名加载，PyInstaller 静态分析看不见，
    所以逐个声明 hidden-import。
    """
    launcher = work_root / "lap_backstop_entry.py"
    return [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--name",
        BUNDLE_NAME,
        "--paths",
        str(repo_root),
        *_migration_hidden_imports(repo_root),
        "--add-data",
        f"{repo_root / DEFAULTS_TOML}{os.pathsep}config",
        "--distpath",
        str(work_root / "dist"),
        "--workpath",
        str(work_root / "work"),
        "--specpath",
        str(work_root),
        str(launcher),
    ]


def replace_current_junction(app_root: Path, target: Path) -> None:
    r"""把 app\current 重建为指向 target 的目录联接。

    已存在的旧联接先摘除再创建，重复执行结果一致；联接不是普通目录，
    删除只摘联接本身，目标目录内容不受影响。

    Raises:
        BuildError: app\current 被普通目录或文件占用时拒绝覆盖。
    """
    link = app_root / CURRENT_DIRNAME
    _remove_junction(link)
    # junction 创建用 CPython 自带的 _winapi，避免为此拉起 cmd；参数方向由单测锁定。
    _winapi.CreateJunction(str(target), str(link))


def build(options: BuildOptions) -> Path:
    """执行一次构建，返回版本目录；dry_run 只打印计划，不写任何文件。

    Raises:
        BuildError: 源文件缺失、PyInstaller 失败、产物不完整或联接被占用。
    """
    version_dir = options.repo_root / APP_DIRNAME / RELEASES_DIRNAME / options.version
    work_root = options.repo_root / "build" / "pyinstaller"
    command = build_pyinstaller_command(options.repo_root, work_root)
    if options.is_dry_run:
        print(f"将执行：{' '.join(command)}")
        print(f"版本目录：{version_dir}")
        print(f"联接：{options.repo_root / APP_DIRNAME / CURRENT_DIRNAME} -> {version_dir}")
        return version_dir
    _require_defaults(options.repo_root)
    _reset_version_dir(version_dir)
    _write_launcher(work_root)
    options.runner(command)
    _flatten_bundle(work_root / "dist", version_dir)
    _require_exe(version_dir)
    replace_current_junction(options.repo_root / APP_DIRNAME, version_dir)
    return version_dir


def run_pyinstaller(command: Sequence[str]) -> None:
    """默认执行器：用当前解释器调 PyInstaller，非零退出按失败处理。

    Raises:
        BuildError: PyInstaller 未安装或执行失败。
    """
    if importlib.util.find_spec("PyInstaller") is None:
        raise BuildError("未找到 PyInstaller，请先安装：pip install pyinstaller")
    try:
        completed = subprocess.run(command, check=False, timeout=BUILD_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as error:
        raise BuildError(f"PyInstaller 超过 {BUILD_TIMEOUT_SECONDS} 秒未完成，已终止") from error
    if completed.returncode != 0:
        raise BuildError(f"PyInstaller 退出码 {completed.returncode}，命令：{' '.join(command)}")


def _read_version(repo_root: Path) -> str:
    pyproject = repo_root / "pyproject.toml"
    try:
        with pyproject.open("rb") as stream:
            version = tomllib.load(stream)["project"]["version"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise BuildError(f"无法从 {pyproject} 读取 project.version：{error}") from error
    if not isinstance(version, str):
        raise BuildError(f"{pyproject} 的 project.version 必须是字符串")
    return version


def _migration_hidden_imports(repo_root: Path) -> list[str]:
    """迁移模块按编号拼名 import_module 加载，静态分析看不见，逐个声明。"""
    pattern = "[0-9]" * 4 + "_*.py"
    stems = sorted(path.stem for path in (repo_root / MIGRATIONS_DIR).glob(pattern))
    return [f"--hidden-import={MIGRATIONS_PACKAGE}.{stem}" for stem in stems]


def _require_defaults(repo_root: Path) -> None:
    if not (repo_root / DEFAULTS_TOML).is_file():
        raise BuildError(f"缺少 {DEFAULTS_TOML}，打包件无法加载默认值")


def _reset_version_dir(version_dir: Path) -> None:
    """先清理再写入；清理范围只有版本目录本身。"""
    try:
        if version_dir.is_junction() or version_dir.is_symlink():
            os.rmdir(version_dir)
        elif version_dir.exists():
            shutil.rmtree(version_dir)
        version_dir.mkdir(parents=True)
    except OSError as error:
        raise BuildError(
            f"无法重建版本目录 {version_dir}，可能正被运行中的 exe 占用：{error}"
        ) from error


def _write_launcher(work_root: Path) -> None:
    work_root.mkdir(parents=True, exist_ok=True)
    launcher = work_root / "lap_backstop_entry.py"
    launcher.write_text(_LAUNCHER_SOURCE, encoding="utf-8")


def _flatten_bundle(dist_dir: Path, version_dir: Path) -> None:
    """把 onedir 产物平铺进版本目录根，exe 与 _internal 同级。"""
    bundle = dist_dir / BUNDLE_NAME
    if not bundle.is_dir():
        raise BuildError(f"PyInstaller 未产出 onedir 目录：{bundle}")
    for entry in sorted(bundle.iterdir()):
        shutil.move(str(entry), str(version_dir / entry.name))


def _require_exe(version_dir: Path) -> None:
    exe = version_dir / EXE_NAME
    if not exe.is_file():
        raise BuildError(f"版本目录缺少入口 {EXE_NAME}，构建产物不完整：{version_dir}")


def _remove_junction(link: Path) -> None:
    if not os.path.lexists(link):
        return
    if link.is_junction() or link.is_symlink():
        os.rmdir(link)
        return
    raise BuildError(f"{link} 不是目录联接，拒绝覆盖；请人工确认后删除再构建")


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口：--version 覆盖版本号，--dry-run 只打印计划。"""
    parser = argparse.ArgumentParser(
        prog="build_app",
        description="把仓库打成自包含单目录 app\\releases\\<version>\\，并重建 app\\current 联接",
    )
    parser.add_argument(
        "--version", default=None, help="版本号，默认取 pyproject.toml 的 project.version"
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印将执行的动作，不写盘")
    arguments = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    options = BuildOptions(
        repo_root=repo_root,
        version=resolve_version(repo_root, arguments.version),
        runner=run_pyinstaller,
        is_dry_run=arguments.dry_run,
    )
    version_dir = build(options)
    if not options.is_dry_run:
        print(f"版本目录：{version_dir}")
        print(f"入口：{version_dir / EXE_NAME}")
        print(f"当前版本：{options.repo_root / APP_DIRNAME / CURRENT_DIRNAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
