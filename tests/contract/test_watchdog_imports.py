"""契约：看门狗导入隔离——允许集之外的依赖一律拦下（docs/08 独立看门狗一节）。

看门狗是核心死了也要照常醒来的独立短命进程，允许的依赖集合 =
标准库 + core.log_names + 看门狗自身模块，本文件是该允许集的唯一定义点。
看门狗自身覆盖三种布局：core/hostops/watchdog*.py 同目录兄弟模块、
单模块 watchdog.py、watchdog/ 包（若存在，一并扫描）。
import-linter 的 watchdog-isolation 契约只保 storage 半边：source 与
forbidden 的 core 重叠对会被 import-linter 按 as_packages 直接跳过，
core 其余子包与其余项目内依赖由这里的扫描拦截；新增越界导入必须同时
改这里的允许集常量与 .importlinter，改代码绕不过去。
"""

from __future__ import annotations

import ast
import sys
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# 看门狗允许集：标准库按 sys.stdlib_module_names 判定；项目内只放行
# core.log_names（日志文件名文法）与看门狗自身模块（watchdog 主模块、
# watchdog_* 兄弟模块与 watchdog/ 包内子模块）。
WATCHDOG_MAIN = "core.hostops.watchdog"
HOSTOPS_PACKAGE = "core.hostops"
ALLOWED_PROJECT_MODULES = frozenset({"core.log_names"})

# 越界样本（源码, 期望报出的被导入目标）：存储、core 其余子包、core 包
# 本体、宿主包上的非兄弟导入与第三方包；扫描是纯静态的，样本不要求
# 可安装可执行。
FORBIDDEN_SAMPLES: list[tuple[str, str]] = [
    ("import storage\n", "storage"),
    ("from core.worker import executor\n", "core.worker"),
    ("from core import log_names\n", "core"),
    ("from core.hostops import serve\n", "core.hostops"),
    ("import core.hostops.serve\n", "core.hostops.serve"),
    ("from core.hostops import deadman, watchdog_state\n", "core.hostops"),
    ("from pydantic import BaseModel\n", "pydantic"),
]

# 放行样本：log_names 精确命中、标准库子模块、__future__ 与看门狗兄弟
# 模块的三种导入形式（绝对、宿主包、相对）。
ALLOWED_SAMPLES: list[str] = [
    "import core.log_names\n",
    "from core.log_names import CORE_LOG_NAME\n",
    "from __future__ import annotations\n",
    "from ctypes import wintypes\n",
    "import core.hostops.watchdog_process\n",
    "from core.hostops import watchdog_state\n",
    "from core.hostops.watchdog_logtail import _latest_core_log\n",
    "from . import watchdog_state\n",
]


@dataclass(frozen=True)
class ImportViolation:
    """一处越界导入：所在模块、行号与被导入目标。"""

    module: str
    line: int
    target: str


def describe_violation(violation: ImportViolation) -> str:
    """违规报告行：模块、行号、被导入目标与允许集；文案跟允许集常量走。"""
    allowed = "、".join(sorted(ALLOWED_PROJECT_MODULES))
    return (
        f"{violation.module}:{violation.line} 越界导入 {violation.target}"
        f"（看门狗允许的依赖 = 标准库、{allowed} 与看门狗自身模块 {WATCHDOG_MAIN} 及其兄弟）"
    )


def scan_source(source: str, *, module: str, package: str) -> list[ImportViolation]:
    """扫一份源码，返回按 (模块, 行号, 目标) 排序的越界导入清单。

    module 是被扫模块名，package 是它所属包，相对导入按 package 落位：
    core/hostops 下的看门狗模块（watchdog.py 与 watchdog_* 兄弟）的
    package 是 core.hostops；包布局的 __init__ 与子模块的 package 是
    core.hostops.watchdog。标准库与允许集内的导入放行；源码语法错误按
    ast.SyntaxError 抛出，不折算成违规。
    """
    tree = ast.parse(source)
    violations = [
        ImportViolation(module=module, line=node.lineno, target=target)
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        for base, names in _statement_imports(node, package)
        for target in _offending_targets(base, names)
    ]
    return sorted(violations, key=lambda item: (item.module, item.line, item.target))


def scan_file(path: Path, module: str) -> list[ImportViolation]:
    """扫一个真实文件：按文件名推所属包，供相对导入落位。"""
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    return scan_source(path.read_text(encoding="utf-8"), module=module, package=package)


def collect_watchdog_source_files(repo_root: Path) -> list[tuple[str, Path]]:
    """看门狗源文件清单，覆盖三种布局：同目录兄弟模块（watchdog*.py）、
    单模块与 watchdog/ 包（若存在，一并扫描）。

    返回 (模块名, 路径)；一种布局都扫不到时抛 FileNotFoundError。
    """
    hostops_dir = repo_root / "core" / "hostops"
    files = [
        (_to_module_name(path.relative_to(repo_root)), path)
        for path in sorted(hostops_dir.glob("watchdog*.py"))
    ]
    package_dir = hostops_dir / "watchdog"
    if package_dir.is_dir():
        files.extend(
            (_to_module_name(path.relative_to(repo_root)), path)
            for path in sorted(package_dir.rglob("*.py"))
        )
    if not files:
        raise FileNotFoundError(f"core/hostops 下扫不到任何看门狗模块：{hostops_dir}")
    return files


def _to_module_name(relative: Path) -> str:
    """仓库内相对路径到模块名：目录逐段连接，__init__ 落在包名上。"""
    parts = list(relative.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _statement_imports(
    node: ast.Import | ast.ImportFrom, package: str
) -> list[tuple[str, tuple[str, ...]]]:
    """一条导入语句落位成 (来源模块, 导入名元组)：import 逐别名无子名，
    from 取来源模块并带上全部导入名（供宿主包豁免判定）。"""
    if isinstance(node, ast.Import):
        return [(alias.name, ()) for alias in node.names]
    base = _resolve_from_base(package, node.level, node.module)
    return [(base, tuple(alias.name for alias in node.names))]


def _resolve_from_base(package: str, level: int, module: str | None) -> str:
    """from 导入的来源模块：level 是相对点数（0 为绝对），1 级即所在包自身。"""
    if level == 0:
        return module or ""
    parts = package.split(".")
    keep = len(parts) - (level - 1)
    if keep <= 0:
        return module or "." * level
    anchor = ".".join(parts[:keep])
    return anchor if module is None else f"{anchor}.{module}"


def _is_watchdog_module(qualified: str) -> bool:
    """看门狗自身模块：主模块、watchdog_* 兄弟与 watchdog/ 包内子级；
    watchdogfoo 这类非家族前缀不算。"""
    return (
        qualified == WATCHDOG_MAIN
        or qualified.startswith(f"{WATCHDOG_MAIN}_")
        or qualified.startswith(f"{WATCHDOG_MAIN}.")
    )


def _is_allowed_source(base: str) -> bool:
    """来源模块是否在允许集：标准库、ALLOWED_PROJECT_MODULES 或看门狗自身。"""
    if base.partition(".")[0] in sys.stdlib_module_names:
        return True
    if base in ALLOWED_PROJECT_MODULES:
        return True
    return _is_watchdog_module(base)


def _offending_targets(base: str, names: tuple[str, ...]) -> list[str]:
    """一条导入语句报出的违规目标；空列表表示放行。

    from core.hostops import watchdog_state 这类宿主包导入：导入名全部
    是看门狗兄弟模块时放行，混入任何其他名字按宿主包本身报。
    """
    if _is_allowed_source(base):
        return []
    if (
        base == HOSTOPS_PACKAGE
        and names
        and all(_is_watchdog_module(f"{base}.{name}") for name in names)
    ):
        return []
    return [base]


def _read_watchdog_isolation_forbidden() -> list[str]:
    """读 .importlinter 的 watchdog-isolation 契约声明的 forbidden_modules。"""
    parser = ConfigParser()
    parser.read(REPO_ROOT / ".importlinter", encoding="utf-8")
    section = parser["importlinter:contract:watchdog-isolation"]
    return [item.strip() for item in section["forbidden_modules"].splitlines() if item.strip()]


@pytest.mark.parametrize(("source", "target"), FORBIDDEN_SAMPLES)
def test_forbidden_import_sample_is_flagged(source: str, target: str) -> None:
    """越界假样本逐条报违规：存储、core 子包、core 本体与第三方包都拦。"""
    violations = scan_source(source, module=WATCHDOG_MAIN, package="core.hostops")
    assert [(item.module, item.target) for item in violations] == [(WATCHDOG_MAIN, target)]


@pytest.mark.parametrize("source", ALLOWED_SAMPLES)
def test_allowed_import_sample_passes(source: str) -> None:
    """允许集内的样本放行：log_names、标准库、__future__ 与看门狗兄弟导入。"""
    assert scan_source(source, module=WATCHDOG_MAIN, package="core.hostops") == []


def test_violation_report_carries_module_target_and_allowed_set() -> None:
    """违规报告含三要素：违规模块、被导入目标与允许集，文案跟常量走。"""
    violations = scan_source("import storage\n", module=WATCHDOG_MAIN, package="core.hostops")
    report = describe_violation(violations[0])
    assert WATCHDOG_MAIN in report
    assert "storage" in report
    assert "core.log_names" in report


def test_module_layout_relative_import_of_non_family_member_is_flagged() -> None:
    """相对导入落在宿主包上：非看门狗家族的成员（deadman）按越界报。"""
    source = "from . import deadman\nimport core.log_names\n"
    violations = scan_source(source, module=WATCHDOG_MAIN, package="core.hostops")
    assert [(item.target, item.line) for item in violations] == [("core.hostops", 1)]


def test_package_layout_internal_imports_pass_and_outside_flagged() -> None:
    """包布局：包内相对导入放行，__init__ 与子模块的越界按各自模块名报。"""
    init = scan_source(
        "from .parser import parse\nfrom .. import deadman\n",
        module=WATCHDOG_MAIN,
        package=WATCHDOG_MAIN,
    )
    sub = scan_source("import storage\n", module=f"{WATCHDOG_MAIN}.parser", package=WATCHDOG_MAIN)
    assert [(item.module, item.target) for item in init + sub] == [
        (WATCHDOG_MAIN, "core.hostops"),
        (f"{WATCHDOG_MAIN}.parser", "storage"),
    ]


def test_real_scan_covers_watchdog_sibling_modules() -> None:
    """真实仓库扫描范围必须盖住看门狗家族：四个兄弟模块一个不能漏。"""
    collected = {path.name for _, path in collect_watchdog_source_files(REPO_ROOT)}
    assert {
        "watchdog.py",
        "watchdog_state.py",
        "watchdog_process.py",
        "watchdog_logtail.py",
    } <= collected
    assert len(collected) >= 4


def test_real_watchdog_sources_have_zero_violations() -> None:
    """对真实仓库扫一次：当前看门狗源码（兄弟、单模块或包布局）零违规。"""
    files = collect_watchdog_source_files(REPO_ROOT)
    assert files, "扫描范围为空，契约没有盖住任何文件"
    violations = [item for module, path in files for item in scan_file(path, module)]
    assert not violations, "\n".join(describe_violation(item) for item in violations)


def test_importlinter_watchdog_contract_keeps_storage_only() -> None:
    """watchdog-isolation 只声明 storage 半边：重叠的 core 对会被直接跳过。"""
    forbidden = _read_watchdog_isolation_forbidden()
    assert "storage" in forbidden
    assert "core" not in forbidden
