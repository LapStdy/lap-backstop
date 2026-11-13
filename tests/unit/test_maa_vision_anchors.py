"""MAA 视觉依赖隔离：插件模块顶层不 import cv2/numpy，缺依赖时提示安装命令。"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from plugins.maa.vision.judge import INSTALL_HINT, HomeJudge

_REPO_ROOT = Path(__file__).resolve().parents[2]
_VISION_ROOTS = {"cv2", "numpy"}
_LAZY_CONTAINERS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)


def _import_time_roots(path: Path) -> set[str]:
    """模块导入期会执行的顶层 import 根包名；函数、类与 lambda 体不执行，跳过。"""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    roots: set[str] = set()
    stack: list[ast.AST] = [tree]
    while stack:
        node = stack.pop()
        for child in ast.iter_child_nodes(node):
            if isinstance(child, _LAZY_CONTAINERS):
                continue
            if isinstance(child, ast.Import):
                roots.update(alias.name.split(".")[0] for alias in child.names)
            elif isinstance(child, ast.ImportFrom) and child.module:
                roots.add(child.module.split(".")[0])
            stack.append(child)
    return roots


def test_no_top_level_cv2_import_outside_vision() -> None:
    """AST 扫描：plugins/maa/ 下除 vision 包自身外，顶层不导入 cv2 与 numpy。"""
    package = _REPO_ROOT / "plugins" / "maa"
    offenders = []
    for path in sorted(package.rglob("*.py")):
        rel = path.relative_to(package).as_posix()
        if rel == "vision" or rel.startswith("vision/"):
            continue
        roots = _import_time_roots(path) & _VISION_ROOTS
        if roots:
            offenders.append(f"{rel}: {sorted(roots)}")
    assert offenders == [], "vision 包之外出现顶层视觉依赖导入：\n" + "\n".join(offenders)


def test_missing_dependency_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """模拟 cv2 与 numpy 都缺失：构造判定器抛 RuntimeError 并带安装命令。"""
    monkeypatch.setitem(sys.modules, "cv2", None)
    monkeypatch.setitem(sys.modules, "numpy", None)
    with pytest.raises(RuntimeError) as excinfo:
        HomeJudge()
    assert INSTALL_HINT in str(excinfo.value)
