"""需求追溯：docs/01 的需求表与测试模块声明双向核对。

测试模块首部写 ``REQS = ("P-101", "P-102")`` 声明覆盖的需求；未覆盖清单在
``tests/fixtures/requirement_baseline.txt``，只缩不涨：基线里多出来的条目说明
该需求已经有用例，把那一行删掉。
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "01-需求与边界.md"
TESTS = ROOT / "tests"
BASELINE = ROOT / "tests" / "fixtures" / "requirement_baseline.txt"
ASSIGN_NAME = "REQS"
ROW_RE = re.compile(r"^\|\s*((?:P|M|A|E|N|S|AC)-\d+)\s*\|")


def documented() -> set[str]:
    """需求表中第一列出现的编号。"""
    ids: set[str] = set()
    for line in DOC.read_text(encoding="utf-8").splitlines():
        match = ROW_RE.match(line.strip())
        if match is not None:
            ids.add(match.group(1))
    return ids


def declared() -> dict[str, set[str]]:
    """测试模块的 REQS 声明：编号 → 声明它的文件集合。"""
    found: dict[str, set[str]] = {}
    for path in sorted(TESTS.rglob("test_*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if ASSIGN_NAME not in names or not isinstance(node.value, (ast.Tuple, ast.List)):
                continue
            for element in node.value.elts:
                value = getattr(element, "value", None)
                if isinstance(value, str):
                    found.setdefault(value, set()).add(str(path.relative_to(ROOT)))
    return found


def baseline() -> set[str]:
    """未覆盖清单；文件缺失按空清单处理，让核对给出完整缺口。"""
    if not BASELINE.is_file():
        return set()
    return {
        line.strip() for line in BASELINE.read_text(encoding="utf-8").splitlines() if line.strip()
    }


def test_declared_ids_exist_in_document() -> None:
    unknown = sorted(set(declared()) - documented())
    assert not unknown, f"测试声明了需求表里没有的编号：{unknown}"


def test_uncovered_requirements_match_baseline() -> None:
    uncovered = documented() - set(declared())
    base = baseline()
    added = sorted(uncovered - base)
    stale = sorted(base - uncovered)
    assert not added, f"新增未被用例覆盖的需求：{added}；补用例，或写进 requirement_baseline.txt"
    assert not stale, f"这些需求已经有用例：{stale}；从 requirement_baseline.txt 删掉对应行"
