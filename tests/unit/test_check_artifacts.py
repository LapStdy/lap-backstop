"""交付物检查的路径豁免：工作树池里的检出不是交付物。

DSH 会话的工作树放在仓库内 `.agents/worktrees`，检出内容是同一份代码的副本；
交付物检查若走进去，会把主区的门禁结果绑到工作树的内容上。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "check_artifacts.py"


def _load_checker() -> Any:
    # 被测脚本按文件路径加载时 scripts 目录不会自动进 sys.path；补上它，
    # 导入解析才与「python scripts/check_artifacts.py」的运行方式等价。
    sys.path.insert(0, str(_SCRIPT_PATH.parent))
    spec = importlib.util.spec_from_file_location("check_artifacts_under_test", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # importlib 加载的模块先注册进 sys.modules，避免机制按名回查时找不到。
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


checker = _load_checker()


def test_worktree_pool_is_exempt(tmp_path: Path) -> None:
    target = tmp_path / ".agents" / "worktrees" / "dsh-demo" / "note.md"
    target.parent.mkdir(parents=True)
    target.write_text("正文\n", encoding="utf-8")

    assert checker.is_exempt(target, tmp_path) is True


def test_ordinary_doc_is_not_exempt(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "note.md"
    target.parent.mkdir()
    target.write_text("正文\n", encoding="utf-8")

    assert checker.is_exempt(target, tmp_path) is False


def test_collect_skips_worktree_pool(tmp_path: Path) -> None:
    target = tmp_path / ".agents" / "worktrees" / "dsh-demo" / "note.md"
    target.parent.mkdir(parents=True)
    target.write_text("正文\n", encoding="utf-8")

    assert checker.collect([str(tmp_path)], tmp_path) == []
