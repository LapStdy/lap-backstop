"""install_git_hooks 用例：钩子内容、可重复执行与旧钩子备份。"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "install_git_hooks.py"


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location("install_git_hooks_under_test", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


install_git_hooks = _load_script()


def test_installs_hook_with_gate_command(tmp_path: Path) -> None:
    result = install_git_hooks.install_pre_commit_hook(tmp_path)
    content = result.hook_path.read_text(encoding="utf-8")
    assert content == install_git_hooks.HOOK_TEMPLATE
    assert "python scripts/gate.py" in content
    assert "\r\n" not in content
    if os.name != "nt":
        assert result.hook_path.stat().st_mode & 0o111
    assert result.backup_path is None


def test_reinstall_without_changes_keeps_no_backup(tmp_path: Path) -> None:
    first = install_git_hooks.install_pre_commit_hook(tmp_path)
    second = install_git_hooks.install_pre_commit_hook(tmp_path)
    assert second.hook_path.read_text(encoding="utf-8") == first.hook_path.read_text(
        encoding="utf-8"
    )
    assert second.backup_path is None


def test_install_backs_up_foreign_hook(tmp_path: Path) -> None:
    hooks = tmp_path / ".git" / "hooks"
    hooks.mkdir(parents=True)
    hooks.joinpath("pre-commit").write_text("echo legacy\n", encoding="utf-8")
    result = install_git_hooks.install_pre_commit_hook(tmp_path)
    assert result.backup_path is not None
    assert result.backup_path.read_text(encoding="utf-8") == "echo legacy\n"
    assert result.hook_path.read_text(encoding="utf-8") == install_git_hooks.HOOK_TEMPLATE


def test_reinstall_after_backup_keeps_single_backup(tmp_path: Path) -> None:
    hooks = tmp_path / ".git" / "hooks"
    hooks.mkdir(parents=True)
    hooks.joinpath("pre-commit").write_text("echo legacy\n", encoding="utf-8")
    install_git_hooks.install_pre_commit_hook(tmp_path)
    again = install_git_hooks.install_pre_commit_hook(tmp_path)
    assert again.backup_path is None
    assert len(list(hooks.glob("*.bak"))) == 1
