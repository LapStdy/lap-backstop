"""pre-commit 钩子端到端：临时仓库里 gate 失败拒提交、全绿放行。"""

from __future__ import annotations

import importlib.util
import subprocess
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


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    return completed


def _git_raw(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )


def make_repo(tmp_path: Path) -> Path:
    """最小 git 仓库：身份配置好，scripts/ 目录就位。"""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "gate-test")
    _git(repo, "config", "user.email", "gate-test@example.com")
    (repo / "scripts").mkdir()
    return repo


def write_gate(repo: Path, exit_code: int) -> None:
    (repo / "scripts" / "gate.py").write_text(
        f"import sys\nsys.exit({exit_code})\n", encoding="utf-8"
    )


def test_pre_commit_rejects_commit_when_gate_fails(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    write_gate(repo, 1)
    install_git_hooks.install_pre_commit_hook(repo)
    rejected = _git_raw(repo, "commit", "--allow-empty", "-m", "gate-fail")
    assert rejected.returncode != 0
    assert _git_raw(repo, "rev-parse", "--verify", "HEAD").returncode != 0


def test_pre_commit_allows_commit_when_gate_passes(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    write_gate(repo, 0)
    install_git_hooks.install_pre_commit_hook(repo)
    _git(repo, "commit", "--allow-empty", "-m", "gate-pass")
    assert _git(repo, "rev-parse", "--verify", "HEAD").returncode == 0
