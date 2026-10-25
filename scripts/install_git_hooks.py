"""安装提交前钩子：pre-commit 跑 scripts/gate.py，未全绿拒绝提交。

钩子装进 git 公共目录（多 worktree 共用一份）；已有内容不同的旧钩子先
备份为 pre-commit.<时刻>.bak 再覆盖，内容一致则只确保可执行位，可重复执行。
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

HOOK_NAME = "pre-commit"
HOOK_TEMPLATE = """#!/bin/sh
# lap-backstop 提交前门禁：七条 gate 全绿才允许提交；由 scripts/install_git_hooks.py 安装。
cd "$(git rev-parse --show-toplevel)" || exit 1
python scripts/gate.py || {
    echo "提交被拒：python scripts/gate.py 未全绿，先整改再提交。" >&2
    exit 1
}
"""

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True, slots=True)
class HookInstall:
    """一次安装的结果；backup_path 是被替换旧钩子的备份，内容一致时为 None。"""

    hook_path: Path
    backup_path: Path | None


def install_pre_commit_hook(repo_root: Path) -> HookInstall:
    """把 pre-commit 钩子写进 repo_root 的 git 公共 hooks 目录。

    Raises:
        OSError: 目录创建、读取旧钩子或写入失败。
    """
    hooks_dir = _hooks_dir(repo_root)
    hooks_dir.mkdir(parents=True, exist_ok=True)
    hook_path = hooks_dir / HOOK_NAME
    template = HOOK_TEMPLATE.encode("utf-8")
    backup_path: Path | None = None
    if hook_path.is_file() and hook_path.read_bytes() != template:
        backup_path = _reserve_backup_path(hooks_dir)
        backup_path.write_bytes(hook_path.read_bytes())
    hook_path.write_text(HOOK_TEMPLATE, encoding="utf-8", newline="\n")
    hook_path.chmod(0o755)
    return HookInstall(hook_path=hook_path, backup_path=backup_path)


def _reserve_backup_path(hooks_dir: Path) -> Path:
    """备份名带时刻，同一秒内重复覆盖也不互相顶掉。"""
    moment = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_path = hooks_dir / f"{HOOK_NAME}.{moment}.bak"
    counter = 1
    while backup_path.exists():
        backup_path = hooks_dir / f"{HOOK_NAME}.{moment}.{counter}.bak"
        counter += 1
    return backup_path


def _hooks_dir(repo_root: Path) -> Path:
    """git 公共目录下的 hooks；不在仓库里或 git 不可用时退回 .git/hooks。"""
    fallback = repo_root / ".git" / "hooks"
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--git-common-dir"],
            capture_output=True,
            check=False,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
    except OSError:
        return fallback
    if completed.returncode != 0 or not completed.stdout.strip():
        return fallback
    gitdir = Path(completed.stdout.strip())
    if not gitdir.is_absolute():
        gitdir = repo_root / gitdir
    return gitdir / "hooks"


def main() -> int:
    """安装到当前仓库，打印落点与备份位置。"""
    result = install_pre_commit_hook(ROOT)
    print(f"已安装 {result.hook_path}")
    if result.backup_path is not None:
        print(f"旧钩子备份于 {result.backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
