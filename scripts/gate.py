"""六条门禁命令的统一入口。

命令清单与顺序见 docs/12 的门禁一节；本地与 CI 共用这一份口径。
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

LINT_IMPORTS_FALLBACK = "from importlinter.cli import lint_imports_command; lint_imports_command()"
TIMEOUT_SECONDS = 600


def _env() -> dict[str, str]:
    """子进程统一用 UTF-8，避免中文路径与配置在 GBK 默认编码下报错。"""
    return {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}


def _lint_imports_command() -> list[str]:
    executable = shutil.which("lint-imports")
    if executable is not None:
        return [executable]
    return [sys.executable, "-c", LINT_IMPORTS_FALLBACK]


def commands() -> list[list[str]]:
    return [
        [sys.executable, "-m", "ruff", "check", "."],
        [sys.executable, "-m", "ruff", "format", "--check", "."],
        [sys.executable, "-m", "mypy"],
        [sys.executable, "-m", "pytest", "-q"],
        _lint_imports_command(),
        [sys.executable, "scripts/check_artifacts.py", "."],
    ]


def main() -> int:
    for command in commands():
        printable = " ".join(command)
        print(f"\n$ {printable}")
        try:
            completed = subprocess.run(
                command,
                cwd=ROOT,
                check=False,
                env=_env(),
                timeout=TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            print(f"门禁超时（{TIMEOUT_SECONDS} 秒）：{printable}")
            return 1
        if completed.returncode != 0:
            print(f"门禁失败：{printable}")
            return completed.returncode
    print("\n六条门禁全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
