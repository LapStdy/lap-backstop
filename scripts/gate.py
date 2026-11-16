"""七条门禁命令的统一入口。

命令清单与顺序见 docs/12 的门禁一节；本地与 CI 共用这一份口径。
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass

LINT_IMPORTS_FALLBACK = "from importlinter.cli import lint_imports_command; lint_imports_command()"
TIMEOUT_SECONDS = 600
# pytest 轴要等真实宿主的超时、心跳与端口探测，耗时随宿主负载漂移（本机实测 654 秒），
# 与照抄一份就结束的 lint 轴共用一个预算，会把贴线跑动压成假红。
PYTEST_TIMEOUT_SECONDS = 900


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
        [sys.executable, "scripts/check_contracts.py"],
        [sys.executable, "scripts/check_artifacts.py", "."],
    ]


def timeout_for(command: list[str]) -> int:
    """按轴的耗时特征取预算：只有 pytest 轴吃宿主事件，其余按默认值截停。"""
    if "-m" in command:
        index = command.index("-m")
        if command[index + 1 : index + 2] == ["pytest"]:
            return PYTEST_TIMEOUT_SECONDS
    return TIMEOUT_SECONDS


def main() -> int:
    for command in commands():
        printable = " ".join(command)
        budget = timeout_for(command)
        print(f"\n$ {printable}")
        started = time.monotonic()
        try:
            completed = subprocess.run(
                command,
                cwd=ROOT,
                check=False,
                env=_env(),
                timeout=budget,
            )
        except subprocess.TimeoutExpired:
            spent = time.monotonic() - started
            print(f"门禁超时（预算 {budget} 秒，已跑 {spent:.0f} 秒）：{printable}")
            return 1
        print(f"用时 {time.monotonic() - started:.1f} 秒")
        if completed.returncode != 0:
            print(f"门禁失败：{printable}")
            return completed.returncode
    print("\n七条门禁全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
