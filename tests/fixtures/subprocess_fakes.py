"""subprocess 假件：记录调用并按配置返回，供单元测试注入 runner。"""

from __future__ import annotations

import subprocess
from collections.abc import Callable

FakeRunner = Callable[..., subprocess.CompletedProcess[bytes]]


def fake_runner(
    returncode: int = 0,
) -> tuple[FakeRunner, list[list[str]]]:
    """假命令执行器：一律返回指定退出码，并把每条命令记录下来。"""
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(list(command))
        return subprocess.CompletedProcess(command, returncode, b"", b"")

    return run, calls


def completed(
    stdout: bytes = b"",
    returncode: int = 0,
) -> subprocess.CompletedProcess[bytes]:
    """构造一条预设结果，供按命令路由的假执行器使用。"""
    return subprocess.CompletedProcess([], returncode, stdout, b"")
