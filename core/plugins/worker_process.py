"""worker 进程树的结束与管道回收：超时、取消与关闭共用一条路径。"""

from __future__ import annotations

import contextlib
import subprocess
import sys

KILL_TIMEOUT_SECONDS = 10


def terminate_tree(process: subprocess.Popen[str]) -> None:
    """结束进程及其子进程；Windows 用 taskkill /T，避免插件拉起的脚本变成孤儿。"""
    if process.poll() is not None:
        return
    if sys.platform == "win32":
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                timeout=KILL_TIMEOUT_SECONDS,
                check=False,
            )
    if process.poll() is None:
        process.kill()
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=KILL_TIMEOUT_SECONDS)


def close_streams(process: subprocess.Popen[str]) -> None:
    """关掉三根管道，让仍在读 stdout/stderr 的线程及时退出。"""
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            with contextlib.suppress(OSError, ValueError):
                stream.close()
