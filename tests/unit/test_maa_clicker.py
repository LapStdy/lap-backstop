"""adb 点击端口用例：命令拼装、越界拒绝与执行失败收口。"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from plugins.maa.vision.clicker import AdbClicker


class _Runner:
    """记录命令的执行器：按预设返回值返回。"""

    def __init__(self, returncode: int = 0) -> None:
        self.calls: list[tuple[list[str], float]] = []
        self._returncode = returncode

    def __call__(self, args: list[str], timeout: float) -> subprocess.CompletedProcess[bytes]:
        self.calls.append((args, timeout))
        return subprocess.CompletedProcess(args, self._returncode, b"", b"")


def test_tap_builds_adb_command() -> None:
    runner = _Runner()
    AdbClicker(adb_path=Path("adb.exe"), runner=runner).tap(640, 672)
    args, timeout = runner.calls[0]
    assert args[-3:] == ["tap", "640", "672"]
    assert "-s" in args and args[args.index("-s") + 1] == "127.0.0.1:16384"
    assert timeout > 0


def test_tap_rejects_out_of_bounds() -> None:
    with pytest.raises(ValueError):
        AdbClicker(runner=_Runner()).tap(1280, 100)
    with pytest.raises(ValueError):
        AdbClicker(runner=_Runner()).tap(100, -1)


def test_tap_failure_raises_oserror() -> None:
    with pytest.raises(OSError):
        AdbClicker(runner=_Runner(returncode=2)).tap(10, 10)
