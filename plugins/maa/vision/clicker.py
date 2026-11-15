"""adb 点击端口：点一次 1280×720 安全坐标，越界拒绝，命令失败抛 OSError。

坐标空间是模拟器画面（docs/arknights/01），与 Windows 窗口位置和显示缩放无关。
执行器可注入，测试不真的点。
"""

from __future__ import annotations

from pathlib import Path

from plugins.maa.vision.frames import DEFAULT_ADDRESS, ProcessRunner, subprocess_runner

FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
_TAP_TIMEOUT_SECONDS = 30.0


class AdbClicker:
    """``Clicker`` 的 adb 实现：每条命令都带 ``-s`` 地址，越界坐标直接拒绝。"""

    def __init__(
        self,
        adb_path: Path = Path("adb"),
        address: str = DEFAULT_ADDRESS,
        runner: ProcessRunner = subprocess_runner,
    ) -> None:
        self._adb_path = adb_path
        self._address = address
        self._runner = runner

    def tap(self, x: int, y: int) -> None:
        """点一次坐标；越界抛 ``ValueError``，执行失败抛 ``OSError``。"""
        if not (0 <= x < FRAME_WIDTH and 0 <= y < FRAME_HEIGHT):
            raise ValueError(f"点击坐标越界：({x}, {y})")
        command = [
            str(self._adb_path),
            "-s",
            self._address,
            "shell",
            "input",
            "tap",
            str(x),
            str(y),
        ]
        completed = self._runner(command, _TAP_TIMEOUT_SECONDS)
        if completed is None or completed.returncode != 0:
            code = None if completed is None else completed.returncode
            raise OSError(f"adb 点击失败：退出码 {code}")
