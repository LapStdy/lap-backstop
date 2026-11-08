"""模拟器动作端口的 Windows 实现：拉起实例、连 adb、判断与拉起游戏包。

environment 能力的 ``prepare`` 只经这个端口改外部状态，单元测试注入假件。连接前
先 ``kill-server``：MuMu 实例的 adb 端口会先于 adbd 就绪，直接 connect 会留下
offline 条目（2026-11-08 实测）。模块只把一次动作发出去，不判断环境是否就绪。
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Protocol

_CREATE_NO_WINDOW = 0x08000000
_MANAGER_SECONDS = 60.0
_ADB_SECONDS = 30.0
_GAME_SECONDS = 30.0
_MANAGER_NAME = "MuMuManager.exe"
_ADB_NAME = "adb.exe"
_MUMU_BIN_DIR = "nx_main"
_LAUNCHER_CATEGORY = "android.intent.category.LAUNCHER"


class EmulatorHost(Protocol):
    """模拟器动作端口：四个方法都改变外部状态，失败抛 OSError。"""

    def start_instance(self, instance_index: int) -> None:
        """拉起指定序号的模拟器实例；已在运行时由实现自行复用。"""
        ...

    def connect_adb(self) -> None:
        """重连模拟器 adb：先清服务再 connect。"""
        ...

    def is_package_running(self, package: str) -> bool:
        """包进程是否在设备上存活。"""
        ...

    def launch_package(self, package: str) -> None:
        """通过 LAUNCHER intent 拉起应用。"""
        ...


class WindowsEmulatorHost:
    """Windows 实现：MuMuManager 拉起实例，adb 连设备与拉起游戏。"""

    def __init__(self, mumu_dir: Path, adb_address: str) -> None:
        self._manager = mumu_dir / _MUMU_BIN_DIR / _MANAGER_NAME
        self._adb = mumu_dir / _MUMU_BIN_DIR / _ADB_NAME
        self._address = adb_address

    def start_instance(self, instance_index: int) -> None:
        """调用 MuMuManager 的 control launch；失败抛 OSError。"""
        self._run(
            [str(self._manager), "control", "-v", str(instance_index), "launch"],
            _MANAGER_SECONDS,
            check=True,
        )

    def connect_adb(self) -> None:
        """先 kill-server 再 connect；两步都不看退出码，连不上由探测判定。"""
        self._run([str(self._adb), "kill-server"], _ADB_SECONDS, check=False)
        self._run([str(self._adb), "connect", self._address], _ADB_SECONDS, check=False)

    def is_package_running(self, package: str) -> bool:
        """pidof 有输出即算在跑；adb 失联按不在跑处理，交给拉起与超时路径。"""
        completed = self._run(
            [str(self._adb), "-s", self._address, "shell", "pidof", package],
            _ADB_SECONDS,
            check=False,
        )
        if completed is None or completed.returncode != 0:
            return False
        return completed.stdout.strip() != b""

    def launch_package(self, package: str) -> None:
        """monkey 走 LAUNCHER intent；拉起失败不在这里判定，等前台超时。"""
        self._run(
            [
                str(self._adb),
                "-s",
                self._address,
                "shell",
                "monkey",
                "-p",
                package,
                "-c",
                _LAUNCHER_CATEGORY,
                "1",
            ],
            _GAME_SECONDS,
            check=False,
        )

    def _run(
        self,
        args: list[str],
        timeout: float,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes] | None:
        """执行一次外部命令；check 为真时非零退出折成 OSError 交上层记失败。"""
        try:
            completed = subprocess.run(
                args,
                capture_output=True,
                timeout=timeout,
                check=False,
                creationflags=_CREATE_NO_WINDOW,
            )
        except (OSError, subprocess.SubprocessError) as error:
            if not check:
                return None
            raise OSError(f"命令执行失败：{args[0]}：{error}") from error
        if check and completed.returncode != 0:
            raise OSError(f"命令返回 {completed.returncode}：{args[0]}")
        return completed
