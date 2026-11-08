"""配置文件的宿主端口：读取、原子替换、备份枚举、删除与进程占用查询。

config_projection 只经这个端口碰 ``gui.new.json`` 与进程表，单元测试注入假件。
写入走同目录临时文件再 ``os.replace``：MAA 读配置时只会看到整份旧内容或整份新内容，
不会读到写了一半的文件。查进程查不出来时按「在跑」处理——写配置的前提是 MAA 必须
没在跑，放行等于绕过这个前提。
"""

from __future__ import annotations

import os
import subprocess
from contextlib import suppress
from pathlib import Path
from typing import Protocol

_TASKLIST_SECONDS = 15.0
_CREATE_NO_WINDOW = 0x08000000
_TEMP_SUFFIX = ".tmp"
_BACKUP_SUFFIX = ".bak"


class ConfigStore(Protocol):
    """配置文件的宿主端口：五个方法只操作配置目录与进程表。"""

    def read(self, path: Path) -> bytes:
        """读取整份文件；不可读时抛 OSError。"""
        ...

    def write(self, path: Path, data: bytes) -> None:
        """原子替换整份文件；失败时抛 OSError。"""
        ...

    def backups(self, config_path: Path) -> tuple[Path, ...]:
        """按文件名升序列出配置文件的 ``<文件名>.<修订>.bak``。"""
        ...

    def delete(self, path: Path) -> None:
        """删除一个备份文件；文件不存在时静默返回。"""
        ...

    def process_running(self, image_name: str) -> bool:
        """映像名对应的进程是否在运行；查询失败按在跑处理。"""
        ...


class WindowsConfigStore:
    """Windows 实现：字节读写、同目录临时文件替换、tasklist 查进程。"""

    def read(self, path: Path) -> bytes:
        """读取整份文件，保持原始字节与 BOM 状态。"""
        return path.read_bytes()

    def write(self, path: Path, data: bytes) -> None:
        """先写同目录临时文件再替换；失败时清掉临时文件并上抛。"""
        temp = path.with_name(f"{path.name}{_TEMP_SUFFIX}")
        try:
            temp.write_bytes(data)
            os.replace(temp, path)
        except OSError:
            with suppress(OSError):
                temp.unlink(missing_ok=True)
            raise

    def backups(self, config_path: Path) -> tuple[Path, ...]:
        """只认 ``<文件名>.<修订>.bak``；不带修订的 ``.bak`` 不归本平台管理。"""
        pattern = f"{config_path.name}.*{_BACKUP_SUFFIX}"
        found = tuple(
            path
            for path in config_path.parent.glob(pattern)
            if path.is_file() and path.name.startswith(f"{config_path.name}.")
        )
        return tuple(sorted(found, key=lambda path: path.name))

    def delete(self, path: Path) -> None:
        """删除一个备份；已被别的进程删掉时不算失败。"""
        path.unlink(missing_ok=True)

    def process_running(self, image_name: str) -> bool:
        """按映像名查进程；tasklist 不可用时按在跑处理，让写入失败关闭。"""
        try:
            completed = subprocess.run(
                ["tasklist", "/FI", f"IMAGENAME eq {image_name}", "/FO", "CSV", "/NH"],
                capture_output=True,
                timeout=_TASKLIST_SECONDS,
                check=False,
                creationflags=_CREATE_NO_WINDOW,
            )
        except (OSError, subprocess.SubprocessError):
            return True
        return image_name in completed.stdout.decode("utf-8", errors="replace")
