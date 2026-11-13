"""帧端口：截一帧 PNG 并解码成 BGR 数组，如实报宽高，不缩放不裁剪。

adb 超时、非零退出或输出不是 PNG 一律返回 ``None``，不抛异常；缺视觉依赖
在解码处抛 ``RuntimeError`` 并提示安装命令；``sha256`` 是原始 PNG 字节的
摘要；宽高不是 1280×720 由门控映射 ``E_ENV_RESOLUTION``，帧端口不判环境。
"""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from plugins.maa.vision.judge import vision_modules

if TYPE_CHECKING:
    import numpy as np

_ADB_TIMEOUT_SECONDS = 30.0
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_CREATE_NO_WINDOW = 0x08000000
DEFAULT_ADB_PATH = Path("adb")
DEFAULT_ADDRESS = "127.0.0.1:16384"


class FrameOrigin(StrEnum):
    """帧来源通道：判定器按通道记证据来源，判定逻辑不随通道变化。"""

    ADB = "adb"
    NATIVE = "native"


class FrameSource(Protocol):
    """帧来源协议：截一帧并返回当前帧；任何截取失败都返回 None，不抛异常。"""

    def capture(self) -> Frame | None:
        """截当前一帧；失败返回 None，不抛异常。"""
        ...


class ProcessRunner(Protocol):
    """可注入的进程执行器：跑一次命令并捕获输出；超时或启动失败抛子进程异常。"""

    def __call__(
        self, args: list[str], timeout: float
    ) -> subprocess.CompletedProcess[bytes] | None:
        """执行一条命令；返回 None 表示调用方按无输出处理。"""
        ...


def subprocess_runner(args: list[str], timeout: float) -> subprocess.CompletedProcess[bytes] | None:
    """默认执行器：subprocess.run 捕获输出、带超时、不检查退出码。"""
    return subprocess.run(
        args, capture_output=True, timeout=timeout, check=False, creationflags=_CREATE_NO_WINDOW
    )


@dataclass(frozen=True)
class Frame:
    """截到的一帧：BGR 图像、宽高、UTC 截取时刻、原始 PNG 摘要与来源。"""

    image: np.ndarray
    width: int
    height: int
    captured_at: datetime
    sha256: str
    source: FrameOrigin


def _decode_bgr(data: bytes) -> np.ndarray | None:
    """PNG 字节解码成 BGR 数组；坏字节返回 None，不修改像素。"""
    cv2, numpy = vision_modules()
    raw = numpy.frombuffer(data, dtype=numpy.uint8)
    return cv2.imdecode(raw, cv2.IMREAD_COLOR)


class AdbFrameSource:
    """``FrameSource`` 的 adb 实现：每帧一次 ``exec-out screencap``，不查设备状态。"""

    def __init__(
        self,
        adb_path: Path = DEFAULT_ADB_PATH,
        address: str = DEFAULT_ADDRESS,
        runner: ProcessRunner = subprocess_runner,
    ) -> None:
        self._adb_path = adb_path
        self._address = address
        self._runner = runner

    def capture(self) -> Frame | None:
        """截一帧并解码；超时、非零退出、不是 PNG 都返回 None，不抛异常。"""
        command = [str(self._adb_path), "-s", self._address, "exec-out", "screencap", "-p"]
        try:
            completed = self._runner(command, _ADB_TIMEOUT_SECONDS)
        except (OSError, subprocess.SubprocessError):
            return None
        if completed is None or completed.returncode != 0:
            return None
        return self._build_frame(completed.stdout)

    def _build_frame(self, data: bytes) -> Frame | None:
        """PNG 字节 → ``Frame``：摘要取原始字节，时刻取解码完成后的 UTC。"""
        if not data.startswith(_PNG_SIGNATURE):
            return None
        image = _decode_bgr(data)
        if image is None:
            return None
        height, width = image.shape[:2]
        return Frame(
            image=image,
            width=width,
            height=height,
            captured_at=datetime.now(UTC),
            sha256=hashlib.sha256(data).hexdigest(),
            source=FrameOrigin.ADB,
        )
