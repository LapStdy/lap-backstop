"""MuMu 实例状态的只读解析：``MuMuManager info`` 的 JSON 与实例判据。

窗口存在不等于实例在跑：MuMu 的控制窗口在实例关闭时也留着，只按窗口判会把「实例
没起来」当成「正在启动」，准备阶段于是跳过拉起、白等到 adb 超时。实例进程与 Android
启动完成度只有 info 查询能回答，字段名随版本改（本机 2026-11-15 实测 6.5.4.0 为
``is_process_started`` / ``is_android_started``，没有 ``player_state`` 与 ``main_wnd``）。
解析不出结论时一律返回 None，由调用方退回窗口判据。
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

MUMU_MANAGER_NAME = "MuMuManager.exe"
_INFO_TIMEOUT_SECONDS = 15.0
_CREATE_NO_WINDOW = 0x08000000


@dataclass(frozen=True, slots=True)
class MumuInstance:
    """一次 info 查询的实例状态；字段与 JSON 同名同义。"""

    index: int
    name: str
    android_version: str
    is_process_started: bool
    is_android_started: bool


def parse_instance_info(text: str) -> MumuInstance | None:
    """把 info 的 JSON 解析成实例状态；error_code 非 0 或字段不合规时返回 None。"""
    try:
        payload = json.loads(text)
    except ValueError:
        return None
    if not isinstance(payload, dict) or payload.get("error_code") != 0:
        return None
    index = _as_index(payload.get("index"))
    name = payload.get("name")
    version = payload.get("android_version")
    started = payload.get("is_process_started")
    android = payload.get("is_android_started")
    if index is None or not isinstance(name, str) or not isinstance(version, str):
        return None
    if not isinstance(started, bool) or not isinstance(android, bool):
        return None
    return MumuInstance(
        index=index,
        name=name,
        android_version=version,
        is_process_started=started,
        is_android_started=android,
    )


def read_instance_info(
    manager: Path,
    index: int,
    timeout: float = _INFO_TIMEOUT_SECONDS,
) -> MumuInstance | None:
    """只读查询实例状态；命令缺失、超时、非零退出或输出不可解析都返回 None。"""
    try:
        completed = subprocess.run(
            [str(manager), "info", "-v", str(index)],
            capture_output=True,
            timeout=timeout,
            check=False,
            creationflags=_CREATE_NO_WINDOW,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return parse_instance_info(completed.stdout.decode("utf-8", errors="replace"))


def _as_index(raw: object) -> int | None:
    """index 在 JSON 里是字符串；只接受非负整数字面量。"""
    if isinstance(raw, bool) or not isinstance(raw, str) or not raw.isdigit():
        return None
    return int(raw)
