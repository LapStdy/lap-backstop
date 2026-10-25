"""核心心跳文件：原子写与陈旧度计算。

核心按 ``[host].heartbeat_interval_seconds`` 周期写；看门狗判死只依赖文件存在、
mtime 与内容里的 pid（docs/08 独立看门狗一节）。写法是同目录临时文件加
``os.replace``：Windows 同卷替换是原子的，读者不会看到半截内容。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TypedDict

HEARTBEAT_FILENAME = "core.heartbeat"


class HeartbeatContent(TypedDict):
    """心跳文件 schema（docs/08 独立看门狗一节的五个字段）。"""

    pid: int
    started_at: str
    version: str
    last_tick: str
    active_run: str | None


def write_heartbeat(path: Path, content: HeartbeatContent) -> None:
    """原子写心跳：先写同目录临时文件，再 ``os.replace`` 顶替。

    Raises:
        OSError: 目录不可写或替换失败。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    payload = json.dumps(content, ensure_ascii=False, sort_keys=True)
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(payload)
    os.replace(temporary, path)


def read_heartbeat(path: Path) -> HeartbeatContent | None:
    """读心跳；文件缺失、内容损坏或字段不齐返回 None，调用方按无心跳处置。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    pid = data.get("pid")
    started_at = data.get("started_at")
    version = data.get("version")
    last_tick = data.get("last_tick")
    active_run = data.get("active_run")
    if not isinstance(pid, int) or isinstance(pid, bool):
        return None
    if not (
        isinstance(started_at, str)
        and isinstance(version, str)
        and isinstance(last_tick, str)
        and (active_run is None or isinstance(active_run, str))
    ):
        return None
    return {
        "pid": pid,
        "started_at": started_at,
        "version": version,
        "last_tick": last_tick,
        "active_run": active_run,
    }


def heartbeat_age_seconds(path: Path, *, now: float) -> float | None:
    """心跳陈旧度（秒），以文件 mtime 计；文件不可见返回 None。

    ``now`` 是当前墙钟的 epoch 秒；判死口径是「超过 [host].stale_seconds」。
    """
    try:
        return now - path.stat().st_mtime
    except OSError:
        return None


__all__ = [
    "HEARTBEAT_FILENAME",
    "HeartbeatContent",
    "heartbeat_age_seconds",
    "read_heartbeat",
    "write_heartbeat",
]
