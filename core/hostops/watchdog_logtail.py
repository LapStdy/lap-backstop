"""核心日志尾部采集与文件名词法：定位最新核心日志并按行数与字符上限取尾部。

核心日志单文件最大 200 MB，全量读会撞上看门狗的 3 秒自约束：只从文件尾
取一个字节窗口再裁行数，尾部随事件 payload 进 SSE 帧，再按总字符封顶。
最新文件的挑选按 core.log_names 的文法解析日期与续号，结果不依赖目录
枚举顺序。
"""

from __future__ import annotations

import os
from pathlib import Path

from core.log_names import CORE_LOG_NAME

_LOG_TAIL_LINES = 50
# 核心日志单文件最大 200 MB：全量读会撞上 3 秒自约束，只从文件尾取一个字节窗口再裁行数。
_LOG_TAIL_WINDOW_BYTES = 262_144
# 尾部随事件 payload 进 SSE 帧：只裁行数时一行可以很长，按总字符再封顶一次。
_LOG_TAIL_MAX_CHARS = 8_192


def _latest_core_log(logs_dir: Path) -> Path | None:
    """按名字里的日期与续号挑最新的 core-YYYY-MM-DD[.N].jsonl。"""
    try:
        entries = list(logs_dir.iterdir())
    except OSError:
        return None
    candidates: list[tuple[tuple[str, int], str, Path]] = []
    for path in entries:
        parsed = _parse_core_log_name(path.name)
        if parsed is None or not path.is_file():
            continue
        candidates.append((parsed, path.name, path))
    if not candidates:
        return None
    # 续号相同（基名与 .1 都按续号 1 解析）时再用文件名定序，结果不依赖 iterdir 顺序。
    return max(candidates)[2]


def _parse_core_log_name(name: str) -> tuple[str, int] | None:
    """解析核心日志文件名；返回日期串与续号（无续号按 1）。"""
    match = CORE_LOG_NAME.fullmatch(name)
    if match is None:
        return None
    index = match.group("index")
    return match.group("day"), 1 if index is None else int(index)


def _read_tail_lines(path: Path) -> list[str]:
    """读文件尾部的字节窗口，返回最后 50 条非空行（跳过空行），总字符不超上限。"""
    try:
        with path.open("rb") as stream:
            size = stream.seek(0, os.SEEK_END)
            start = max(0, size - _LOG_TAIL_WINDOW_BYTES)
            stream.seek(start)
            data = stream.read()
    except OSError:
        return []
    lines = data.decode("utf-8", errors="replace").split("\n")
    if start > 0 and lines:
        # 窗口起点可能落在行中间，第一行是半截，弃掉。
        lines = lines[1:]
    return _cap_tail_chars([line for line in lines if line.strip()][-_LOG_TAIL_LINES:])


def _cap_tail_chars(lines: list[str]) -> list[str]:
    """从新往旧累计到总字符上限；最新一行自身超限时只留它的前段。"""
    kept: list[str] = []
    used = 0
    for line in reversed(lines):
        if used + len(line) <= _LOG_TAIL_MAX_CHARS:
            kept.append(line)
            used += len(line)
            continue
        if not kept:
            kept.append(line[:_LOG_TAIL_MAX_CHARS])
        break
    return list(reversed(kept))
