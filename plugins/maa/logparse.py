"""MAA 日志解析：gui.log 与 asst.log 的信号识别与按字节偏移的增量读取。

信号清单与语义真源是 docs/arknights/02 的「日志信号」表。识别用子串匹配
而非列对齐：GUI 日志的列宽随版本漂移，信号文案才是文档承诺的稳定面。
asst.log 只提取 SanityBeforeStage 理智采样；AllTasksCompleted 在整轮
出错时同样触发，不作为完成依据。

时间戳取自行首：gui.log 是裸时间戳，asst.log 带方括号前缀。MAA 写本地
墙钟时间且不带时区标记，按宿主本地时区补全（docs/11：不外传 naive
datetime）。读取只消费完整行，尾部半行留到下一轮；文件变短视为滚动或
截断，偏移归零重读。解码坏字节替换为 U+FFFD：观测不能因个别坏字节中断。
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

LOG_DIR_NAME = "debug"
GUI_LOG_NAME = "gui.log"
ASST_LOG_NAME = "asst.log"

KIND_TASK_STARTED = "task.started"
KIND_TASK_COMPLETED = "task.completed"
KIND_TASK_ERROR = "task.error"
KIND_RUN_COMPLETED = "run.completed"
KIND_RUN_FAILED = "run.failed"
KIND_RUN_DISCONNECTED = "run.disconnected"
KIND_SANITY_SAMPLE = "sanity.sample"

_TASK_RULES: tuple[tuple[str, str], ...] = (
    ("开始任务: ", KIND_TASK_STARTED),
    ("完成任务: ", KIND_TASK_COMPLETED),
    ("任务出错: ", KIND_TASK_ERROR),
)
_RUN_RULES: tuple[tuple[str, str], ...] = (
    ("任务已全部完成！", KIND_RUN_COMPLETED),
    ("任务已结束，以下任务出现错误:", KIND_RUN_FAILED),
    ("游戏掉线，停止任务", KIND_RUN_DISCONNECTED),
)
_TASK_KINDS = frozenset(kind for _, kind in _TASK_RULES)

_SANITY_MARKER = "SanityBeforeStage"
_SANITY_KEYS = ("current_sanity", "max_sanity")

_HEAD_TIME_RE = re.compile(r"^\[?(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:[.,](\d{1,6}))?")


@dataclass(frozen=True, slots=True)
class ParsedSignal:
    """一条已识别的日志信号：名称、时间、参数与原始行。"""

    kind: str
    payload: Mapping[str, Any]
    at: datetime | None
    line: str


@dataclass(frozen=True, slots=True)
class LogChunk:
    """一轮增量读取的结果：完整新行、下一轮字节起点、文件是否变短。"""

    lines: tuple[str, ...]
    offset: int
    truncated: bool


def parse_gui_line(line: str) -> ParsedSignal | None:
    """识别 gui.log 单行；命中已知信号返回结构化信号，未知行返回 None。"""
    for needle, kind in (*_TASK_RULES, *_RUN_RULES):
        if needle not in line:
            continue
        payload = {"task": _text_after(line, needle)} if kind in _TASK_KINDS else {}
        return ParsedSignal(kind=kind, payload=payload, at=_head_time(line), line=line)
    return None


def parse_asst_line(line: str) -> ParsedSignal | None:
    """识别 asst.log 单行；只提取 SanityBeforeStage 采样，其余返回 None。"""
    if _SANITY_MARKER not in line:
        return None
    payload = _sanity_payload(line)
    if payload is None:
        return None
    return ParsedSignal(kind=KIND_SANITY_SAMPLE, payload=payload, at=_head_time(line), line=line)


def file_size(path: Path) -> int:
    """返回文件当前字节数；文件不存在按 0 处理（等价于从头读）。"""
    try:
        return path.stat().st_size
    except OSError:
        return 0


def read_new_lines(path: Path, offset: int) -> LogChunk:
    """从字节偏移读取新增的完整行；文件变短视为滚动或截断，从头重读。"""
    try:
        with path.open("rb") as stream:
            size = stream.seek(0, io.SEEK_END)
            if size == offset:
                return LogChunk((), offset, truncated=False)
            start = 0 if size < offset else offset
            truncated = size < offset
            stream.seek(start)
            chunk = stream.read()
    except OSError:
        return LogChunk((), offset, truncated=False)
    last_newline = chunk.rfind(b"\n")
    if last_newline < 0:
        return LogChunk((), start, truncated=truncated)
    text = chunk[:last_newline].decode("utf-8", errors="replace")
    return LogChunk(
        # Windows 日志以 CRLF 落盘，行尾回车不属于信号内容。
        lines=tuple(line.removesuffix("\r") for line in text.split("\n")),
        offset=start + last_newline + 1,
        truncated=truncated,
    )


def _text_after(line: str, needle: str) -> str:
    """取信号文案之后的任务名；一行只含一个已知信号文案。"""
    return line.split(needle, 1)[1].strip()


def _head_time(line: str) -> datetime | None:
    """解析行首时间戳；无时间戳或非法日期返回 None。"""
    match = _HEAD_TIME_RE.match(line)
    if match is None:
        return None
    date_text, time_text, fraction = match.groups()
    microsecond = int(fraction.ljust(6, "0")) if fraction else 0
    try:
        local = datetime.fromisoformat(f"{date_text}T{time_text}.{microsecond:06d}")
    except ValueError:
        return None
    return local.astimezone()


def _sanity_payload(line: str) -> dict[str, int] | None:
    """从行内首个 JSON 对象收集理智数值；两项齐备才算一次采样。"""
    start = line.find("{")
    if start < 0:
        return None
    try:
        document, _ = json.JSONDecoder().raw_decode(line[start:])
    except ValueError:
        return None
    found = _collect_sanity(document, {})
    return dict(found) if len(found) == len(_SANITY_KEYS) else None


def _collect_sanity(node: object, found: dict[str, int]) -> dict[str, int]:
    """在回调 JSON 的任意层级收集理智数值；嵌套形态由 MAA 版本决定。"""
    if isinstance(node, dict):
        for key, value in node.items():
            if _is_sanity_entry(key, value):
                found[str(key)] = value
            else:
                _collect_sanity(value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_sanity(item, found)
    return found


def _is_sanity_entry(key: object, value: object) -> bool:
    """理智数值键判定；bool 是 int 的子类，需排除。"""
    return (
        isinstance(key, str)
        and key in _SANITY_KEYS
        and isinstance(value, int)
        and not isinstance(value, bool)
    )
