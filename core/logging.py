"""结构化日志：字段化输出、不写自由文本，供内核与 worker 使用。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from contracts import Clock
from core.clock import SystemClock


class NullLogger:
    """丢弃全部日志；测试与不关心输出的路径使用。"""

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


class JsonLineLogger:
    """把每条日志写成一行 JSON，追加到指定文件。"""

    def __init__(self, path: Path, *, clock: Clock | None = None) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._clock: Clock = SystemClock() if clock is None else clock

    def debug(self, fields: Mapping[str, Any]) -> None:
        self._write("DEBUG", fields)

    def info(self, fields: Mapping[str, Any]) -> None:
        self._write("INFO", fields)

    def warn(self, fields: Mapping[str, Any]) -> None:
        self._write("WARNING", fields)

    def error(self, fields: Mapping[str, Any]) -> None:
        self._write("ERROR", fields)

    def _write(self, level: str, fields: Mapping[str, Any]) -> None:
        payload: dict[str, Any] = {"at": self._clock.utc_now().isoformat(), "level": level}
        payload.update(fields)
        # 事件名由调用方给；缺失时留标记，保证每行都能按 event 检索。
        payload.setdefault("event", "unspecified")
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")


__all__ = ["JsonLineLogger", "NullLogger"]
