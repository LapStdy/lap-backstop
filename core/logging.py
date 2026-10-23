"""结构化日志：字段化输出、不写自由文本，供内核与 worker 使用。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any


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

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path

    def debug(self, fields: Mapping[str, Any]) -> None:
        self._write("DEBUG", fields)

    def info(self, fields: Mapping[str, Any]) -> None:
        self._write("INFO", fields)

    def warn(self, fields: Mapping[str, Any]) -> None:
        self._write("WARNING", fields)

    def error(self, fields: Mapping[str, Any]) -> None:
        self._write("ERROR", fields)

    def _write(self, level: str, fields: Mapping[str, Any]) -> None:
        line = json.dumps(
            {"level": level, **dict(fields)},
            ensure_ascii=False,
            sort_keys=True,
        )
        with self._path.open("a", encoding="utf-8") as stream:
            stream.write(line + "\n")


__all__ = ["JsonLineLogger", "NullLogger"]
