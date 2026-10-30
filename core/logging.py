"""结构化日志：字段化输出、不写自由文本，供内核与 worker 使用。"""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import tempfile
import threading
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import Clock
from core.clock import SystemClock

MAX_FILE_BYTES = 200 * 1024 * 1024
COMPRESS_AFTER_DAYS = 14

_DAILY_NAME = re.compile(r"^core-(?P<day>\d{4}-\d{2}-\d{2})(?:\.(?P<index>\d+))?\.jsonl$")


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
        line = _format_line(self._clock.utc_now(), level, fields)
        with self._path.open("a", encoding="utf-8") as stream:
            stream.write(line)


class DailyRotatingJsonLineLogger:
    """按 UTC 日界切分日志文件：满额续号、按保留窗口压缩旧文件。

    启动与跨天时扫描目录，重启接当天最后一个未满文件；日期回退同样只追加，
    不截断已有文件。
    """

    def __init__(
        self,
        logs_dir: Path,
        *,
        clock: Clock | None = None,
        max_file_bytes: int = MAX_FILE_BYTES,
        keep_days: int = COMPRESS_AFTER_DAYS,
    ) -> None:
        self._logs_dir = logs_dir
        self._clock: Clock = SystemClock() if clock is None else clock
        self._max_file_bytes = max_file_bytes
        self._keep_days = keep_days
        self._lock = threading.Lock()
        self._day: date
        self._path: Path
        self._bytes: int
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._open_day(self._today())

    def debug(self, fields: Mapping[str, Any]) -> None:
        self._write("DEBUG", fields)

    def info(self, fields: Mapping[str, Any]) -> None:
        self._write("INFO", fields)

    def warn(self, fields: Mapping[str, Any]) -> None:
        self._write("WARNING", fields)

    def error(self, fields: Mapping[str, Any]) -> None:
        self._write("ERROR", fields)

    def _write(self, level: str, fields: Mapping[str, Any]) -> None:
        with self._lock:
            now = self._clock.utc_now().astimezone(UTC)
            if now.date() != self._day:
                self._open_day(now.date())
            self._append_locked(level, fields, now)

    def _today(self) -> date:
        return self._clock.utc_now().astimezone(UTC).date()

    def _open_day(self, day: date) -> None:
        self._path, self._bytes = self._resolve_file(day)
        self._day = day
        self._compress_before(day)

    def _resolve_file(self, day: date) -> tuple[Path, int]:
        candidates = [
            (index, path) for path, file_day, index in self._existing_files() if file_day == day
        ]
        if not candidates:
            return self._file_path(day, 1), 0
        index, path = max(candidates)
        size = path.stat().st_size
        if size < self._max_file_bytes:
            return path, size
        return self._file_path(day, index + 1), 0

    def _file_path(self, day: date, index: int) -> Path:
        name = f"core-{day.isoformat()}.jsonl"
        if index > 1:
            name = f"core-{day.isoformat()}.{index}.jsonl"
        return self._logs_dir / name

    def _existing_files(self) -> list[tuple[Path, date, int]]:
        files: list[tuple[Path, date, int]] = []
        for path in self._logs_dir.iterdir():
            parsed = _parse_daily_name(path.name)
            if parsed is None or not path.is_file():
                continue
            files.append((path, parsed[0], parsed[1]))
        return files

    def _append_locked(self, level: str, fields: Mapping[str, Any], now: datetime) -> None:
        # 写满后从下一个序号继续：整行先落盘，换号只发生在下一行写入之前。
        if self._bytes >= self._max_file_bytes:
            self._path, self._bytes = self._resolve_file(self._day)
        line = _format_line(now, level, fields).encode("utf-8")
        with self._path.open("ab") as stream:
            stream.write(line)
        self._bytes += len(line)

    def _compress_before(self, day: date) -> None:
        cutoff = day - timedelta(days=max(self._keep_days - 1, 0))
        for path, file_day, _index in self._existing_files():
            if file_day >= cutoff or path == self._path:
                continue
            target = path.with_name(f"{path.name}.gz")
            if not path.is_file():
                # 另一实例可能已把源文件归档并删除：归档已完成，跳过不算失败。
                continue
            if target.exists():
                # 已有压缩件就跳过：重复压缩只会浪费启动时间。
                continue
            try:
                _gzip_replace(path, target)
            except OSError as error:
                self._report_compress_failure(path, error)

    def _report_compress_failure(self, path: Path, error: OSError) -> None:
        self._append_locked(
            "ERROR",
            {"event": "log.compress_failed", "file": path.name, "reason": str(error)},
            self._clock.utc_now().astimezone(UTC),
        )


def _format_line(now: datetime, level: str, fields: Mapping[str, Any]) -> str:
    payload: dict[str, Any] = {"at": now.isoformat(), "level": level}
    payload.update(fields)
    # 事件名由调用方给；缺失时留标记，保证每行都能按 event 检索。
    payload.setdefault("event", "unspecified")
    return json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"


def _parse_daily_name(name: str) -> tuple[date, int] | None:
    match = _DAILY_NAME.fullmatch(name)
    if match is None:
        return None
    try:
        day = date.fromisoformat(match.group("day"))
    except ValueError:
        return None
    index = match.group("index")
    return day, 1 if index is None else int(index)


def _gzip_replace(source: Path, target: Path) -> None:
    # 先写临时文件再原子替换：半截压缩件不会被当成已完成。
    # 临时名按调用唯一：重复启动的实例会同时压缩同一个旧文件，固定名会互相截断。
    handle, temporary_name = tempfile.mkstemp(
        dir=target.parent, prefix=f"{target.name}.", suffix=".part"
    )
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        with source.open("rb") as reader, gzip.open(temporary, "wb") as writer:
            shutil.copyfileobj(reader, writer)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    # 另一实例可能已删除源文件：归档已完成，缺源不算失败。
    source.unlink(missing_ok=True)


__all__ = [
    "COMPRESS_AFTER_DAYS",
    "MAX_FILE_BYTES",
    "DailyRotatingJsonLineLogger",
    "JsonLineLogger",
    "NullLogger",
]
