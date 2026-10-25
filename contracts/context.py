"""RunContext 与插件可见的运行时接口。

唯一真源是 docs/05 的 RunContext 一节；内核构造、插件只读。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """插件唯一的时间来源；测试时替换成虚拟时钟。"""

    def monotonic(self) -> float:
        """单调递增秒数，只用于计时；不受系统时间回拨影响。"""
        ...

    def utc_now(self) -> datetime:
        """带时区的当前 UTC 时刻。"""
        ...


@runtime_checkable
class CancelToken(Protocol):
    """由内核持有；插件只能查询。"""

    def is_cancelled(self) -> bool:
        """本次运行是否已被请求取消。"""
        ...


@runtime_checkable
class Logger(Protocol):
    """结构化日志：字段化，不写自由文本，不落密钥。"""

    def debug(self, fields: Mapping[str, Any]) -> None: ...

    def info(self, fields: Mapping[str, Any]) -> None: ...

    def warn(self, fields: Mapping[str, Any]) -> None: ...

    def error(self, fields: Mapping[str, Any]) -> None: ...


EmitEvent = Callable[[str, Mapping[str, Any]], None]
RecordEvidence = Callable[[str, Path, str], str]


@dataclass(frozen=True, slots=True)
class RunContext:
    run_id: str
    attempt: int
    template_id: str
    plugin_id: str
    environment_id: str
    config: Mapping[str, Any]
    work_dir: Path
    evidence_dir: Path
    deadline_at: datetime
    cancel: CancelToken
    clock: Clock
    secrets: Mapping[str, str]
    log: Logger
    emit: EmitEvent
    record_evidence: RecordEvidence
