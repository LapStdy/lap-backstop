"""worker 通道的构造参数：超时、取消、重启与降级恢复口径。

默认值唯一真源是 config/defaults.toml 的 [worker] 段（docs/11 配置一节）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from contracts import Logger
from storage.defaults import default


@dataclass(frozen=True, slots=True)
class WorkerLimits:
    """worker 通道的超时、取消、重启与降级恢复参数。"""

    call_timeout_seconds: float
    start_timeout_seconds: float
    cancel_grace_seconds: float
    max_restarts: int
    degraded_retry_seconds: float

    def __post_init__(self) -> None:
        intervals = {
            "call_timeout_seconds": self.call_timeout_seconds,
            "start_timeout_seconds": self.start_timeout_seconds,
            "cancel_grace_seconds": self.cancel_grace_seconds,
            "degraded_retry_seconds": self.degraded_retry_seconds,
        }
        for name, value in intervals.items():
            if value <= 0:
                raise ValueError(f"worker.{name} 必须大于 0")
        if self.max_restarts <= 0:
            raise ValueError("worker.max_restarts 必须大于 0")

    @classmethod
    def from_defaults(cls) -> WorkerLimits:
        """按 config/defaults.toml 的 [worker] 段构造。"""
        return cls(
            call_timeout_seconds=float(default("worker", "call_timeout_seconds")),
            start_timeout_seconds=float(default("worker", "start_timeout_seconds")),
            cancel_grace_seconds=float(default("worker", "cancel_grace_seconds")),
            max_restarts=int(default("worker", "max_restarts")),
            degraded_retry_seconds=float(default("worker", "degraded_retry_seconds")),
        )


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """拉起一个插件 worker 所需的全部参数。"""

    plugins_root: Path
    limits: WorkerLimits = field(default_factory=WorkerLimits.from_defaults)
    log: Logger | None = None
    command: tuple[str, ...] | None = None
