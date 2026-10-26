"""Supervisor 的磁盘水位观察：三级水位沿触发事件，critical 要求本轮中止。

Supervisor 只保留 tick 的推进顺序与调度决策；水位判定、事件名与边沿状态归本模块（docs/08）。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from contracts import Logger
from core.jobs.run_service import DISK_CRITICAL_BYTES, DISK_GUARD_BYTES, DISK_WARN_BYTES

# docs/08 磁盘水位的事件名；字节阈值真源在 run_service。
DISK_LOW_EVENT = "host.disk_low"
DISK_FULL_GUARD_EVENT = "host.disk_full_guard"
DISK_CRITICAL_EVENT = "host.disk_critical"


class DiskWatermark:
    """tick 开头的磁盘水位检查：warn 与 guard 按边沿记事件，critical 触发停机信号。"""

    def __init__(
        self,
        disk_free: Callable[[], int | None],
        log: Logger,
        emit_global: Callable[[str, Mapping[str, Any]], None],
    ) -> None:
        self._disk_free = disk_free
        self._log = log
        self._emit_global = emit_global
        self._disk_warn_reported = False
        self._disk_guard_reported = False
        self._disk_critical_reported = False

    @property
    def is_critical(self) -> bool:
        """最近一次 check 是否观测到磁盘低于 500 MB；serve 循环据此退出。"""
        return self._disk_critical_reported

    def check(self) -> bool:
        """三级水位检查点（docs/08）：warn 与 guard 按边沿各记一次，critical 触发停机。

        三档互斥取当前最深一档；回到上一档重新武装边沿，再次下跌时重新报。

        Returns:
            True 表示低于 500 MB，本轮立即中止。
        """
        free = self._disk_free()
        if free is None:
            return False
        if free < DISK_CRITICAL_BYTES:
            if not self._disk_critical_reported:
                self._disk_critical_reported = True
                self._announce(
                    self._log.error,
                    DISK_CRITICAL_EVENT,
                    {"free_bytes": free, "threshold_bytes": DISK_CRITICAL_BYTES},
                )
            return True
        if free < DISK_GUARD_BYTES:
            if not self._disk_guard_reported:
                self._disk_guard_reported = True
                self._announce(
                    self._log.warn,
                    DISK_FULL_GUARD_EVENT,
                    {
                        "free_bytes": free,
                        "threshold_bytes": DISK_GUARD_BYTES,
                        "error_code": "E_DISK",
                    },
                )
            return False
        self._disk_guard_reported = False
        if free < DISK_WARN_BYTES:
            if not self._disk_warn_reported:
                self._disk_warn_reported = True
                self._announce(
                    self._log.warn,
                    DISK_LOW_EVENT,
                    {"free_bytes": free, "threshold_bytes": DISK_WARN_BYTES},
                )
            return False
        self._disk_warn_reported = False
        return False

    def _announce(
        self,
        notify: Callable[[Mapping[str, Any]], None],
        event: str,
        fields: dict[str, object],
    ) -> None:
        """同一份水位字段进日志与全局事件：日志供排障，事件入库可回放。"""
        notify({"event": event, **fields})
        self._emit_global(event, fields)


__all__ = [
    "DISK_CRITICAL_EVENT",
    "DISK_FULL_GUARD_EVENT",
    "DISK_LOW_EVENT",
    "DiskWatermark",
]
