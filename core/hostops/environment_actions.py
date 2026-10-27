"""环境动作实现：在调用线程自开连接跑环境状态机。

serve 的 supervisor 连接归 tick 线程；请求线程复用它会被 sqlite3 的线程检查
拒绝（check_same_thread 默认开启）。因此每个动作按调用打开独立连接，WAL 下
与 tick 的写互不阻塞；实例锁让动作串行，避免同一 provider 被并发调用。
领域异常（环境不存在、状态拒绝、provider 缺失）原样冒泡，由 API 端点按
HTTP 口径映射 404/409。
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Final

from contracts import Clock, EventType, Logger, PhaseResult
from core.environments.actions import EnvironmentActionResult
from core.environments.errors import EnvironmentStateError
from core.environments.service import EnvironmentService
from core.logging import NullLogger
from core.plugins.registry import PluginRegistry
from core.supervisor_environments import EnvironmentSupervision
from storage import open_database
from storage.repositories.events import EventLog

Action = Callable[[EnvironmentSupervision], PhaseResult]

# 动作可能耗掉 provider 的整个超时周期；等锁超时按「环境忙」拒绝，
# 不让第二个请求线程无限期挂在 HTTP 连接上（docs/11 并发：锁一律带超时）。
_LOCK_TIMEOUT_SECONDS: Final[float] = 5.0


class EnvironmentActionService:
    """手动预热与复位：动作在调用线程用自己的连接执行，动作之间串行。"""

    def __init__(
        self,
        database: Path,
        clock: Clock,
        registry: PluginRegistry,
        *,
        log: Logger | None = None,
        lock_timeout_seconds: float = _LOCK_TIMEOUT_SECONDS,
    ) -> None:
        self._database = database
        self._data_root = database.parent
        self._clock = clock
        self._registry = registry
        self._log: Logger = NullLogger() if log is None else log
        self._lock_timeout_seconds = lock_timeout_seconds
        self._lock = threading.Lock()

    def prepare(self, environment_id: str) -> EnvironmentActionResult:
        """手动预热：把环境带向 ready；busy 时拒绝。

        Raises:
            EnvironmentNotFoundError: 环境不存在。
            EnvironmentStateError: 状态机拒绝，如环境 busy。
            PluginError: provider 缺失或未声明 environment 能力。
        """
        return self._serialized(lambda supervision: supervision.prepare(environment_id))

    def reset(self, environment_id: str) -> EnvironmentActionResult:
        """手动复位：任何状态先回 booting，成功后仍等探针确认 ready。

        Raises:
            EnvironmentNotFoundError: 环境不存在。
            EnvironmentStateError: 状态机拒绝。
            PluginError: provider 缺失或未声明 environment 能力。
        """
        return self._serialized(lambda supervision: supervision.reset(environment_id))

    def _serialized(self, action: Action) -> EnvironmentActionResult:
        """在锁内用本次调用的连接执行动作；连接随调用关闭，不留到其它线程。

        Raises:
            EnvironmentStateError: 等锁超时，说明另一个动作正在执行同一批环境；
                按「环境忙」拒绝比让请求线程无限期挂着更符合 docs/03 的 409 口径。
        """
        if not self._lock.acquire(timeout=self._lock_timeout_seconds):
            raise EnvironmentStateError("环境动作正在执行，稍后再试")
        try:
            connection = open_database(self._database)
            try:
                result = action(self._supervision(connection))
            finally:
                connection.close()
        finally:
            self._lock.release()
        return EnvironmentActionResult(
            ok=result.ok,
            error_code=result.error_code,
            detail=result.detail,
        )

    def _supervision(self, connection: sqlite3.Connection) -> EnvironmentSupervision:
        """按本次连接装配环境动作；事件走同一连接，插件上下文取数据库所在目录。"""
        events = EventLog(connection)
        clock = self._clock

        def emit_global(event: str, payload: Mapping[str, Any]) -> None:
            events.append_global(
                EventType.CORE_EVENT,
                {"event": event, "payload": dict(payload)},
                at=clock.utc_now(),
            )

        return EnvironmentSupervision(
            EnvironmentService(connection, clock),
            self._registry,
            clock,
            data_root=self._data_root,
            log=self._log,
            emit_global=emit_global,
        )


__all__ = ["EnvironmentActionService"]
