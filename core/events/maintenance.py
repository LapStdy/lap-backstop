"""全局事件保留维护：serve 循环内按日闸门把 events_global 收敛到保留窗口。

docs/03 保留表定口径：全局事件只保留最近 1000 条。触发与事件写入解耦，
由 serve 每轮询问一次，每天最多执行一遍；最后执行日期记在 settings，
清理、日闸门与结果事件在同一事务里落盘，崩溃重启不重复执行、也不漏。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date

from contracts import Clock, EventType, Logger
from core.api.store import put_settings
from core.logging import NullLogger
from storage.db.transaction import transaction
from storage.repositories.events import EventLog

GLOBAL_EVENTS_PRUNED_ON_KEY = "maintenance.global_events_pruned_on"
GLOBAL_EVENTS_PRUNED_EVENT = "core.global_events_pruned"


@dataclass(frozen=True, slots=True)
class PruneOutcome:
    """一次实际执行的清理结果。"""

    deleted: int
    retained: int


class GlobalEventsPruner:
    """全局事件日闸门清理器：每天第一个到达的 tick 执行，当天其余 tick 跳过。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        *,
        log: Logger | None = None,
    ) -> None:
        """创建清理器；执行与否只看 settings 里的日期键，进程内不记状态。

        Args:
            connection: 内核单写连接。
            clock: 「今天」与事件落库时间的唯一来源。
            log: 结构化日志；不落事件载荷。
        """
        self._connection = connection
        self._clock = clock
        self._log: Logger = NullLogger() if log is None else log
        self._events = EventLog(connection)

    def run_once(self) -> PruneOutcome | None:
        """今天还没清理就清理一次并落结果事件；当天已清理返回 None。

        清理行数、日期键与 core.event 在同一事务里提交：进程在任一点崩溃，
        重启后要么整体已生效（当天不再执行），要么整体回滚（当天补执行）。

        Returns:
            PruneOutcome: 删除与清理后的保留条数；当天已执行过则为 None。

        Raises:
            sqlite3.Error: 数据库不可写；由调用方记日志，不拖垮核心循环。
        """
        today = self._clock.utc_now().date()
        if _pruned_on(self._connection) == today:
            return None
        with transaction(self._connection):
            deleted = self._events.prune_global()
            retained = _count_global(self._connection)
            moment = self._clock.utc_now()
            put_settings(
                self._connection,
                {GLOBAL_EVENTS_PRUNED_ON_KEY: today.isoformat()},
                at=moment,
            )
            self._events.append_global(
                EventType.CORE_EVENT,
                {
                    "event": GLOBAL_EVENTS_PRUNED_EVENT,
                    "detail": {"deleted": deleted, "retained": retained},
                },
                at=moment,
            )
        self._log.info(
            {"event": GLOBAL_EVENTS_PRUNED_EVENT, "deleted": deleted, "retained": retained}
        )
        return PruneOutcome(deleted=deleted, retained=retained)


def _pruned_on(connection: sqlite3.Connection) -> date | None:
    """读最后清理日期键；键缺失、坏 JSON 或非日期字符串都按从未清理处理。"""
    row = connection.execute(
        "SELECT value FROM settings WHERE key = ?",
        (GLOBAL_EVENTS_PRUNED_ON_KEY,),
    ).fetchone()
    if row is None:
        return None
    try:
        value = json.loads(str(row[0]))
    except json.JSONDecodeError:
        return None
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _count_global(connection: sqlite3.Connection) -> int:
    """清理后的剩余条数，进结果事件；一天一次的量级不值得增量簿记。"""
    row = connection.execute("SELECT COUNT(*) FROM events_global").fetchone()
    return int(row[0])


__all__ = [
    "GLOBAL_EVENTS_PRUNED_EVENT",
    "GLOBAL_EVENTS_PRUNED_ON_KEY",
    "GlobalEventsPruner",
    "PruneOutcome",
]
