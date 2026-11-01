"""启动清扫：把上次进程遗留的 pending 通知账目落成 failed。

pending 只在进程存活期间存在（docs/03）：正常停机由 NotifyService.close 把
未投递项落成 failed；硬杀与断电走不到收尾，serve 在迁移完成后、第一次 poll
之前清扫这些行，账目历史不留跨进程的中间态。
"""

from __future__ import annotations

import sqlite3
from typing import Final

from contracts import Clock, EventType, Logger
from core.logging import NullLogger
from storage.db.transaction import transaction
from storage.repositories.events import EventLog
from storage.repositories.notifications import NotificationRepository, PendingNotification

SWEEP_ERROR: Final[str] = "启动清扫：上次进程未正常退出"
_SWEEP_EVENT: Final[str] = "notify.pending_swept"


def sweep_pending_notifications(
    connection: sqlite3.Connection,
    clock: Clock,
    *,
    log: Logger | None = None,
) -> int:
    """把遗留 pending 行逐条落成 failed 并各记一条 core.event，返回清扫条数。

    终态与证据事件在同一事务里落盘：事件写不进去时行保持 pending，下次启动重扫。
    单行失败只记错误日志，其余行照常清扫，不阻断启动；读不出 pending 行的
    数据库故障向上抛，按启动失败处置。

    Args:
        connection: 内核单写连接；清扫发生在启动线程。
        clock: 事件时间来源。
        log: 结构化日志；缺省不输出。

    Returns:
        本次清扫的行数。

    Raises:
        sqlite3.Error: 读不出 pending 行等数据库故障；单行失败不在此列。
    """
    logger = NullLogger() if log is None else log
    repository = NotificationRepository(connection)
    events = EventLog(connection)
    swept = 0
    for row in repository.pending_rows():
        try:
            with transaction(connection):
                repository.mark_failed(
                    row.notification_id, attempts=row.attempts, error=SWEEP_ERROR
                )
                events.append_global(EventType.CORE_EVENT, _swept_event(row), at=clock.utc_now())
        except sqlite3.Error as error:
            logger.error(_sweep_failure(row.notification_id, error))
            continue
        swept += 1
    if swept:
        logger.info({"event": "notify.sweep_done", "count": swept})
    return swept


def _swept_event(row: PendingNotification) -> dict[str, object]:
    """清扫证据事件：payload 只带复现账目去向所需的三个字段。"""
    return {
        "event": _SWEEP_EVENT,
        "payload": {
            "notification_id": row.notification_id,
            "channel": row.channel,
            "error": SWEEP_ERROR,
        },
    }


def _sweep_failure(notification_id: str, error: sqlite3.Error) -> dict[str, object]:
    """单行清扫失败的错误日志：带通知 id，供事后判定哪条没扫成。"""
    return {
        "event": "notify.sweep_row_failed",
        "notification_id": notification_id,
        "error": str(error),
    }


__all__ = ["SWEEP_ERROR", "sweep_pending_notifications"]
