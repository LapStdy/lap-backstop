"""通知账目仓储：notifications 表的投递记账写入。

行由内核通知服务在投递编排线程写入（入账 pending、落终态 sent/failed）；
查询侧见 core/api/notifications_store.py。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

from contracts import NotificationStatus
from storage.repositories.iso_time import format_at


class NotificationRepository:
    """notifications 表记账：入账一条待投递，并按 id 把终态落成 sent 或 failed。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def insert_pending(self, notification_id: str, run_id: str | None, channel: str) -> None:
        """入账一条待投递通知；尝试次数从 0 起算，终态时刻与错误留空。

        Raises:
            sqlite3.Error: 写入失败。
        """
        self._connection.execute(
            "INSERT INTO notifications (id, run_id, channel, status, attempts, sent_at, error) "
            "VALUES (?, ?, ?, ?, 0, NULL, NULL)",
            (notification_id, run_id, channel, NotificationStatus.PENDING.value),
        )

    def mark_sent(self, notification_id: str, *, attempts: int, at: datetime) -> None:
        """投递成功：写终态 sent、实际尝试次数与完成时刻。

        Raises:
            sqlite3.Error: 写入失败。
        """
        self._connection.execute(
            "UPDATE notifications SET status = ?, attempts = ?, sent_at = ? WHERE id = ?",
            (NotificationStatus.SENT.value, attempts, format_at(at), notification_id),
        )

    def mark_failed(self, notification_id: str, *, attempts: int, error: str) -> None:
        """投递耗尽：写终态 failed、实际尝试次数与最后一次错误；sent_at 保持为空。

        Raises:
            sqlite3.Error: 写入失败。
        """
        self._connection.execute(
            "UPDATE notifications SET status = ?, attempts = ?, error = ? WHERE id = ?",
            (NotificationStatus.FAILED.value, attempts, error, notification_id),
        )


__all__ = ["NotificationRepository"]
