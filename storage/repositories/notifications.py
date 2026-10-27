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
    """notifications 表记账：入账一条待投递，再把 pending 落成 sent 或 failed；终态只落一次。"""

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
        """投递成功：把 pending 落成 sent；账目只能从 pending 落一次终态。

        Raises:
            sqlite3.DatabaseError: 账目不存在或已有终态，说明投递结果与账目对不上。
            sqlite3.Error: 写入失败。
        """
        self._finalize(
            "UPDATE notifications SET status = ?, attempts = ?, sent_at = ? "
            "WHERE id = ? AND status = ?",
            (
                NotificationStatus.SENT.value,
                attempts,
                format_at(at),
                notification_id,
                NotificationStatus.PENDING.value,
            ),
            notification_id,
        )

    def mark_failed(self, notification_id: str, *, attempts: int, error: str) -> None:
        """投递耗尽：把 pending 落成 failed 并写最后一条错误；sent_at 保持为空。

        Raises:
            sqlite3.DatabaseError: 账目不存在或已有终态，说明投递结果与账目对不上。
            sqlite3.Error: 写入失败。
        """
        self._finalize(
            "UPDATE notifications SET status = ?, attempts = ?, error = ? "
            "WHERE id = ? AND status = ?",
            (
                NotificationStatus.FAILED.value,
                attempts,
                error,
                notification_id,
                NotificationStatus.PENDING.value,
            ),
            notification_id,
        )

    def _finalize(self, sql: str, parameters: tuple[object, ...], notification_id: str) -> None:
        """落终态并校验恰好命中一行；0 行意味着账目缺失或已被改写，不能静默吞掉。"""
        cursor = self._connection.execute(sql, parameters)
        if cursor.rowcount == 1:
            return
        row = self._connection.execute(
            "SELECT status FROM notifications WHERE id = ?", (notification_id,)
        ).fetchone()
        if row is None:
            raise sqlite3.DatabaseError(f"通知账目不存在：{notification_id}")
        raise sqlite3.DatabaseError(
            f"通知账目已是 {row['status']}，不能再落终态：{notification_id}"
        )


__all__ = ["NotificationRepository"]
