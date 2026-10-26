"""通知记录数据访问：只读列表。

行由通知服务（core/notify/service.py）投递记账写入；本层只读并映射成 JSON 结构。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TypedDict

from storage.repositories.sql import optional_str


@dataclass(frozen=True, slots=True)
class NotificationFilters:
    """GET /notifications 的过滤条件；run_id 与 status 精确匹配，limit 已收敛到合法区间。"""

    run_id: str | None = None
    status: str | None = None
    limit: int = 100


class NotificationJson(TypedDict):
    """notifications 一行的应答形态（docs/03 字段表）。"""

    id: str
    run_id: str | None
    channel: str
    status: str
    attempts: int
    sent_at: str | None
    error: str | None


_NOTIFICATION_QUERY = (
    "SELECT id, run_id, channel, status, attempts, sent_at, error FROM notifications"
)


def notification_rows(
    connection: sqlite3.Connection, filters: NotificationFilters
) -> list[NotificationJson]:
    """按过滤条件取通知记录，sent_at 新的在前。

    无 sent_at 的行（pending、failed）没有可排序的完成时刻，排在已发送之后；
    同一刻由 id 倒序兜底，保证顺序稳定（docs/03 端点表）。
    """
    clauses: list[str] = []
    params: list[object] = []
    if filters.run_id is not None:
        clauses.append("run_id = ?")
        params.append(filters.run_id)
    if filters.status is not None:
        clauses.append("status = ?")
        params.append(filters.status)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = connection.execute(
        f"{_NOTIFICATION_QUERY}{where} ORDER BY sent_at DESC, id DESC LIMIT ?",
        (*params, filters.limit),
    ).fetchall()
    return [_notification_json(row) for row in rows]


def _notification_json(row: sqlite3.Row) -> NotificationJson:
    return {
        "id": str(row["id"]),
        "run_id": optional_str(row["run_id"]),
        "channel": str(row["channel"]),
        "status": str(row["status"]),
        "attempts": int(row["attempts"]),
        "sent_at": optional_str(row["sent_at"]),
        "error": optional_str(row["error"]),
    }


__all__ = ["NotificationFilters", "NotificationJson", "notification_rows"]
