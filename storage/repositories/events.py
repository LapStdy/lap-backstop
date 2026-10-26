"""事件日志仓储：append-only，运行内 seq 单调；写入者只有内核单写连接。"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from contracts import (
    GLOBAL_EVENTS,
    RUN_SCOPED_EVENTS,
    EventType,
    global_event_id,
    run_event_id,
)
from storage.db.transaction import transaction
from storage.repositories.iso_time import format_at, parse_at
from storage.repositories.json_columns import dump_json, load_json_object

# docs/03 保留表定口径：全局事件只保留最近 1000 条。
GLOBAL_EVENTS_KEEP = 1000


@dataclass(frozen=True, slots=True)
class StoredEvent:
    """一条已落盘的事件。"""

    id: str
    run_id: str | None
    seq: int
    type: str
    at: datetime
    payload: Mapping[str, object]


class EventLog:
    """事件读写；运行事件与全局事件分别落两张表。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def append_run(
        self,
        run_id: str,
        event_type: EventType,
        payload: Mapping[str, object],
        *,
        at: datetime,
    ) -> StoredEvent:
        """追加一条运行事件，返回带 ``r:<run_id>:<seq>`` 的落盘结果。

        Raises:
            ValueError: 事件类型不属于运行作用域。
            sqlite3.Error: 写入失败。
        """
        if event_type not in RUN_SCOPED_EVENTS:
            raise ValueError(f"{event_type.value} 不是运行作用域事件")
        seq = self._next_run_seq(run_id)
        event_id = run_event_id(run_id, seq)
        moment = format_at(at)
        self._connection.execute(
            "INSERT INTO run_events (id, run_id, seq, type, ts, payload) VALUES (?, ?, ?, ?, ?, ?)",
            (event_id, run_id, seq, event_type.value, moment, dump_json(payload)),
        )
        return StoredEvent(
            id=event_id,
            run_id=run_id,
            seq=seq,
            type=event_type.value,
            at=parse_at(moment),
            payload=dict(payload),
        )

    def append_global(
        self,
        event_type: EventType,
        payload: Mapping[str, object],
        *,
        at: datetime,
    ) -> StoredEvent:
        """追加一条全局事件，返回带 ``g:<seq>`` 的落盘结果。

        Raises:
            ValueError: 事件类型不是全局事件。
            sqlite3.Error: 写入失败。
        """
        if event_type not in GLOBAL_EVENTS:
            raise ValueError(f"{event_type.value} 不是全局事件")
        moment = format_at(at)
        cursor = self._connection.execute(
            "INSERT INTO events_global (type, payload, ts) VALUES (?, ?, ?)",
            (event_type.value, dump_json(payload), moment),
        )
        seq = cursor.lastrowid
        if seq is None:
            raise sqlite3.DatabaseError("全局事件写入后没有序号")
        return StoredEvent(
            id=global_event_id(seq),
            run_id=None,
            seq=seq,
            type=event_type.value,
            at=parse_at(moment),
            payload=dict(payload),
        )

    def events_for_run(self, run_id: str) -> tuple[StoredEvent, ...]:
        """按 seq 升序读出某个运行的全部事件。"""
        rows = self._connection.execute(
            "SELECT id, run_id, seq, type, ts, payload FROM run_events "
            "WHERE run_id = ? ORDER BY seq",
            (run_id,),
        ).fetchall()
        return tuple(
            StoredEvent(
                id=str(row["id"]),
                run_id=str(row["run_id"]),
                seq=int(row["seq"]),
                type=str(row["type"]),
                at=parse_at(str(row["ts"])),
                payload=load_json_object(row["payload"], "事件载荷"),
            )
            for row in rows
        )

    def prune_global(self, *, keep: int = GLOBAL_EVENTS_KEEP) -> int:
        """把全局事件收敛到最近 ``keep`` 条，返回删除的条数。

        按序号范围删除最旧的超限行：定位保留窗口起点的 seq 后整段删除，
        不做全表扫描；窗口内行的 seq 原样保留，AUTOINCREMENT 不回退。
        重复调用是幂等的：第二次返回 0。

        Args:
            keep: 保留条数，至少 1。

        Returns:
            实际删除的条数；表不超过保留条数时为 0。

        Raises:
            ValueError: keep 小于 1。
            sqlite3.Error: 删除失败，事务整体回滚。
        """
        if keep < 1:
            raise ValueError(f"keep 必须 ≥ 1，收到 {keep}")
        with transaction(self._connection):
            row = self._connection.execute(
                "SELECT seq FROM events_global ORDER BY seq DESC LIMIT 1 OFFSET ?",
                (keep - 1,),
            ).fetchone()
            if row is None:
                return 0
            cursor = self._connection.execute(
                "DELETE FROM events_global WHERE seq < ?",
                (int(row[0]),),
            )
            return int(cursor.rowcount)

    def _next_run_seq(self, run_id: str) -> int:
        row = self._connection.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM run_events WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise sqlite3.DatabaseError("无法计算运行事件序号")
        return int(row[0])
