"""租约仓储：活跃租约查询、续租、释放与两张过期表。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from contracts import LeaseState
from storage.repositories.iso_time import format_at, parse_at, parse_optional_at

_COLUMNS = "id, resource_id, run_id, owner, state, acquired_at, expires_at, renewed_at"
_ACTIVE_STATES = (LeaseState.LEASED.value, LeaseState.EXPIRING.value)


@dataclass(frozen=True, slots=True)
class LeaseRow:
    """resource_leases 的一行。"""

    id: str
    resource_id: str
    run_id: str
    owner: str
    state: LeaseState
    acquired_at: datetime
    expires_at: datetime
    renewed_at: datetime | None


@dataclass(frozen=True, slots=True)
class NewLease:
    """新建租约的输入。"""

    resource_id: str
    run_id: str
    owner: str
    acquired_at: datetime
    expires_at: datetime


class LeaseRepository:
    """resource_leases 表的读写；容量判断由 core/resources 负责。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get(self, lease_id: str) -> LeaseRow | None:
        """按 id 取租约。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM resource_leases WHERE id = ?",
            (lease_id,),
        ).fetchone()
        return None if row is None else _row_to_row(row)

    def active_for(self, resource_id: str) -> tuple[LeaseRow, ...]:
        """某个资源当前持有的租约。"""
        placeholders = ", ".join("?" for _ in _ACTIVE_STATES)
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM resource_leases "
            f"WHERE resource_id = ? AND state IN ({placeholders}) "
            "ORDER BY acquired_at",
            (resource_id, *_ACTIVE_STATES),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def active_for_run(self, run_id: str) -> tuple[LeaseRow, ...]:
        """某个运行当前持有的租约，按资源 id 排序。"""
        placeholders = ", ".join("?" for _ in _ACTIVE_STATES)
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM resource_leases "
            f"WHERE run_id = ? AND state IN ({placeholders}) "
            "ORDER BY resource_id",
            (run_id, *_ACTIVE_STATES),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def active(self) -> tuple[LeaseRow, ...]:
        """全部活跃租约，按过期时间排序；启动清理与续租扫描用。"""
        placeholders = ", ".join("?" for _ in _ACTIVE_STATES)
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM resource_leases "
            f"WHERE state IN ({placeholders}) ORDER BY expires_at",
            (*_ACTIVE_STATES,),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def insert(self, lease: NewLease) -> LeaseRow:
        """写入一条 leased 租约。"""
        lease_id = uuid4().hex
        self._connection.execute(
            "INSERT INTO resource_leases "
            "(id, resource_id, run_id, owner, state, acquired_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                lease_id,
                lease.resource_id,
                lease.run_id,
                lease.owner,
                LeaseState.LEASED.value,
                format_at(lease.acquired_at),
                format_at(lease.expires_at),
            ),
        )
        record = self.get(lease_id)
        if record is None:
            raise sqlite3.DatabaseError("写入租约后读不到记录")
        return record

    def renew(self, lease_id: str, *, expires_at: datetime, at: datetime) -> bool:
        """续租；租约已不在活跃状态时返回 False。"""
        placeholders = ", ".join("?" for _ in _ACTIVE_STATES)
        cursor = self._connection.execute(
            f"UPDATE resource_leases SET expires_at = ?, renewed_at = ? "
            f"WHERE id = ? AND state IN ({placeholders})",
            (format_at(expires_at), format_at(at), lease_id, *_ACTIVE_STATES),
        )
        return cursor.rowcount == 1

    def release(self, lease_id: str) -> bool:
        """释放租约；已释放时返回 False。"""
        placeholders = ", ".join("?" for _ in _ACTIVE_STATES)
        cursor = self._connection.execute(
            f"UPDATE resource_leases SET state = ? WHERE id = ? AND state IN ({placeholders})",
            (LeaseState.RELEASED.value, lease_id, *_ACTIVE_STATES),
        )
        return cursor.rowcount == 1

    def begin_expiry(self, now: datetime) -> tuple[LeaseRow, ...]:
        """把过期租约从 leased 推到 expiring，返回这些租约。"""
        self._connection.execute(
            "UPDATE resource_leases SET state = ? WHERE state = ? AND expires_at <= ?",
            (LeaseState.EXPIRING.value, LeaseState.LEASED.value, format_at(now)),
        )
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM resource_leases "
            "WHERE state = ? AND expires_at <= ? ORDER BY expires_at",
            (LeaseState.EXPIRING.value, format_at(now)),
        ).fetchall()
        return tuple(_row_to_row(row) for row in rows)

    def finish_expiry(self, now: datetime) -> tuple[LeaseRow, ...]:
        """把 expiring 租约正式释放，返回释放后的记录。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM resource_leases "
            "WHERE state = ? AND expires_at <= ? ORDER BY expires_at",
            (LeaseState.EXPIRING.value, format_at(now)),
        ).fetchall()
        leases = tuple(_row_to_row(row) for row in rows)
        self._connection.execute(
            "UPDATE resource_leases SET state = ? WHERE state = ? AND expires_at <= ?",
            (LeaseState.RELEASED.value, LeaseState.EXPIRING.value, format_at(now)),
        )
        return leases


def _row_to_row(row: sqlite3.Row) -> LeaseRow:
    return LeaseRow(
        id=str(row["id"]),
        resource_id=str(row["resource_id"]),
        run_id=str(row["run_id"]),
        owner=str(row["owner"]),
        state=LeaseState(str(row["state"])),
        acquired_at=parse_at(str(row["acquired_at"])),
        expires_at=parse_at(str(row["expires_at"])),
        renewed_at=parse_optional_at(_optional_str(row["renewed_at"])),
    )


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)
