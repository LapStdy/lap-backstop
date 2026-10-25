"""JobRun 仓储：幂等创建、CAS 状态迁移、优先队列与截止时间查询。"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from uuid import uuid4

from contracts import TERMINAL_RUN_STATES, RunState
from storage.repositories.iso_time import format_at, parse_at, parse_optional_at
from storage.repositories.json_columns import dump_json, load_json_object
from storage.repositories.sql import optional_str, placeholders

_COLUMNS = (
    "id, template_id, schedule_id, scheduled_at, created_at, idempotency_key, "
    "state, attempt, priority, deadline_at, started_at, finished_at, verdict_id, "
    "error_code, detail"
)

_OVERDUE_STATES: tuple[RunState, ...] = (
    RunState.SCHEDULED,
    RunState.QUEUED,
    RunState.PREPARING,
    RunState.WAITING,
    RunState.RECOVERING,
)

_NON_TERMINAL_STATES: tuple[RunState, ...] = tuple(
    state for state in RunState if state not in TERMINAL_RUN_STATES
)


@dataclass(frozen=True, slots=True)
class JobRunRecord:
    """job_runs 的一行；时间字段一律带时区的 UTC。"""

    id: str
    template_id: str
    schedule_id: str | None
    scheduled_at: datetime | None
    created_at: datetime
    idempotency_key: str
    state: RunState
    attempt: int
    priority: int
    deadline_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    verdict_id: str | None
    error_code: str | None
    detail: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class NewRun:
    """创建一次运行的输入；``idempotency_key`` 相同则命中既有 run。"""

    template_id: str
    schedule_id: str | None
    scheduled_at: datetime | None
    created_at: datetime
    idempotency_key: str
    priority: int
    deadline_at: datetime | None
    detail: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StateChange:
    """一次 CAS 状态迁移；``source`` 与库内当前状态不一致时不生效。"""

    run_id: str
    source: RunState
    target: RunState
    at: datetime
    error_code: str | None = None


class JobRunRepository:
    """job_runs 表的读写；调用方用事务包住「迁移 + 事件」两件事。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get(self, run_id: str) -> JobRunRecord | None:
        """按 id 取运行；不存在返回 None。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM job_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    def get_by_idempotency_key(self, key: str) -> JobRunRecord | None:
        """按幂等键取运行；不存在返回 None。"""
        row = self._connection.execute(
            f"SELECT {_COLUMNS} FROM job_runs WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        return None if row is None else _row_to_record(row)

    def create_or_get(self, run: NewRun) -> tuple[JobRunRecord, bool]:
        """幂等创建运行。

        Returns:
            (记录, 是否本次新建)；命中唯一键时返回既有记录与 False。
        """
        run_id = uuid4().hex
        cursor = self._connection.execute(
            "INSERT INTO job_runs (id, template_id, schedule_id, scheduled_at, created_at, "
            "idempotency_key, state, attempt, priority, deadline_at, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?) "
            "ON CONFLICT (idempotency_key) DO NOTHING",
            (
                run_id,
                run.template_id,
                run.schedule_id,
                None if run.scheduled_at is None else format_at(run.scheduled_at),
                format_at(run.created_at),
                run.idempotency_key,
                RunState.SCHEDULED.value,
                run.priority,
                None if run.deadline_at is None else format_at(run.deadline_at),
                dump_json(run.detail),
            ),
        )
        created = cursor.rowcount == 1
        record = self.get_by_idempotency_key(run.idempotency_key)
        if record is None:
            raise sqlite3.DatabaseError("创建运行后读不到记录")
        return record, created

    def transition(self, change: StateChange) -> bool:
        """按 ``source`` 做 CAS 更新；状态已被别人改动时返回 False。"""
        finished = change.target in TERMINAL_RUN_STATES
        cursor = self._connection.execute(
            "UPDATE job_runs SET "
            "state = ?, "
            "started_at = CASE WHEN ? = ? AND started_at IS NULL THEN ? ELSE started_at END, "
            "finished_at = CASE WHEN ? THEN ? ELSE finished_at END, "
            "error_code = COALESCE(?, error_code) "
            "WHERE id = ? AND state = ?",
            (
                change.target.value,
                change.target.value,
                RunState.EXECUTING.value,
                format_at(change.at),
                1 if finished else 0,
                format_at(change.at),
                change.error_code,
                change.run_id,
                change.source.value,
            ),
        )
        return cursor.rowcount == 1

    def queue(self, limit: int = 1) -> tuple[JobRunRecord, ...]:
        """按 priority 降序、scheduled_at 升序、created_at 升序取出排队运行。"""
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM job_runs WHERE state = ? "
            "ORDER BY priority DESC, scheduled_at ASC, created_at ASC LIMIT ?",
            (RunState.QUEUED.value, limit),
        ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def overdue(self, now: datetime) -> tuple[JobRunRecord, ...]:
        """尚未进入 executing 且已过截止时间的运行。"""
        marks = placeholders(_OVERDUE_STATES)
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM job_runs "
            f"WHERE state IN ({marks}) AND deadline_at IS NOT NULL AND deadline_at <= ? "
            "ORDER BY deadline_at",
            (*(state.value for state in _OVERDUE_STATES), format_at(now)),
        ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def non_terminal(self) -> tuple[JobRunRecord, ...]:
        """所有非终态运行；启动恢复时按最后事件重建它们。"""
        marks = placeholders(_NON_TERMINAL_STATES)
        rows = self._connection.execute(
            f"SELECT {_COLUMNS} FROM job_runs WHERE state IN ({marks}) ORDER BY created_at",
            tuple(state.value for state in _NON_TERMINAL_STATES),
        ).fetchall()
        return tuple(_row_to_record(row) for row in rows)

    def set_verdict(self, run_id: str, verdict_id: str) -> None:
        """把结论 id 回填到运行。"""
        self._connection.execute(
            "UPDATE job_runs SET verdict_id = ? WHERE id = ?",
            (verdict_id, run_id),
        )

    def set_attempt(self, run_id: str, attempt: int) -> None:
        """写入下一次尝试序号。"""
        self._connection.execute(
            "UPDATE job_runs SET attempt = ? WHERE id = ?",
            (attempt, run_id),
        )

    def set_detail(self, run_id: str, detail: Mapping[str, object]) -> None:
        """覆盖运行摘要，供恢复等待等调度提示使用。"""
        self._connection.execute(
            "UPDATE job_runs SET detail = ? WHERE id = ?",
            (dump_json(detail), run_id),
        )


def _row_to_record(row: sqlite3.Row) -> JobRunRecord:
    return JobRunRecord(
        id=str(row["id"]),
        template_id=str(row["template_id"]),
        schedule_id=optional_str(row["schedule_id"]),
        scheduled_at=parse_optional_at(optional_str(row["scheduled_at"])),
        created_at=parse_at(str(row["created_at"])),
        idempotency_key=str(row["idempotency_key"]),
        state=RunState(str(row["state"])),
        attempt=int(row["attempt"]),
        priority=int(row["priority"]),
        deadline_at=parse_optional_at(optional_str(row["deadline_at"])),
        started_at=parse_optional_at(optional_str(row["started_at"])),
        finished_at=parse_optional_at(optional_str(row["finished_at"])),
        verdict_id=optional_str(row["verdict_id"]),
        error_code=optional_str(row["error_code"]),
        detail=load_json_object(row["detail"], "detail"),
    )
