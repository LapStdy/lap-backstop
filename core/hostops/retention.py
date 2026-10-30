"""保留清理：serve 日闸门按 docs/03 保留表清理过期运行记录与 advisor 调用。

日闸门的形状照 GlobalEventsPruner：本地时刻到点后的第一个 tick 执行一次，
日期键、删除行与结果事件在同一事务里落盘，崩溃重启既不重复也不漏。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from contracts import TERMINAL_RUN_STATES, Clock, EventType, Logger
from core.api.store import put_settings
from core.logging import NullLogger
from storage.db.transaction import transaction
from storage.defaults import default
from storage.repositories.events import EventLog
from storage.repositories.iso_time import format_at

RETENTION_PRUNED_ON_KEY = "maintenance.retention_pruned_on"
RETENTION_PRUNED_EVENT = "core.retention_pruned"
_GIB = 1024**3
_TERMINAL_STATES = tuple(state.value for state in TERMINAL_RUN_STATES)
_TERMINAL_MARKS = ",".join("?" for _ in _TERMINAL_STATES)


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """保留期限；默认值来自 config/defaults.toml，测试可整体替换。"""

    daily_at: time
    evidence_days: int
    evidence_total_bytes: int
    run_records_days: int
    verdicts_days: int
    logs_days: int
    logs_archive_days: int
    advisor_days: int

    @classmethod
    def from_defaults(cls) -> RetentionPolicy:
        """按 config/defaults.toml 的 [retention] 段组装策略。"""
        return cls(
            daily_at=time.fromisoformat(str(default("retention", "daily_at"))),
            evidence_days=int(default("retention", "evidence_days")),
            evidence_total_bytes=int(default("retention", "evidence_total_gb")) * _GIB,
            run_records_days=int(default("retention", "run_records_days")),
            verdicts_days=int(default("retention", "verdicts_days")),
            logs_days=int(default("retention", "logs_days")),
            logs_archive_days=int(default("retention", "logs_archive_days")),
            advisor_days=int(default("retention", "advisor_days")),
        )


@dataclass(frozen=True, slots=True)
class RetentionOutcome:
    """一次实际执行的清理结果；detail 的键名是事件契约，不随实现调整。"""

    date: str
    evidence_files_deleted: int
    evidence_bytes_freed: int
    evidence_bytes_after: int
    run_events_deleted: int
    run_phases_deleted: int
    verdicts_deleted: int
    runs_deleted: int
    advisor_deleted: int
    log_files_deleted: int

    def detail(self) -> dict[str, object]:
        """事件载荷里的 detail；字段顺序与人读顺序一致。"""
        return {
            "date": self.date,
            "evidence_files_deleted": self.evidence_files_deleted,
            "evidence_bytes_freed": self.evidence_bytes_freed,
            "evidence_bytes_after": self.evidence_bytes_after,
            "run_events_deleted": self.run_events_deleted,
            "run_phases_deleted": self.run_phases_deleted,
            "verdicts_deleted": self.verdicts_deleted,
            "runs_deleted": self.runs_deleted,
            "advisor_deleted": self.advisor_deleted,
            "log_files_deleted": self.log_files_deleted,
        }


@dataclass(frozen=True, slots=True)
class _RowCounts:
    """一次事务里各表的删除行数。"""

    run_events: int
    run_phases: int
    verdicts: int
    runs: int
    advisor: int


class RetentionRunner:
    """保留清理日闸门：每天到点后的第一个 tick 执行，当天其余 tick 跳过。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        *,
        data_root: Path,
        log: Logger | None = None,
        policy: RetentionPolicy | None = None,
    ) -> None:
        """创建清理器；执行与否只看 settings 里的本地日期键，进程内不记状态。

        构造参数超过上限 4 属评审已说明的例外：连接、时钟、数据根是清理对象，
        log 与 policy 是可选注入项，测试按需替换，语义互不重叠。

        Args:
            connection: 内核单写连接。
            clock: 「现在」与事件落库时间的唯一来源。
            data_root: 数据根；证据与日志分别在其下的 evidence 与 logs。
            log: 结构化日志；记录被跳过的文件，不落事件载荷。
            policy: 保留期限；缺省按 config/defaults.toml 组装。
        """
        self._connection = connection
        self._clock = clock
        self._data_root = data_root
        self._log: Logger = NullLogger() if log is None else log
        self._policy = RetentionPolicy.from_defaults() if policy is None else policy
        self._zone = ZoneInfo(str(default("scheduler", "timezone")))
        self._events = EventLog(connection)

    def run_once(self) -> RetentionOutcome | None:
        """到点且今天还没清理就清理一次；未到点或当天已清理返回 None。

        数据库删除、日期键与结果事件同事务提交：进程在事务内崩溃时整体回滚，
        下个 tick 重试。

        Returns:
            RetentionOutcome: 本次删除统计；未到点或当天已执行过则为 None。

        Raises:
            sqlite3.Error: 数据库不可写；由调用方记日志，不拖垮核心循环。
        """
        today = self._due_date()
        if today is None:
            return None
        with transaction(self._connection):
            counts = self._prune_rows()
            outcome = self._outcome(today, counts)
            moment = self._clock.utc_now()
            put_settings(
                self._connection,
                {RETENTION_PRUNED_ON_KEY: today.isoformat()},
                at=moment,
            )
            self._events.append_global(
                EventType.CORE_EVENT,
                {"event": RETENTION_PRUNED_EVENT, "detail": outcome.detail()},
                at=moment,
            )
        self._log.info({"event": RETENTION_PRUNED_EVENT, **outcome.detail()})
        return outcome

    def _due_date(self) -> date | None:
        """本地时刻已到 daily_at 且当天未清理时返回本地日期，否则 None。"""
        local = self._clock.utc_now().astimezone(self._zone)
        if local.time() < self._policy.daily_at:
            return None
        today = local.date()
        return None if _pruned_on(self._connection) == today else today

    def _outcome(self, today: date, counts: _RowCounts) -> RetentionOutcome:
        """把行删除统计合成结果；detail 字段在这里一次定型。"""
        return RetentionOutcome(
            date=today.isoformat(),
            evidence_files_deleted=0,
            evidence_bytes_freed=0,
            evidence_bytes_after=0,
            run_events_deleted=counts.run_events,
            run_phases_deleted=counts.run_phases,
            verdicts_deleted=counts.verdicts,
            runs_deleted=counts.runs,
            advisor_deleted=counts.advisor,
            log_files_deleted=0,
        )

    def _prune_rows(self) -> _RowCounts:
        """按 30 天删运行事件与阶段、按 90 天整棵删终态运行、按 90 天删 advisor。"""
        records_cutoff = format_at(
            self._clock.utc_now() - timedelta(days=self._policy.run_records_days)
        )
        events = _delete_by_run_query(self._connection, "run_events", records_cutoff)
        phases = _delete_by_run_query(self._connection, "run_phases", records_cutoff)
        runs_cutoff = format_at(self._clock.utc_now() - timedelta(days=self._policy.verdicts_days))
        late = self._delete_old_runs(runs_cutoff)
        advisor = late.advisor + _delete_old_advisor(
            self._connection, self._policy.advisor_days, self._clock
        )
        return _RowCounts(
            run_events=events + late.run_events,
            run_phases=phases + late.run_phases,
            verdicts=late.verdicts,
            runs=late.runs,
            advisor=advisor,
        )

    def _delete_old_runs(self, cutoff: str) -> _RowCounts:
        """按外键顺序整棵删掉超期终态运行：先断 job_runs.verdict_id，再清引用它的子表。"""
        ids = _terminal_run_ids(self._connection, cutoff)
        if not ids:
            return _RowCounts(run_events=0, run_phases=0, verdicts=0, runs=0, advisor=0)
        marks = ",".join("?" for _ in ids)
        # job_runs.verdict_id 反向引用 verdicts：不先置空，删 verdicts 会撞外键。
        self._connection.execute(
            f"UPDATE job_runs SET verdict_id = NULL WHERE id IN ({marks})", ids
        )
        _delete_rows(self._connection, "evidence", "run_id", ids)
        verdicts = _delete_rows(self._connection, "verdicts", "run_id", ids)
        events = _delete_rows(self._connection, "run_events", "run_id", ids)
        phases = _delete_rows(self._connection, "run_phases", "run_id", ids)
        _delete_rows(self._connection, "request_idempotency", "run_id", ids)
        _delete_rows(self._connection, "notifications", "run_id", ids)
        _delete_rows(self._connection, "resource_leases", "run_id", ids)
        # advisor_calls 也引用 job_runs：不按 run id 清掉，删 job_runs 会撞外键。
        advisor = _delete_rows(self._connection, "advisor_calls", "run_id", ids)
        runs = _delete_rows(self._connection, "job_runs", "id", ids)
        return _RowCounts(
            run_events=events,
            run_phases=phases,
            verdicts=verdicts,
            runs=runs,
            advisor=advisor,
        )


def _terminal_run_ids(connection: sqlite3.Connection, cutoff: str) -> tuple[str, ...]:
    """早于 cutoff 的终态运行 id。"""
    rows = connection.execute(
        f"SELECT id FROM job_runs WHERE state IN ({_TERMINAL_MARKS}) AND created_at < ?",
        (*_TERMINAL_STATES, cutoff),
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _delete_by_run_query(connection: sqlite3.Connection, table: str, cutoff: str) -> int:
    """删早于 cutoff 的终态运行在 table 里的行，返回删除行数。"""
    cursor = connection.execute(
        f"DELETE FROM {table} WHERE run_id IN "
        f"(SELECT id FROM job_runs WHERE state IN ({_TERMINAL_MARKS}) AND created_at < ?)",
        (*_TERMINAL_STATES, cutoff),
    )
    return int(cursor.rowcount)


def _delete_rows(
    connection: sqlite3.Connection, table: str, column: str, ids: Iterable[str]
) -> int:
    """按列值集合删行，返回删除行数；表名与列名只来自本模块常量。"""
    values = tuple(ids)
    marks = ",".join("?" for _ in values)
    cursor = connection.execute(f"DELETE FROM {table} WHERE {column} IN ({marks})", values)
    return int(cursor.rowcount)


def _delete_old_advisor(connection: sqlite3.Connection, days: int, clock: Clock) -> int:
    """删早于保留期限的 advisor_calls 行。"""
    cutoff = format_at(clock.utc_now() - timedelta(days=days))
    cursor = connection.execute("DELETE FROM advisor_calls WHERE created_at < ?", (cutoff,))
    return int(cursor.rowcount)


def _pruned_on(connection: sqlite3.Connection) -> date | None:
    """读最后清理的本地日期；键缺失、坏 JSON 或非日期字符串都按从未清理处理。"""
    row = connection.execute(
        "SELECT value FROM settings WHERE key = ?",
        (RETENTION_PRUNED_ON_KEY,),
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


__all__ = [
    "RETENTION_PRUNED_EVENT",
    "RETENTION_PRUNED_ON_KEY",
    "RetentionOutcome",
    "RetentionPolicy",
    "RetentionRunner",
]
