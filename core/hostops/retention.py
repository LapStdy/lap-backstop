"""保留清理：serve 日闸门按 docs/03 保留表清理证据文件、运行记录与日志。

日闸门的形状照 GlobalEventsPruner：本地时刻到点后的第一个 tick 执行一次，
日期键、删除行与结果事件在同一事务里落盘，崩溃重启既不重复也不漏。
文件删除先于事务且逐份容错：事务回滚只会留下「记录在、文件缺」，
下载按既有 410 口径处理；单份文件失败不拦住整轮清理。
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from contracts import TERMINAL_RUN_STATES, Clock, EventType, Logger
from core.api.store import put_settings
from core.log_names import CORE_LOG_ARCHIVED_NAME, CORE_LOG_NAME
from core.logging import NullLogger
from storage.db.transaction import transaction
from storage.defaults import default
from storage.repositories.events import EventLog
from storage.repositories.iso_time import format_at

RETENTION_PRUNED_ON_KEY = "maintenance.retention_pruned_on"
RETENTION_PRUNED_EVENT = "core.retention_pruned"
EVIDENCE_DIRNAME = "evidence"
LOGS_DIRNAME = "logs"
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
class _EvidencePass:
    """文件清理的结果：份数、释放字节与清理后的证据根总量。"""

    files: int = 0
    bytes_freed: int = 0
    bytes_after: int = 0


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

        文件删除在事务之前完成，数据库删除、日期键与结果事件同事务提交：
        进程在事务内崩溃时整体回滚，下个 tick 重试，文件删除可重复执行。

        Returns:
            RetentionOutcome: 本次删除统计；未到点或当天已执行过则为 None。

        Raises:
            OSError: 数据目录不可读。
            sqlite3.Error: 数据库不可写；由调用方记日志，不拖垮核心循环。
        """
        today = self._due_date()
        if today is None:
            return None
        files = self._prune_evidence_files()
        logs = self._prune_logs()
        with transaction(self._connection):
            counts = self._prune_rows()
            outcome = self._outcome(today, files, counts, logs)
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

    def _outcome(
        self,
        today: date,
        files: _EvidencePass,
        counts: _RowCounts,
        logs: int,
    ) -> RetentionOutcome:
        """把文件与行两组统计合成结果；detail 字段在这里一次定型。"""
        return RetentionOutcome(
            date=today.isoformat(),
            evidence_files_deleted=files.files,
            evidence_bytes_freed=files.bytes_freed,
            evidence_bytes_after=files.bytes_after,
            run_events_deleted=counts.run_events,
            run_phases_deleted=counts.run_phases,
            verdicts_deleted=counts.verdicts,
            runs_deleted=counts.runs,
            advisor_deleted=counts.advisor,
            log_files_deleted=logs,
        )

    def _prune_evidence_files(self) -> _EvidencePass:
        """先按天数删终态运行的证据文件，再按总量上限从最旧补删；证据行保留。"""
        root = (self._data_root / EVIDENCE_DIRNAME).resolve()
        if not root.is_dir():
            return _EvidencePass()
        cutoff = format_at(self._clock.utc_now() - timedelta(days=self._policy.evidence_days))
        deleted = 0
        freed = 0
        for relative in _evidence_paths(self._connection, cutoff):
            removed = _unlink_evidence(root, relative, self._log)
            if removed is not None:
                deleted += 1
                freed += removed
        quota_files, quota_bytes = self._enforce_quota(root)
        return _EvidencePass(
            files=deleted + quota_files,
            bytes_freed=freed + quota_bytes,
            bytes_after=_evidence_bytes(root, self._log),
        )

    def _enforce_quota(self, root: Path) -> tuple[int, int]:
        """证据根总量超过上限时，按 evidence.created_at 从旧到新删终态运行的文件。"""
        total = _evidence_bytes(root, self._log)
        if total <= self._policy.evidence_total_bytes:
            return 0, 0
        deleted = 0
        freed = 0
        for relative in _evidence_paths_by_age(self._connection):
            if total <= self._policy.evidence_total_bytes:
                break
            removed = _unlink_evidence(root, relative, self._log)
            if removed is None:
                continue
            deleted += 1
            freed += removed
            total -= removed
        return deleted, freed

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

    def _prune_logs(self) -> int:
        """原始日志按 14 天、压缩件按 90 天删除；watchdog 与不匹配文件不动。"""
        logs_dir = self._data_root / LOGS_DIRNAME
        if not logs_dir.is_dir():
            return 0
        today = self._clock.utc_now().astimezone(UTC).date()
        raw_cutoff = today - timedelta(days=self._policy.logs_days)
        archive_cutoff = today - timedelta(days=self._policy.logs_archive_days)
        deleted = 0
        for path in logs_dir.iterdir():
            parsed = _log_file(path)
            if parsed is None:
                continue
            day, archived = parsed
            cutoff = archive_cutoff if archived else raw_cutoff
            if day >= cutoff or not _remove_file(path, self._log):
                continue
            deleted += 1
        return deleted


def _log_file(path: Path) -> tuple[date, bool] | None:
    """解析 core-日界日志文件名；返回日期与是否压缩件，不匹配返回 None。"""
    if not path.is_file():
        return None
    for pattern, archived in ((CORE_LOG_ARCHIVED_NAME, True), (CORE_LOG_NAME, False)):
        match = pattern.fullmatch(path.name)
        if match is None:
            continue
        try:
            return date.fromisoformat(match.group("day")), archived
        except ValueError:
            return None
    return None


def _remove_file(path: Path, log: Logger) -> bool:
    """删一个日志文件；删不掉只记日志，本轮其余文件继续处理。"""
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    except OSError as error:
        log.warn({"event": "retention.log_delete_failed", "file": path.name, "error": str(error)})
        return False
    return True


def _unlink_evidence(root: Path, relative: str, log: Logger) -> int | None:
    """删一份证据文件并返回释放字节；缺失、越界或失败返回 None。"""
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root):
        log.warn({"event": "retention.evidence_path_outside_root", "path": relative})
        return None
    try:
        size = candidate.stat().st_size
    except FileNotFoundError:
        return None
    except OSError as error:
        log.warn({"event": "retention.evidence_stat_failed", "path": relative, "error": str(error)})
        return None
    try:
        candidate.unlink()
    except FileNotFoundError:
        return None
    except OSError as error:
        log.warn(
            {"event": "retention.evidence_delete_failed", "path": relative, "error": str(error)}
        )
        return None
    _drop_empty_dirs(root, candidate.parent)
    return size


def _drop_empty_dirs(root: Path, start: Path) -> None:
    """删完文件后自下而上收掉空目录；非空或不存在都停手。"""
    current = start
    while current != root and current.is_relative_to(root):
        with suppress(OSError):
            current.rmdir()
        if current.exists():
            break
        current = current.parent


def _evidence_bytes(root: Path, log: Logger) -> int:
    """证据根下全部文件的总字节；单个文件读不到只记日志并跳过。"""
    total = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            total += path.stat().st_size
        except OSError as error:
            log.warn(
                {"event": "retention.evidence_stat_failed", "path": path.name, "error": str(error)}
            )
    return total


def _evidence_paths(connection: sqlite3.Connection, cutoff: str) -> tuple[str, ...]:
    """早于 cutoff 的终态运行证据路径，按创建时间从旧到新。"""
    rows = connection.execute(
        "SELECT e.path FROM evidence e JOIN job_runs r ON r.id = e.run_id "
        f"WHERE r.state IN ({_TERMINAL_MARKS}) AND e.created_at < ? ORDER BY e.created_at",
        (*_TERMINAL_STATES, cutoff),
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _evidence_paths_by_age(connection: sqlite3.Connection) -> tuple[str, ...]:
    """终态运行的全部证据路径，按创建时间从旧到新。"""
    rows = connection.execute(
        "SELECT e.path FROM evidence e JOIN job_runs r ON r.id = e.run_id "
        f"WHERE r.state IN ({_TERMINAL_MARKS}) ORDER BY e.created_at",
        _TERMINAL_STATES,
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


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
    "EVIDENCE_DIRNAME",
    "LOGS_DIRNAME",
    "RETENTION_PRUNED_EVENT",
    "RETENTION_PRUNED_ON_KEY",
    "RetentionOutcome",
    "RetentionPolicy",
    "RetentionRunner",
]
