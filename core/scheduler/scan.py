"""调度扫描：把持久化的 next_run_at 变成 JobRun。

每个调度一次 tick 只推进自己的 next_run_at；创建运行与推进发生在同一事务里，
崩溃时要么两件事都落盘，要么都不落盘，重启后按补跑策略重算。
补跑暂停（启动校时写入的 settings 标记，docs/08）期间跳过错过的发生点。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from contracts import Clock, RunState
from core.hostops.timesync import catch_up_paused
from core.jobs.run_service import JobRunService
from core.scheduler.occurrences import (
    CatchUp,
    Occurrence,
    ScheduleKind,
    ScheduleSpec,
    next_occurrence,
    plan_missed,
    scheduled_idempotency_key,
)
from storage.db.transaction import transaction
from storage.repositories.job_runs import JobRunRecord, NewRun
from storage.repositories.schedules import ScheduleRepository, ScheduleRow


class ScheduleScanner:
    """扫描启用调度，到点或到预热点就建 run。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        runs: JobRunService,
    ) -> None:
        self._connection = connection
        self._clock = clock
        self._runs = runs
        self._schedules = ScheduleRepository(connection)

    def tick(self, now: datetime | None = None) -> tuple[JobRunRecord, ...]:
        """推进一步；返回本次新建（并已入队）的运行。"""
        moment = self._clock.utc_now() if now is None else now
        rows = self._schedules.enabled()
        paused = catch_up_paused(self._connection) if rows else False
        created: list[JobRunRecord] = []
        for row in rows:
            spec = spec_from_row(row)
            created.extend(self._advance(row, spec, moment, paused))
        return tuple(created)

    def _advance(
        self,
        row: ScheduleRow,
        spec: ScheduleSpec,
        moment: datetime,
        paused: bool,
    ) -> tuple[JobRunRecord, ...]:
        if spec.next_run_at is None:
            with transaction(self._connection):
                self._schedules.set_next_run(
                    spec.id,
                    next_occurrence(spec, moment),
                    updated_at=moment,
                )
            return ()
        trigger = spec.next_run_at - timedelta(minutes=spec.preheat_minutes)
        if moment < trigger:
            return ()
        if moment >= spec.next_run_at:
            # 补跑暂停时把已到点的发生点整段跳过，next_run_at 照常推进；
            # 时钟不可信期间的欠账不补，等暂停解除后从下一个发生点继续。
            occurrences = () if paused else plan_missed(spec, moment)
            advance_from = moment
        else:
            occurrences = (Occurrence(scheduled_at=spec.next_run_at),)
            advance_from = spec.next_run_at
        created: list[JobRunRecord] = []
        with transaction(self._connection):
            for occurrence in occurrences:
                record, is_new = self._runs.create_scheduled(
                    _new_run(row, occurrence, moment),
                    merged_scheduled_ats=occurrence.merged_scheduled_ats,
                )
                if is_new or record.state is RunState.SCHEDULED:
                    self._runs.enqueue(record.id)
                created.append(record)
            self._schedules.set_next_run(
                spec.id,
                next_occurrence(spec, advance_from),
                updated_at=moment,
            )
        return tuple(created)


def spec_from_row(row: ScheduleRow) -> ScheduleSpec:
    """把仓储记录还原成调度语义对象。

    Raises:
        ValueError: 枚举取值或 interval 的数值无法解析。
        core.scheduler.occurrences.ScheduleError: 调度字段自相矛盾。
    """
    return ScheduleSpec(
        id=row.id,
        kind=ScheduleKind(row.kind),
        timezone=row.timezone,
        catch_up=CatchUp(row.catch_up),
        window_minutes=row.window_minutes,
        coalesce=row.coalesce,
        priority=row.priority,
        deadline_minutes=row.deadline_minutes,
        preheat_minutes=row.preheat_minutes,
        keep_warm_minutes=row.keep_warm_minutes,
        expr=row.expr,
        at=row.at,
        every_minutes=_interval_minutes(row),
        created_at=row.created_at,
        next_run_at=row.next_run_at,
        enabled=row.enabled,
    )


def _interval_minutes(row: ScheduleRow) -> int | None:
    if row.kind != ScheduleKind.INTERVAL.value:
        return None
    if row.expr is None:
        raise ValueError(f"调度 {row.id} 是 interval 但缺 every_minutes")
    return int(row.expr)


def _new_run(row: ScheduleRow, occurrence: Occurrence, created_at: datetime) -> NewRun:
    return NewRun(
        template_id=row.job_template_id,
        schedule_id=row.id,
        scheduled_at=occurrence.scheduled_at,
        created_at=created_at,
        idempotency_key=scheduled_idempotency_key(row.id, occurrence.scheduled_at),
        priority=row.priority,
        deadline_at=occurrence.scheduled_at + timedelta(minutes=row.deadline_minutes),
    )
