"""JobRun 生命周期服务：幂等创建、状态迁移、事件追加、队列与截止时间。"""

from __future__ import annotations

import sqlite3
from datetime import datetime

from contracts import TERMINAL_RUN_STATES, Clock, EventType, RunState
from core.jobs.errors import IllegalRunTransitionError, RunNotFoundError
from core.jobs.state_machine import can_transition, is_terminal
from storage.db.transaction import transaction
from storage.repositories.events import EventLog, StoredEvent
from storage.repositories.job_runs import (
    JobRunRecord,
    JobRunRepository,
    NewRun,
    StateChange,
)


class JobRunService:
    """把状态机、仓储与事件日志拼成一次可回放的运行生命周期。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock) -> None:
        self._connection = connection
        self._clock = clock
        self._runs = JobRunRepository(connection)
        self._events = EventLog(connection)

    def get(self, run_id: str) -> JobRunRecord:
        """取运行；不存在抛 ``RunNotFoundError``。"""
        record = self._runs.get(run_id)
        if record is None:
            raise RunNotFoundError(f"JobRun 不存在：{run_id}")
        return record

    def create_scheduled(
        self,
        run: NewRun,
        merged_scheduled_ats: tuple[datetime, ...] = (),
    ) -> tuple[JobRunRecord, bool]:
        """幂等创建计划运行，并写 ``run.created``。

        Returns:
            (记录, 是否新建)；重复发生点命中幂等键时返回既有记录与 False。
        """
        with transaction(self._connection):
            record, created = self._runs.create_or_get(run)
            if created:
                self._events.append_run(
                    record.id,
                    EventType.RUN_CREATED,
                    _created_payload(record, merged_scheduled_ats),
                    at=run.created_at,
                )
        return record, created

    def enqueue(self, run_id: str) -> JobRunRecord:
        """把 scheduled 或 recovering 的运行放入队列，并写 ``run.queued``。"""
        record = self.transition(run_id, RunState.QUEUED, reason="enqueue")
        now = self._clock.utc_now()
        self._events.append_run(
            run_id,
            EventType.RUN_QUEUED,
            {"run_id": run_id, "priority": record.priority},
            at=now,
        )
        return record

    def transition(
        self,
        run_id: str,
        target: RunState,
        *,
        reason: str = "",
        error_code: str | None = None,
    ) -> JobRunRecord:
        """按状态机迁移一次。

        Args:
            run_id: 要迁移的运行 id。
            target: 目标状态。
            reason: 迁移原因，写入 ``run.state.changed`` 的 reason。
            error_code: 目标状态对应的错误码，终态时随运行记录落库。

        Returns:
            迁移后的运行记录。

        Raises:
            RunNotFoundError: 运行不存在。
            IllegalRunTransitionError: 状态机拒绝本次迁移，或并发修改导致迁移未生效。
        """
        record = self.get(run_id)
        now = self._clock.utc_now()
        if not can_transition(record.state, target):
            self._reject(record, target, reason, now)
            raise IllegalRunTransitionError(
                f"JobRun 不允许从 {record.state.value} 迁到 {target.value}"
            )
        updated = False
        with transaction(self._connection):
            updated = self._runs.transition(
                StateChange(
                    run_id=run_id,
                    source=record.state,
                    target=target,
                    at=now,
                    error_code=error_code,
                )
            )
            if updated:
                self._events.append_run(
                    run_id,
                    EventType.RUN_STATE_CHANGED,
                    {
                        "run_id": run_id,
                        "from_state": record.state.value,
                        "to_state": target.value,
                        "reason": reason,
                    },
                    at=now,
                )
                if target in TERMINAL_RUN_STATES:
                    self._events.append_run(
                        run_id,
                        EventType.RUN_FINISHED,
                        {
                            "run_id": run_id,
                            "status": target.value,
                            "duration_ms": _duration_ms(record, now),
                        },
                        at=now,
                    )
        if not updated:
            self._reject(record, target, reason, now)
            raise IllegalRunTransitionError("运行状态已被并发修改，迁移未生效")
        return self.get(run_id)

    def fail_overdue(self, now: datetime | None = None) -> tuple[JobRunRecord, ...]:
        """把超过截止时间仍未进入 executing 的运行记为 failed / E_DEADLINE。"""
        moment = self._clock.utc_now() if now is None else now
        failed: list[JobRunRecord] = []
        for record in self._runs.overdue(moment):
            current = self.enqueue(record.id) if record.state is RunState.SCHEDULED else record
            failed.append(
                self.transition(
                    current.id,
                    RunState.FAILED,
                    reason="deadline",
                    error_code="E_DEADLINE",
                )
            )
        return tuple(failed)

    def request_cancel(self, run_id: str, reason: str = "") -> JobRunRecord:
        """记录取消请求；排队或未开始的运行立即取消，其余等阶段边界。"""
        record = self.get(run_id)
        if is_terminal(record.state):
            raise IllegalRunTransitionError("终态运行不能取消")
        now = self._clock.utc_now()
        self._events.append_run(
            run_id,
            EventType.RUN_INTERRUPTED,
            {"run_id": run_id, "reason": reason or "user_cancel"},
            at=now,
        )
        if record.state in (RunState.SCHEDULED, RunState.QUEUED):
            return self.transition(run_id, RunState.CANCELLED, reason=reason or "user_cancel")
        return record

    def next_queued(self, limit: int = 1) -> tuple[JobRunRecord, ...]:
        """按出队顺序取排队运行。"""
        return self._runs.queue(limit)

    def mark_recovering(
        self,
        run_id: str,
        *,
        next_attempt: int,
        retry_at: datetime,
        error_code: str,
    ) -> JobRunRecord:
        """进入 recovering：写下次 attempt 与重试时刻，交给调度方按时间恢复。"""
        with transaction(self._connection):
            self.transition(run_id, RunState.RECOVERING, reason=error_code, error_code=error_code)
            self._runs.set_attempt(run_id, next_attempt)
            self._runs.set_detail(
                run_id,
                {"retry_at": retry_at.isoformat(), "error_code": error_code},
            )
        return self.get(run_id)

    def non_terminal(self) -> tuple[JobRunRecord, ...]:
        """所有非终态运行，供启动恢复扫描。"""
        return self._runs.non_terminal()

    def events(self, run_id: str) -> tuple[StoredEvent, ...]:
        """某个运行的事件流，按 seq 升序。"""
        return self._events.events_for_run(run_id)

    def _reject(
        self,
        record: JobRunRecord,
        target: RunState,
        reason: str,
        at: datetime,
    ) -> None:
        self._events.append_run(
            record.id,
            EventType.RUN_STATE_CHANGED,
            {
                "run_id": record.id,
                "from_state": record.state.value,
                "to_state": target.value,
                "reason": reason or "illegal_transition",
            },
            at=at,
        )


def _created_payload(
    record: JobRunRecord,
    merged_scheduled_ats: tuple[datetime, ...],
) -> dict[str, object]:
    return {
        "run_id": record.id,
        "template_id": record.template_id,
        "schedule_id": record.schedule_id,
        "scheduled_at": "" if record.scheduled_at is None else record.scheduled_at.isoformat(),
        "merged_scheduled_ats": [moment.isoformat() for moment in merged_scheduled_ats],
    }


def _duration_ms(record: JobRunRecord, now: datetime) -> int:
    start = record.started_at if record.started_at is not None else record.created_at
    return int((now - start).total_seconds() * 1000)
