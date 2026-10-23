"""核心 supervisor：常驻循环里的调度、队列、租约、保温与启动恢复。

每次 tick 只推进一轮：建运行、收敛逾期、回收与续租租约、关闭到期保温、推进一个可执行运行。
所有时间来自注入的 Clock，所有状态落在数据库；崩溃后按持久化状态重建（docs/02、docs/09）。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import Clock, EnvironmentRef, EventType, Logger, RunContext, RunState
from core.environments.service import EnvironmentCall, EnvironmentService
from core.jobs.run_service import JobRunService
from core.jobs.state_machine import can_transition, is_terminal
from core.logging import NullLogger
from core.plugins.registry import PluginRegistry
from core.recovery.policy import RecoveryPolicy
from core.resources.errors import ResourceBusyError
from core.resources.service import LeaseService
from core.scheduler.scan import ScheduleScanner
from core.worker.executor import CancelFlag, WorkerExecutor
from storage.defaults import default
from storage.repositories.environments import EnvironmentRow
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRepository
from storage.repositories.leases import LeaseRow
from storage.repositories.schedules import ScheduleRepository

_RENEW_INTERVAL = timedelta(seconds=int(default("leases", "renew_interval_seconds")))
_LEASE_RECOVER_DELAY = timedelta(seconds=int(default("leases", "recover_delay_seconds")))
_HOLDING_LEASE_STATES = frozenset(
    {
        RunState.PREPARING,
        RunState.WAITING,
        RunState.EXECUTING,
        RunState.VERIFYING,
    }
)
_ORPHAN_STATES = frozenset(
    {
        RunState.PREPARING,
        RunState.EXECUTING,
        RunState.VERIFYING,
        RunState.CLEANING,
    }
)


class Supervisor:
    """单进程内核循环：tick 推进一轮，request_cancel 处理叫停。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        registry: PluginRegistry,
        *,
        data_root: Path,
        log: Logger | None = None,
    ) -> None:
        self._connection = connection
        self._clock = clock
        self._registry = registry
        self._data_root = data_root
        self._log: Logger = NullLogger() if log is None else log
        self._runs = JobRunService(connection, clock)
        self._scanner = ScheduleScanner(connection, clock, self._runs)
        self._templates = JobTemplateRepository(connection)
        self._schedules = ScheduleRepository(connection)
        self._environments = EnvironmentService(connection, clock)
        self._leases = LeaseService(connection, clock)
        self._events = EventLog(connection)
        self._cancel_flags: dict[str, CancelFlag] = {}
        self._last_renew_at: datetime | None = None

    def startup(self) -> tuple[JobRunRecord, ...]:
        """启动扫描：崩溃遗留的执行中运行记 unknown，到点的恢复重新排队，回收孤儿租约。"""
        settled: list[JobRunRecord] = []
        for run in self._runs.non_terminal():
            settled.extend(self._settle_startup(run))
        for lease in self._leases.active():
            run = self._runs.get(lease.run_id)
            if run is None or is_terminal(run.state):
                self._release_orphan(lease)
        return tuple(settled)

    def tick(self) -> None:
        """推进一轮：调度、逾期、租约、保温、队列。"""
        now = self._clock.utc_now()
        self._scanner.tick(now)
        self._runs.fail_overdue(now)
        self._expire_leases(now)
        self._renew_leases(now)
        self._close_warm(now)
        self._dispatch(now)

    def request_cancel(self, run_id: str, reason: str = "user_cancel") -> JobRunRecord:
        """记录叫停：未开始的立即 cancelled；执行中的置位标记，由 worker 在阶段边界收尾。"""
        self._runs.request_cancel(run_id, reason)
        flag = self._cancel_flags.get(run_id)
        if flag is not None:
            flag.cancel()
        return self._runs.get(run_id)

    def _settle_startup(self, run: JobRunRecord) -> tuple[JobRunRecord, ...]:
        if run.state in _ORPHAN_STATES:
            return (self._runs.transition(run.id, RunState.UNKNOWN, reason="startup_recovery"),)
        if run.state is RunState.SCHEDULED:
            return (self._runs.enqueue(run.id),)
        if run.state is RunState.RECOVERING and _retry_due(run, self._clock.utc_now()):
            return (self._runs.transition(run.id, RunState.QUEUED, reason="startup_recovery"),)
        return ()

    def _release_orphan(self, lease: LeaseRow) -> None:
        try:
            self._leases.release(lease.run_id, reason="startup_orphan")
        except Exception as error:
            self._log.warn(
                {
                    "event": "supervisor.orphan_release_failed",
                    "run_id": lease.run_id,
                    "error": str(error),
                }
            )

    def _expire_leases(self, now: datetime) -> None:
        for lease in self._leases.expire(now):
            self._lease_lost(lease)

    def _lease_lost(self, lease: LeaseRow) -> None:
        """租约过期：按恢复预算把运行送回 recovering，预算耗尽记 failed。"""
        run = self._runs.get(lease.run_id)
        if run is None or is_terminal(run.state):
            return
        if not can_transition(run.state, RunState.RECOVERING):
            if can_transition(run.state, RunState.FAILED):
                self._runs.transition(
                    run.id,
                    RunState.FAILED,
                    reason="lease_expired",
                    error_code="E_RESOURCE_LEASE_LOST",
                )
            return
        decision = self._policy_for(run).decide(
            error_code="E_RESOURCE_LEASE_LOST",
            attempt=run.attempt,
        )
        if decision.exhausted:
            self._runs.transition(
                run.id,
                RunState.FAILED,
                reason="lease_expired",
                error_code="E_RESOURCE_LEASE_LOST",
            )
            return
        retry_at = self._clock.utc_now() + _LEASE_RECOVER_DELAY
        self._runs.mark_recovering(
            run.id,
            next_attempt=decision.next_attempt,
            retry_at=retry_at,
            error_code="E_RESOURCE_LEASE_LOST",
        )

    def _renew_leases(self, now: datetime) -> None:
        """每 60 秒给仍持有租约的活动运行续租，不依赖 worker 存活。"""
        if self._last_renew_at is not None and now - self._last_renew_at < _RENEW_INTERVAL:
            return
        self._last_renew_at = now
        renewed: set[str] = set()
        for lease in self._leases.active():
            if lease.run_id in renewed:
                continue
            run = self._runs.get(lease.run_id)
            if run is None or run.state not in _HOLDING_LEASE_STATES:
                continue
            self._leases.renew(lease.run_id)
            renewed.add(lease.run_id)

    def _close_warm(self, now: datetime) -> None:
        for row in self._environments.warm_expired(now):
            self._close_environment(row, now)

    def _close_environment(self, row: EnvironmentRow, now: datetime) -> None:
        try:
            provider = self._registry.environment(row.provider_id)
            call = EnvironmentCall(
                environment_id=row.id,
                provider=provider,
                ref=EnvironmentRef(id=row.id, provider_id=row.provider_id),
                ctx=self._warm_context(row, now),
            )
            self._environments.cleanup(call, keep_warm=False, warm_minutes=0)
        except Exception as error:
            self._log.warn(
                {
                    "event": "supervisor.warm_close_failed",
                    "environment_id": row.id,
                    "error": str(error),
                }
            )

    def _warm_context(self, row: EnvironmentRow, now: datetime) -> RunContext:
        """保温关闭没有运行归属，给插件一份最小上下文；该路径不接受证据。"""
        run_id = f"warm:{row.id}"
        return RunContext(
            run_id=run_id,
            attempt=0,
            template_id="",
            plugin_id=row.provider_id,
            environment_id=row.id,
            config={},
            work_dir=self._data_root / "work" / run_id,
            evidence_dir=self._data_root / "evidence" / run_id,
            deadline_at=now,
            cancel=CancelFlag(),
            clock=self._clock,
            secrets={},
            log=self._log,
            emit=self._emit_global,
            record_evidence=_reject_evidence,
        )

    def _dispatch(self, now: datetime) -> None:
        """推进一个可执行运行：优先排队，其次到点的 waiting 与 recovering。"""
        queued = self._runs.next_queued(limit=1)
        if queued:
            self._execute(queued[0])
            return
        for run in self._runs.non_terminal():
            if run.state is RunState.SCHEDULED:
                self._runs.enqueue(run.id)
                return
            if run.state is RunState.WAITING and _scheduled_due(run, now):
                self._execute(run)
                return
            if run.state is RunState.RECOVERING and _retry_due(run, now):
                self._execute(run)
                return

    def _execute(self, run: JobRunRecord) -> None:
        schedule = None if run.schedule_id is None else self._schedules.get(run.schedule_id)
        keep_warm = 0 if schedule is None else schedule.keep_warm_minutes
        cancel = self._cancel_flags.setdefault(run.id, CancelFlag())
        executor = WorkerExecutor(
            self._connection,
            self._clock,
            self._registry,
            data_root=self._data_root,
            log=self._log,
            keep_warm_minutes=keep_warm,
            cancel=cancel,
        )
        try:
            executor.execute(run.id)
        except ResourceBusyError:
            self._log.info({"event": "supervisor.resource_busy", "run_id": run.id})
        finally:
            self._cancel_flags.pop(run.id, None)

    def _emit_global(self, event: str, payload: Mapping[str, Any]) -> None:
        self._events.append_global(
            EventType.CORE_EVENT,
            {"event": event, "payload": dict(payload)},
            at=self._clock.utc_now(),
        )

    def _policy_for(self, run: JobRunRecord) -> RecoveryPolicy:
        template = self._templates.get(run.template_id)
        if template is None:
            return RecoveryPolicy()
        try:
            return RecoveryPolicy.from_mapping(template.recovery_policy)
        except ValueError:
            return RecoveryPolicy()


def _reject_evidence(kind: str, path: Path, summary: str) -> str:
    raise ValueError("保温关闭不接收证据")


def _retry_at(run: JobRunRecord) -> datetime | None:
    value = run.detail.get("retry_at")
    if not isinstance(value, str):
        return None
    moment = datetime.fromisoformat(value)
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _retry_due(run: JobRunRecord, now: datetime) -> bool:
    retry_at = _retry_at(run)
    return retry_at is None or retry_at <= now


def _scheduled_due(run: JobRunRecord, now: datetime) -> bool:
    return run.scheduled_at is None or run.scheduled_at <= now


__all__ = ["Supervisor"]
