"""核心 supervisor：常驻循环里的调度、队列、租约、保温与启动恢复。

每次 tick 只推进一轮：建运行、收敛逾期、回收与续租租约、关闭到期保温、推进一个可执行运行。
所有时间来自注入的 Clock，所有状态落在数据库；崩溃后按持久化状态重建（docs/02、docs/09）。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import (
    Clock,
    EventType,
    Logger,
    PhaseResult,
    RunState,
)
from core.environments.service import EnvironmentService
from core.events.replay import ReplayError, RunSnapshot, replay_run
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import DiskFullError, JobRunService, disk_free_bytes
from core.jobs.state_machine import can_transition, is_terminal
from core.logging import NullLogger
from core.plugins.registry import PluginRegistry
from core.recovery.policy import RecoveryPolicy
from core.resources.errors import ResourceBusyError
from core.resources.service import LeaseService
from core.scheduler.scan import ScheduleScanner
from core.supervisor_disk import DiskWatermark
from core.supervisor_environments import EnvironmentSupervision
from core.worker.executor import CancelFlag, WorkerExecutor
from storage.defaults import default
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
        # docs/09：租约持有到 cleanup 完成；cleaning 期间同样续租，
        # 不让长 cleanup 因 TTL 过期被回收后把运行标成 failed。
        RunState.CLEANING,
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
        disk_free: Callable[[], int | None] | None = None,
    ) -> None:
        self._connection = connection
        self._clock = clock
        self._registry = registry
        self._data_root = data_root
        self._log: Logger = NullLogger() if log is None else log
        self._disk_free: Callable[[], int | None] = (
            (lambda: disk_free_bytes(data_root)) if disk_free is None else disk_free
        )
        self._runs = JobRunService(connection, clock, disk_free=self._disk_free)
        self._scanner = ScheduleScanner(connection, clock, self._runs)
        self._templates = JobTemplateRepository(connection)
        self._schedules = ScheduleRepository(connection)
        self._leases = LeaseService(connection, clock)
        self._events = EventLog(connection)
        self._cancel_flags: dict[str, CancelFlag] = {}
        self._last_renew_at: datetime | None = None
        self._disk_watermark = DiskWatermark(self._disk_free, self._log, self._emit_global)
        self._environment_watch = EnvironmentSupervision(
            EnvironmentService(connection, clock),
            registry,
            clock,
            data_root=data_root,
            log=self._log,
            emit_global=self._emit_global,
        )

    def startup(self) -> tuple[JobRunRecord, ...]:
        """启动扫描：重建运行、回收孤儿租约，最后对状态不明的环境各探一次。"""
        settled: list[JobRunRecord] = []
        for run in self._runs.non_terminal():
            snapshot = self._replay(run)
            if snapshot is not None and snapshot.attempt != run.attempt:
                self._log.warn(
                    {
                        "event": "supervisor.attempt_divergence",
                        "run_id": run.id,
                        "table_attempt": run.attempt,
                        "stream_attempt": snapshot.attempt,
                    }
                )
            settled.extend(self._settle_startup(run, snapshot))
        for lease in self._leases.active():
            try:
                run = self._runs.get(lease.run_id)
            except RunNotFoundError:
                self._release_orphan(lease)
                continue
            if is_terminal(run.state):
                self._release_orphan(lease)
        self._environment_watch.probe_environments()
        return tuple(settled)

    def tick(self) -> None:
        """推进一轮：水位、调度、逾期、租约、保温、队列。

        磁盘低于 500 MB 时本轮只记事件即中止，serve 循环据此以退出码 5 收场；
        建运行被 2 GB 守卫拒绝时发生点保持待办，磁盘恢复后按补跑策略重算（docs/08）。
        """
        now = self._clock.utc_now()
        if self._disk_watermark.check():
            return
        try:
            self._scanner.tick(now)
        except DiskFullError as error:
            self._log.debug(
                {
                    "event": "supervisor.schedule_disk_guarded",
                    "free_bytes": error.free_bytes,
                }
            )
        self._runs.fail_overdue(now)
        self._expire_leases(now)
        self._renew_leases(now)
        self._environment_watch.close_warm(now)
        self._dispatch(now)

    @property
    def is_disk_critical(self) -> bool:
        """最近一次 tick 是否观测到磁盘低于 500 MB；serve 循环据此退出。"""
        return self._disk_watermark.is_critical

    def request_cancel(self, run_id: str, reason: str = "user_cancel") -> JobRunRecord:
        """记录叫停：未开始的立即 cancelled；执行中的置位标记，由 worker 在阶段边界收尾。"""
        self._runs.request_cancel(run_id, reason)
        flag = self._cancel_flags.get(run_id)
        if flag is not None:
            flag.cancel()
        return self._runs.get(run_id)

    def prepare_environment(self, environment_id: str) -> PhaseResult:
        """手动预热：与 tick 调度无关，不建运行、不改运行状态（docs/09 预热）。

        Raises:
            EnvironmentNotFoundError: 环境不存在。
            EnvironmentStateError: 状态机拒绝，如环境 busy。
            PluginError: provider 缺失或未声明 environment 能力。
        """
        return self._environment_watch.prepare(environment_id)

    def reset_environment(self, environment_id: str) -> PhaseResult:
        """手动复位：任何状态先回 booting，成功后仍等探针确认 ready。

        Raises:
            EnvironmentNotFoundError: 环境不存在。
            PluginError: provider 缺失或未声明 environment 能力。
        """
        return self._environment_watch.reset(environment_id)

    def _replay(self, run: JobRunRecord) -> RunSnapshot | None:
        """回放事件流重建快照；流损坏不拦启动，返回 None 由调用方按表状态兜底。"""
        try:
            return replay_run(self._runs.events(run.id))
        except ReplayError as error:
            self._log.warn(
                {"event": "supervisor.replay_failed", "run_id": run.id, "error": str(error)}
            )
            return None

    def _settle_startup(
        self,
        run: JobRunRecord,
        snapshot: RunSnapshot | None,
    ) -> tuple[JobRunRecord, ...]:
        """决策用事件流回放出的状态；落地前先探表状态能否迁移。

        撕裂（表与事件流不一致）只告警不动表：绕过状态机硬迁会写出与流矛盾的
        被拒迁移记录，让这条流从此回放不了，留给人工处置。
        """
        state = run.state if snapshot is None else snapshot.state
        if state in _ORPHAN_STATES:
            return self._settle_to(run, RunState.UNKNOWN, stream_state=state)
        if state is RunState.SCHEDULED or (
            state is RunState.RECOVERING and _retry_due(run, self._clock.utc_now())
        ):
            return self._settle_to(run, RunState.QUEUED, stream_state=state)
        return ()

    def _settle_to(
        self,
        run: JobRunRecord,
        target: RunState,
        *,
        stream_state: RunState,
    ) -> tuple[JobRunRecord, ...]:
        if run.state is not target and not can_transition(run.state, target):
            self._log.warn(
                {
                    "event": "supervisor.startup_state_conflict",
                    "run_id": run.id,
                    "table_state": run.state.value,
                    "stream_state": stream_state.value,
                }
            )
            return ()
        if target is RunState.QUEUED:
            return (self._runs.enqueue(run.id),)
        return (self._runs.transition(run.id, target, reason="startup_recovery"),)

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
        """租约过期：按恢复预算把运行送回 recovering，预算耗尽记 failed。

        租约在而运行行缺失属库损坏，回收动作在 expire 里已完成，这里无运行可处置。
        """
        try:
            run = self._runs.get(lease.run_id)
        except RunNotFoundError:
            return
        if is_terminal(run.state):
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
