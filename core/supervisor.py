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
    EnvironmentRef,
    EnvironmentState,
    EventType,
    Logger,
    RunContext,
    RunState,
)
from core.environments.service import EnvironmentCall, EnvironmentService
from core.events.replay import ReplayError, RunSnapshot, replay_run
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import (
    DISK_CRITICAL_BYTES,
    DISK_GUARD_BYTES,
    DISK_WARN_BYTES,
    DiskFullError,
    JobRunService,
    disk_free_bytes,
)
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
# docs/08 磁盘水位的事件名；字节阈值真源在 run_service。
DISK_LOW_EVENT = "host.disk_low"
DISK_FULL_GUARD_EVENT = "host.disk_full_guard"
DISK_CRITICAL_EVENT = "host.disk_critical"
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
# 崩溃只让中间态失真：off 是稳定关机、ready 可被下一次运行直接复用，
# 其余状态都可能是崩溃瞬间的半程，启动时必须重新探测（docs/09）。
_STARTUP_PROBE_SKIP = frozenset({EnvironmentState.OFF, EnvironmentState.READY})


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
        self._environments = EnvironmentService(connection, clock)
        self._leases = LeaseService(connection, clock)
        self._events = EventLog(connection)
        self._cancel_flags: dict[str, CancelFlag] = {}
        self._last_renew_at: datetime | None = None
        self._disk_warn_reported = False
        self._disk_guard_reported = False
        self._disk_critical_reported = False

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
        self._probe_environments()
        return tuple(settled)

    def tick(self) -> None:
        """推进一轮：水位、调度、逾期、租约、保温、队列。

        磁盘低于 500 MB 时本轮只记事件即中止，serve 循环据此以退出码 5 收场；
        建运行被 2 GB 守卫拒绝时发生点保持待办，磁盘恢复后按补跑策略重算（docs/08）。
        """
        now = self._clock.utc_now()
        if self._check_disk():
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
        self._close_warm(now)
        self._dispatch(now)

    @property
    def is_disk_critical(self) -> bool:
        """最近一次 tick 是否观测到磁盘低于 500 MB；serve 循环据此退出。"""
        return self._disk_critical_reported

    def _check_disk(self) -> bool:
        """三级水位检查点（docs/08）：warn 与 guard 按边沿各记一次，critical 触发停机。

        三档互斥取当前最深一档；回到上一档重新武装边沿，再次下跌时重新报。

        Returns:
            True 表示低于 500 MB，本轮立即中止。
        """
        free = self._disk_free()
        if free is None:
            return False
        if free < DISK_CRITICAL_BYTES:
            if not self._disk_critical_reported:
                self._disk_critical_reported = True
                self._announce_disk(
                    self._log.error,
                    DISK_CRITICAL_EVENT,
                    {"free_bytes": free, "threshold_bytes": DISK_CRITICAL_BYTES},
                )
            return True
        if free < DISK_GUARD_BYTES:
            if not self._disk_guard_reported:
                self._disk_guard_reported = True
                self._announce_disk(
                    self._log.warn,
                    DISK_FULL_GUARD_EVENT,
                    {
                        "free_bytes": free,
                        "threshold_bytes": DISK_GUARD_BYTES,
                        "error_code": "E_DISK",
                    },
                )
            return False
        self._disk_guard_reported = False
        if free < DISK_WARN_BYTES:
            if not self._disk_warn_reported:
                self._disk_warn_reported = True
                self._announce_disk(
                    self._log.warn,
                    DISK_LOW_EVENT,
                    {"free_bytes": free, "threshold_bytes": DISK_WARN_BYTES},
                )
            return False
        self._disk_warn_reported = False
        return False

    def _announce_disk(
        self,
        notify: Callable[[Mapping[str, Any]], None],
        event: str,
        fields: dict[str, object],
    ) -> None:
        """同一份水位字段进日志与全局事件：日志供排障，事件入库可回放。"""
        notify({"event": event, **fields})
        self._emit_global(event, fields)

    def request_cancel(self, run_id: str, reason: str = "user_cancel") -> JobRunRecord:
        """记录叫停：未开始的立即 cancelled；执行中的置位标记，由 worker 在阶段边界收尾。"""
        self._runs.request_cancel(run_id, reason)
        flag = self._cancel_flags.get(run_id)
        if flag is not None:
            flag.cancel()
        return self._runs.get(run_id)

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

    def _probe_environments(self) -> None:
        """启动探测与运行恢复互相独立：不管运行重建结果如何，中间态环境各探一次。

        只挂在启动扫描上，tick 不探测；探后仍 unknown 的等下一次启动再判，本轮不补探。
        """
        now = self._clock.utc_now()
        for row in self._environments.list_all():
            if row.state in _STARTUP_PROBE_SKIP:
                continue
            try:
                provider = self._registry.environment(row.provider_id)
            except Exception as error:
                self._log.warn(
                    {
                        "event": "supervisor.probe_provider_missing",
                        "environment_id": row.id,
                        "error": str(error),
                    }
                )
                self._environments.mark_unknown(row.id, detail=f"探针不可用：{error}")
                continue
            call = EnvironmentCall(
                environment_id=row.id,
                provider=provider,
                ref=EnvironmentRef(id=row.id, provider_id=row.provider_id),
                ctx=self._standalone_context(row, now, "probe"),
            )
            self._environments.probe(call)

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
                ctx=self._standalone_context(row, now, "warm"),
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

    def _standalone_context(
        self,
        row: EnvironmentRow,
        now: datetime,
        purpose: str,
    ) -> RunContext:
        """无运行归属的内核动作（保温关闭、启动探测）给插件的最小上下文；不接受证据。"""
        run_id = f"{purpose}:{row.id}"
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
    """拒绝证据写入：无运行归属的动作没有可挂靠的运行与证据目录。"""
    raise ValueError("无运行归属的内核动作不接收证据")


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
