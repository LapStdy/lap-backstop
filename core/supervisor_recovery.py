"""Supervisor 的启动收敛与运行到期判断。

启动扫描按事件流回放决策运行去向并回收孤儿租约；retry/scheduled 两组到期判断
是启动收敛与 tick 调度共用的纯函数。Supervisor 只保留 tick 的推进顺序与调度
决策，这两块归本模块（docs/09）。
"""

from __future__ import annotations

from datetime import UTC, datetime

from contracts import Clock, Logger, RunState
from core.events.replay import ReplayError, RunSnapshot, replay_run
from core.jobs.run_service import JobRunService
from core.jobs.state_machine import can_transition
from core.resources.service import LeaseService
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.leases import LeaseRow

_ORPHAN_STATES = frozenset(
    {
        RunState.PREPARING,
        RunState.EXECUTING,
        RunState.VERIFYING,
        RunState.CLEANING,
    }
)


class StartupRecovery:
    """启动扫描的运行收敛：事件流回放、状态迁移决策与孤儿租约回收。"""

    def __init__(
        self,
        runs: JobRunService,
        leases: LeaseService,
        clock: Clock,
        *,
        log: Logger,
    ) -> None:
        self._runs = runs
        self._leases = leases
        self._clock = clock
        self._log = log

    def replay(self, run: JobRunRecord) -> RunSnapshot | None:
        """回放事件流重建快照；流损坏不拦启动，返回 None 由调用方按表状态兜底。"""
        try:
            return replay_run(self._runs.events(run.id))
        except ReplayError as error:
            self._log.warn(
                {"event": "supervisor.replay_failed", "run_id": run.id, "error": str(error)}
            )
            return None

    def settle_startup(
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
            state is RunState.RECOVERING and retry_due(run, self._clock.utc_now())
        ):
            return self._settle_to(run, RunState.QUEUED, stream_state=state)
        return ()

    def release_orphan(self, lease: LeaseRow) -> None:
        """回收指向终态运行或缺失运行的孤儿租约；释放失败只告警，不拦启动扫描。"""
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


def _retry_at(run: JobRunRecord) -> datetime | None:
    value = run.detail.get("retry_at")
    if not isinstance(value, str):
        return None
    moment = datetime.fromisoformat(value)
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def retry_due(run: JobRunRecord, now: datetime) -> bool:
    """recovering 运行是否到重试时点：detail 无 retry_at 记录视为立即到期。"""
    retry_at = _retry_at(run)
    return retry_at is None or retry_at <= now


def scheduled_due(run: JobRunRecord, now: datetime) -> bool:
    """waiting 运行是否到计划时点：无 scheduled_at 视为立即到期。"""
    return run.scheduled_at is None or run.scheduled_at <= now


__all__ = ["StartupRecovery", "retry_due", "scheduled_due"]
