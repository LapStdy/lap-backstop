"""Supervisor 的租约看护：到期处置、丢失送恢复与周期续租。

Supervisor 只保留 tick 的推进顺序与调度决策；租约过期后的运行去向、
恢复预算的判定与续租节律归本模块（docs/09）。
"""

from __future__ import annotations

from datetime import datetime, timedelta

from contracts import Clock, Logger, RunState
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import JobRunService
from core.jobs.state_machine import can_transition, is_terminal
from core.recovery.policy import RecoveryPolicy
from core.resources.service import LeaseService
from storage.defaults import default
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRepository
from storage.repositories.leases import LeaseRow

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


class LeaseSupervision:
    """tick 上的租约看护：过期按恢复预算处置，持有中的活动运行周期续租。"""

    def __init__(
        self,
        runs: JobRunService,
        leases: LeaseService,
        templates: JobTemplateRepository,
        clock: Clock,
        *,
        log: Logger,
    ) -> None:
        self._runs = runs
        self._leases = leases
        self._templates = templates
        self._clock = clock
        self._log = log
        self._last_renew_at: datetime | None = None

    def expire(self, now: datetime) -> None:
        """回收已到期的租约，受影响的运行按恢复预算送 recovering 或标 failed。"""
        for lease in self._leases.expire(now):
            self._lease_lost(lease)

    def renew(self, now: datetime) -> None:
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

    def _policy_for(self, run: JobRunRecord) -> RecoveryPolicy:
        template = self._templates.get(run.template_id)
        if template is None:
            return RecoveryPolicy()
        try:
            return RecoveryPolicy.from_mapping(template.recovery_policy)
        except ValueError:
            return RecoveryPolicy()


__all__ = ["LeaseSupervision"]
