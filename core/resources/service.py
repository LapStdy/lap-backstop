"""租约服务：先租后跑、TTL 续租、过期回收。

容量判断与插入在同一事务里完成；单写连接保证判断与写入之间不会插进第三方。
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from contracts import Clock, EventType
from core.jobs.errors import RunNotFoundError
from core.resources.errors import ResourceBusyError, ResourceNotFoundError
from storage.db.transaction import transaction
from storage.defaults import default
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRepository
from storage.repositories.leases import LeaseRepository, LeaseRow, NewLease
from storage.repositories.resources import ResourceRepository

OWNER = "supervisor"
DEFAULT_TTL = timedelta(seconds=int(default("leases", "default_ttl_seconds")))


class LeaseService:
    """资源租约的生命周期。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock) -> None:
        self._connection = connection
        self._clock = clock
        self._resources = ResourceRepository(connection)
        self._leases = LeaseRepository(connection)
        self._runs = JobRunRepository(connection)
        self._events = EventLog(connection)

    def acquire(
        self,
        run_id: str,
        resource_ids: tuple[str, ...],
        *,
        ttl: timedelta | None = None,
    ) -> tuple[LeaseRow, ...]:
        """按资源 id 升序一次性获取全部租约；任一不可用则不获取任何租约。

        Raises:
            RunNotFoundError: 运行不存在。
            ResourceNotFoundError: 资源未注册。
            ResourceBusyError: 某资源已被占满，对应错误码 E_BUSY。
        """
        ordered = tuple(sorted(set(resource_ids)))
        if not ordered:
            return ()
        if self._runs.get(run_id) is None:
            raise RunNotFoundError(f"JobRun 不存在：{run_id}")
        now = self._clock.utc_now()
        lifetime = DEFAULT_TTL if ttl is None else ttl
        leases: list[LeaseRow] = []
        with transaction(self._connection):
            for resource_id in ordered:
                resource = self._resources.get(resource_id)
                if resource is None:
                    raise ResourceNotFoundError(f"资源不存在：{resource_id}")
                if len(self._leases.active_for(resource_id)) >= resource.capacity:
                    raise ResourceBusyError(f"资源已被占用：{resource_id}")
            for resource_id in ordered:
                lease = self._leases.insert(
                    NewLease(
                        resource_id=resource_id,
                        run_id=run_id,
                        owner=OWNER,
                        acquired_at=now,
                        expires_at=now + lifetime,
                    )
                )
                self._events.append_run(
                    run_id,
                    EventType.RESOURCE_LEASED,
                    {
                        "resource_id": resource_id,
                        "run_id": run_id,
                        "expires_at": lease.expires_at.isoformat(),
                    },
                    at=now,
                )
                leases.append(lease)
        return tuple(leases)

    def renew(self, run_id: str, *, ttl: timedelta | None = None) -> tuple[LeaseRow, ...]:
        """给运行持有的全部租约续期，返回续期后的记录。"""
        now = self._clock.utc_now()
        lifetime = DEFAULT_TTL if ttl is None else ttl
        renewed: list[LeaseRow] = []
        with transaction(self._connection):
            for lease in self._leases.active_for_run(run_id):
                if not self._leases.renew(lease.id, expires_at=now + lifetime, at=now):
                    continue
                record = self._leases.get(lease.id)
                if record is None:
                    continue
                self._events.append_run(
                    run_id,
                    EventType.RESOURCE_RENEWED,
                    {
                        "resource_id": record.resource_id,
                        "run_id": run_id,
                        "expires_at": record.expires_at.isoformat(),
                    },
                    at=now,
                )
                renewed.append(record)
        return tuple(renewed)

    def release(self, run_id: str, *, reason: str = "released") -> tuple[LeaseRow, ...]:
        """释放运行持有的全部租约。"""
        now = self._clock.utc_now()
        released: list[LeaseRow] = []
        with transaction(self._connection):
            for lease in self._leases.active_for_run(run_id):
                if not self._leases.release(lease.id):
                    continue
                record = self._leases.get(lease.id)
                if record is None:
                    continue
                self._events.append_run(
                    run_id,
                    EventType.RESOURCE_RELEASED,
                    {
                        "resource_id": record.resource_id,
                        "run_id": run_id,
                        "reason": reason,
                    },
                    at=now,
                )
                released.append(record)
        return tuple(released)

    def expire(self, now: datetime | None = None) -> tuple[LeaseRow, ...]:
        """回收过期租约：leased → expiring → released，并写事件。"""
        moment = self._clock.utc_now() if now is None else now
        expired: list[LeaseRow] = []
        with transaction(self._connection):
            self._leases.begin_expiry(moment)
            released = self._leases.finish_expiry(moment)
            for lease in released:
                self._events.append_run(
                    lease.run_id,
                    EventType.RESOURCE_RELEASED,
                    {
                        "resource_id": lease.resource_id,
                        "run_id": lease.run_id,
                        "reason": "expired",
                    },
                    at=moment,
                )
                self._events.append_global(
                    EventType.CORE_EVENT,
                    {
                        "event": "core.lease_expired",
                        "detail": f"{lease.resource_id}/{lease.run_id}",
                    },
                    at=moment,
                )
                record = self._leases.get(lease.id)
                if record is not None:
                    expired.append(record)
        return tuple(expired)

    def active_for_run(self, run_id: str) -> tuple[LeaseRow, ...]:
        """运行当前持有的租约。"""
        return self._leases.active_for_run(run_id)

    def active(self) -> tuple[LeaseRow, ...]:
        """全部活跃租约；启动清理孤儿租约与续租扫描用。"""
        return self._leases.active()
