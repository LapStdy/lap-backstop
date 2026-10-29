"""核心 supervisor：常驻循环的推进顺序与调度决策。

每次 tick 只推进一轮：磁盘水位、调度扫描、逾期收敛、租约看护、保温关闭、推进一个可执行运行；
顺序定在本模块，各步机制分别在 supervisor_disk、supervisor_leases、supervisor_environments
与 supervisor_recovery。所有时间来自注入的 Clock，所有状态落在数据库；启动扫描按持久化状态
重建（docs/02、docs/09）。
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Mapping
from datetime import datetime
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
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import DiskFullError, JobRunService, disk_free_bytes
from core.jobs.state_machine import is_terminal
from core.logging import NullLogger
from core.plugins.registry import PluginRegistry
from core.resources.errors import ResourceBusyError
from core.resources.service import LeaseService
from core.scheduler.scan import ScheduleScanner
from core.supervisor_disk import DiskWatermark
from core.supervisor_environments import EnvironmentSupervision
from core.supervisor_leases import LeaseSupervision
from core.supervisor_recovery import StartupRecovery, retry_due, scheduled_due
from core.worker.executor import CancelFlag, WorkerExecutor
from storage import open_database
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRepository
from storage.repositories.schedules import ScheduleRepository


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
        shutdown: Callable[[], bool] | None = None,
    ) -> None:
        self._connection = connection
        self._database = _database_path(connection)
        self._clock = clock
        self._registry = registry
        self._data_root = data_root
        self._log: Logger = NullLogger() if log is None else log
        self._shutdown: Callable[[], bool] = (lambda: False) if shutdown is None else shutdown
        self._disk_free: Callable[[], int | None] = (
            (lambda: disk_free_bytes(data_root)) if disk_free is None else disk_free
        )
        self._runs = JobRunService(connection, clock, disk_free=self._disk_free)
        self._scanner = ScheduleScanner(connection, clock, self._runs)
        self._schedules = ScheduleRepository(connection)
        self._leases = LeaseService(connection, clock)
        self._events = EventLog(connection)
        # 标记的增删归 tick 线程，请求线程只置位；字典读写都在锁内。
        self._cancel_flags: dict[str, CancelFlag] = {}
        self._cancel_lock = threading.Lock()
        self._disk_watermark = DiskWatermark(self._disk_free, self._log, self._emit_global)
        self._lease_watch = LeaseSupervision(
            self._runs,
            self._leases,
            JobTemplateRepository(connection),
            clock,
            log=self._log,
        )
        self._startup_recovery = StartupRecovery(self._runs, self._leases, clock, log=self._log)
        self._environment_watch = EnvironmentSupervision(
            EnvironmentService(connection, clock),
            registry,
            clock,
            data_root=data_root,
            log=self._log,
            emit_global=self._emit_global,
        )

    def startup(self) -> tuple[JobRunRecord, ...]:
        """启动扫描：重建运行、回收孤儿租约，最后对每个环境各探一次。"""
        settled: list[JobRunRecord] = []
        for run in self._runs.non_terminal():
            snapshot = self._startup_recovery.replay(run)
            if snapshot is not None and snapshot.attempt != run.attempt:
                self._log.warn(
                    {
                        "event": "supervisor.attempt_divergence",
                        "run_id": run.id,
                        "table_attempt": run.attempt,
                        "stream_attempt": snapshot.attempt,
                    }
                )
            settled.extend(self._startup_recovery.settle_startup(run, snapshot))
        for lease in self._leases.active():
            try:
                run = self._runs.get(lease.run_id)
            except RunNotFoundError:
                self._startup_recovery.release_orphan(lease)
                continue
            if is_terminal(run.state):
                self._startup_recovery.release_orphan(lease)
        self._environment_watch.probe_environments()
        return tuple(settled)

    def tick(self) -> None:
        """推进一轮：停机、水位、调度、逾期、租约、保温、队列。

        停机事件置位后本轮不再推进任何一步：不查磁盘水位、不扫新运行，在途运行由
        执行器在阶段边界收尾（docs/08）。
        磁盘低于 500 MB 时本轮只记事件即中止，serve 循环据此以退出码 5 收场；
        建运行被 2 GB 守卫拒绝时发生点保持待办，磁盘恢复后按补跑策略重算（docs/08）。
        """
        if self._shutdown():
            return
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
        self._lease_watch.expire(now)
        self._lease_watch.renew(now)
        self._environment_watch.close_warm(now)
        self._dispatch(now)

    @property
    def is_disk_critical(self) -> bool:
        """最近一次 tick 是否观测到磁盘低于 500 MB；serve 循环据此退出。"""
        return self._disk_watermark.is_critical

    def request_cancel(self, run_id: str, reason: str = "user_cancel") -> JobRunRecord:
        """记录叫停：未开始的立即 cancelled；执行中的置位标记，由 worker 在阶段边界收尾。"""
        record = self._runs.request_cancel(run_id, reason)
        self._signal_cancel(run_id)
        return record

    def stop(self, run_id: str, reason: str = "user_cancel") -> JobRunRecord:
        """请求线程的叫停入口：自开连接落取消请求，再置位执行中的取消标记。

        tick 的连接只归 tick 线程（docs/11 并发）；取消请求先落 ``run.interrupted``，
        所以 waiting 或两次 tick 之间没有标记时也不丢，下一轮 dispatch 按事件把
        运行收敛成 cancelled。

        Raises:
            RunNotFoundError: 运行不存在。
            IllegalRunTransitionError: 运行已到终态，不能再取消。
        """
        connection = open_database(self._database)
        try:
            service = JobRunService(connection, self._clock)
            try:
                return service.request_cancel(run_id, reason)
            finally:
                # 落库与行迁移竞争失败时仍要触达执行器；终态运行的标记已由
                # tick 线程清走，置位是空操作。
                self._signal_cancel(run_id)
        finally:
            connection.close()

    def _signal_cancel(self, run_id: str) -> None:
        """置位该运行的取消标记；标记不存在（waiting、两次 tick 之间）时不动。"""
        with self._cancel_lock:
            flag = self._cancel_flags.get(run_id)
            if flag is not None:
                flag.cancel()

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
            if run.state is RunState.WAITING and scheduled_due(run, now):
                self._execute(run)
                return
            if run.state is RunState.RECOVERING and retry_due(run, now):
                self._execute(run)
                return

    def _execute(self, run: JobRunRecord) -> None:
        schedule = None if run.schedule_id is None else self._schedules.get(run.schedule_id)
        keep_warm = 0 if schedule is None else schedule.keep_warm_minutes
        cancel = self._cancel_for(run.id)
        # 叫停先落库再置位：标记不存在期间收到的请求，靠事件在这里补上。
        if self._runs.cancel_requested(run.id):
            cancel.cancel()
        executor = WorkerExecutor(
            self._connection,
            self._clock,
            self._registry,
            data_root=self._data_root,
            log=self._log,
            keep_warm_minutes=keep_warm,
            cancel=cancel,
            shutdown=self._shutdown,
        )
        try:
            executor.execute(run.id)
        except ResourceBusyError:
            self._log.info({"event": "supervisor.resource_busy", "run_id": run.id})
        finally:
            self._drop_cancel(run.id)

    def _cancel_for(self, run_id: str) -> CancelFlag:
        """取该运行的取消标记；执行期间同一运行复用同一个对象。"""
        with self._cancel_lock:
            return self._cancel_flags.setdefault(run_id, CancelFlag())

    def _drop_cancel(self, run_id: str) -> None:
        """执行收尾后清走标记；之后再来的叫停靠 run_events 在下一轮生效。"""
        with self._cancel_lock:
            self._cancel_flags.pop(run_id, None)

    def _emit_global(self, event: str, payload: Mapping[str, Any]) -> None:
        self._events.append_global(
            EventType.CORE_EVENT,
            {"event": event, "payload": dict(payload)},
            at=self._clock.utc_now(),
        )


def _database_path(connection: sqlite3.Connection) -> Path:
    """取主库文件路径：叫停入口在请求线程自开连接，内存库无法这样共享。

    Raises:
        ValueError: 连接指向内存库或临时库。
    """
    row = connection.execute("PRAGMA database_list").fetchone()
    file_name = "" if row is None else str(row[2])
    if not file_name:
        raise ValueError("Supervisor 需要文件数据库：叫停入口在请求线程自开连接")
    return Path(file_name)


__all__ = ["Supervisor"]
