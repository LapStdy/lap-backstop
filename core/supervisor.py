"""核心 supervisor：常驻循环的推进顺序与调度决策。

每次 tick 只推进一轮：磁盘水位、调度扫描、逾期收敛、租约看护、保温关闭、派发一个
可执行运行；运行由独立执行线程推进，tick 不等待执行（docs/08 停机与阶段边界）。
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
from core.supervisor_routing import (
    ROUTE_WINDOW,
    RouteAnnouncer,
    RouteCandidate,
    route_dispatch,
)
from core.worker.executor import CancelFlag, WorkerExecutor
from storage import open_database
from storage.defaults import default
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRepository
from storage.repositories.schedules import ScheduleRepository

SUSPENDED_EVENT = "host.suspended"
# 运行中墙钟相对单调时钟的跳变上限（docs/08 时钟与 NTP）。
_JUMP_THRESHOLD_SECONDS = float(default("host", "clock_jump_seconds"))


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
        self._templates = JobTemplateRepository(connection)
        self._leases = LeaseService(connection, clock)
        self._events = EventLog(connection)
        # 执行线程的登记与取消标记共用一把锁：登记随派发建立、随线程收尾注销，
        # tick 查重、请求线程置位、停机收尾 join 互不交错。
        self._executors_lock = threading.Lock()
        self._cancel_flags: dict[str, CancelFlag] = {}
        self._executor_threads: dict[str, threading.Thread] = {}
        # 跳变检测的基线：首轮只记录不判定，两轮之间才可算跳变量。
        self._last_wall: datetime | None = None
        self._last_mono: float | None = None
        self._disk_watermark = DiskWatermark(self._disk_free, self._log, self._emit_global)
        self._lease_watch = LeaseSupervision(
            self._runs,
            self._leases,
            self._templates,
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
        self._route_announcer = RouteAnnouncer(
            self._log, self._emit_global, self._registry.degradation_reason
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
        """推进一轮：停机、跳变、水位、调度、逾期、租约、保温、派发。

        停机事件置位后本轮不再推进任何一步：不查磁盘水位、不扫新运行，在途运行由
        执行器在阶段边界收尾（docs/08）。派发只启动执行线程，不等待执行；租约续租
        在这里按节律推进，不依赖执行线程存活（docs/09）。
        磁盘低于 500 MB 时本轮只记事件即中止，serve 循环据此以退出码 5 收场；
        建运行被 2 GB 守卫拒绝时发生点保持待办，磁盘恢复后按补跑策略重算（docs/08）。
        """
        if self._shutdown():
            return
        now = self._clock.utc_now()
        self._check_wall_jump(now)
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

    def join_executors(self, timeout_seconds: float) -> None:
        """等待全部执行线程收尾；超时的线程不再等待，留给下次启动回收（docs/08）。

        serve 循环退出前的收口点：线程各自持有连接与租约收尾口径，这里只等，
        不动数据库；超时记一条日志与全局事件后返回，进程照常退出。

        Args:
            timeout_seconds: 全部线程共享的等待上限秒数。
        """
        deadline = self._clock.monotonic() + timeout_seconds
        with self._executors_lock:
            threads = tuple(self._executor_threads.values())
        for thread in threads:
            thread.join(max(0.0, deadline - self._clock.monotonic()))
        with self._executors_lock:
            stuck = sorted(
                run_id for run_id, thread in self._executor_threads.items() if thread.is_alive()
            )
        if not stuck:
            return
        self._log.error({"event": "supervisor.executor_join_timeout", "run_ids": stuck})
        self._emit_global("supervisor.executor_join_timeout", {"run_ids": stuck})

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
        with self._executors_lock:
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

    def _check_wall_jump(self, now: datetime) -> None:
        """检测墙钟相对单调时钟的跳变；超过阈值按休眠恢复处置（docs/08 时钟与 NTP）。

        两次 tick 之间墙钟增量与单调增量之差即跳变量：进程内无论空闲还是执行运行，
        两个时钟同速前进，差值接近 0，不会误报；休眠恢复或校时回拨才拉出大差值。
        基线每轮都更新，磁盘水位等提前返回不会把两轮间隔攒成假跳变。
        """
        mono = self._clock.monotonic()
        previous_wall, previous_mono = self._last_wall, self._last_mono
        self._last_wall, self._last_mono = now, mono
        if previous_wall is None or previous_mono is None:
            return
        jump = (now - previous_wall).total_seconds() - (mono - previous_mono)
        if abs(jump) <= _JUMP_THRESHOLD_SECONDS:
            return
        self._events.append_global(
            EventType.HOST_EVENT,
            {
                "event": SUSPENDED_EVENT,
                "detail": f"墙钟跳变 {jump:.1f} 秒，全部环境置 unknown 并重新探测",
                "jump_seconds": round(jump, 3),
            },
            at=now,
        )
        self._environment_watch.recover_suspend(detail=f"墙钟跳变 {jump:.1f} 秒")

    def _dispatch(self, now: datetime) -> None:
        """派发一个可执行运行：排队候选先按插件降级路由，其次到点的 waiting 与 recovering。

        路由只作用于排队候选（docs/05 多插件路由与降级）：降级插件的运行让位给可用
        插件的运行，全部降级时不派发并留事件；waiting 与 recovering 仍按原有顺序推进。
        已有执行线程在推进的运行不重复派发，不出现两个线程同时推一次运行（docs/08）。
        """
        queued = self._runs.next_queued(limit=ROUTE_WINDOW)
        if queued:
            decision = route_dispatch(
                self._queued_candidates(queued), self._registry.degradation_reason
            )
            self._route_announcer.record(decision)
            if decision.chosen is not None:
                self._execute(decision.chosen.run)
                return
            if decision.error_code is not None:
                return
        for run in self._runs.non_terminal():
            if run.state is RunState.SCHEDULED:
                self._runs.enqueue(run.id)
                return
            due_waiting = run.state is RunState.WAITING and scheduled_due(run, now)
            if due_waiting and not self._is_inflight(run.id):
                self._execute(run)
                return
            due_retry = run.state is RunState.RECOVERING and retry_due(run, now)
            if due_retry and not self._is_inflight(run.id):
                self._execute(run)
                return

    def _queued_candidates(self, queued: tuple[JobRunRecord, ...]) -> tuple[RouteCandidate, ...]:
        """把排队运行映射成路由候选；模板缺失按空插件 id 交执行器按配置错误收尾。

        已有执行线程在推进的运行不进候选：同一段派发里不重复启动一次运行（docs/08）。
        """
        candidates: list[RouteCandidate] = []
        for run in queued:
            if self._is_inflight(run.id):
                continue
            template = self._templates.get(run.template_id)
            plugin_id = template.plugin_id if template is not None else ""
            candidates.append(RouteCandidate(run=run, plugin_id=plugin_id))
        return tuple(candidates)

    def _execute(self, run: JobRunRecord) -> None:
        """为运行派一个执行线程：tick 只登记与启动，不等执行（docs/08 停机与阶段边界）。"""
        schedule = None if run.schedule_id is None else self._schedules.get(run.schedule_id)
        keep_warm = 0 if schedule is None else schedule.keep_warm_minutes
        cancel = self._cancel_for(run.id)
        # 叫停先落库再置位：标记不存在期间收到的请求，靠事件在这里补上。
        if self._runs.cancel_requested(run.id):
            cancel.cancel()
        self._spawn_executor(run.id, keep_warm, cancel)

    def _spawn_executor(self, run_id: str, keep_warm: int, cancel: CancelFlag) -> None:
        """登记并启动执行线程；启动失败时注销登记，不留阻塞收尾的空位。

        Raises:
            RuntimeError: 线程无法启动，属程序错误，交上层按退出处置。
        """
        thread = threading.Thread(
            target=self._run_executor,
            args=(run_id, keep_warm, cancel),
            name=f"run-executor-{run_id[:8]}",
            daemon=True,
        )
        with self._executors_lock:
            self._executor_threads[run_id] = thread
        try:
            thread.start()
        except RuntimeError:
            with self._executors_lock:
                self._executor_threads.pop(run_id, None)
            self._drop_cancel(run_id)
            raise

    def _run_executor(self, run_id: str, keep_warm: int, cancel: CancelFlag) -> None:
        """执行线程主体：自开连接推进一次运行，收尾时归还标记与登记（docs/11 并发）。

        执行内的意外异常在此兜底：记日志与全局事件，不冒泡进 tick，也不让核心退出；
        运行行保持原样，交既有恢复与启动扫描处置。
        """
        connection = open_database(self._database)
        try:
            executor = WorkerExecutor(
                connection,
                self._clock,
                self._registry,
                data_root=self._data_root,
                log=self._log,
                keep_warm_minutes=keep_warm,
                cancel=cancel,
                shutdown=self._shutdown,
            )
            try:
                executor.execute(run_id)
            except ResourceBusyError:
                self._log.info({"event": "supervisor.resource_busy", "run_id": run_id})
            except Exception as error:
                self._record_executor_failure(connection, run_id, error)
        finally:
            self._drop_cancel(run_id)
            with self._executors_lock:
                self._executor_threads.pop(run_id, None)
            connection.close()

    def _record_executor_failure(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        error: Exception,
    ) -> None:
        """执行线程的兜底记账：日志与全局事件都写在线程自己的连接上（docs/11 并发）。"""
        payload = {"run_id": run_id, "error": str(error), "error_type": type(error).__name__}
        self._log.error({"event": "supervisor.executor_failed", **payload})
        try:
            EventLog(connection).append_global(
                EventType.CORE_EVENT,
                {"event": "supervisor.executor_failed", "payload": payload},
                at=self._clock.utc_now(),
            )
        except sqlite3.Error as db_error:
            self._log.error(
                {
                    "event": "supervisor.executor_event_failed",
                    "run_id": run_id,
                    "error": str(db_error),
                }
            )

    def _is_inflight(self, run_id: str) -> bool:
        """该运行是否已有执行线程在推进；登记在派发时建立、线程收尾时注销。"""
        with self._executors_lock:
            return run_id in self._executor_threads

    def _cancel_for(self, run_id: str) -> CancelFlag:
        """取该运行的取消标记；执行期间同一运行复用同一个对象。"""
        with self._executors_lock:
            return self._cancel_flags.setdefault(run_id, CancelFlag())

    def _drop_cancel(self, run_id: str) -> None:
        """执行收尾后清走标记；之后再来的叫停靠 run_events 在下一轮生效。"""
        with self._executors_lock:
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
