"""核心 supervisor：常驻循环的推进顺序与调度决策。

每次 tick 只推进一轮：磁盘水位、调度扫描、逾期收敛、租约看护、保温关闭、派发一个
可执行运行；运行由独立执行线程推进，tick 不等待执行（docs/08 停机与阶段边界）。
顺序定在本模块，各步机制分别在 supervisor_disk、supervisor_leases、
supervisor_environments、supervisor_recovery、supervisor_routing、supervisor_shutdown
与 supervisor_executors。所有时间来自注入的 Clock，所有状态落在数据库；启动扫描按
持久化状态重建（docs/02、docs/09）。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, cast

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
from core.resources.service import LeaseService
from core.scheduler.scan import ScheduleScanner
from core.supervisor_disk import DiskWatermark
from core.supervisor_environments import EnvironmentSupervision, SupervisionIo
from core.supervisor_executors import ExecutorSupervision, ExecutorSupervisionDeps
from core.supervisor_leases import LeaseSupervision, LeaseSupervisionDeps
from core.supervisor_recovery import StartupRecovery, retry_due, scheduled_due
from core.supervisor_routing import (
    ROUTE_WINDOW,
    RouteAnnouncer,
    RouteCandidate,
    route_dispatch,
)
from storage import open_database
from storage.defaults import default
from storage.repositories.events import EventLog
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRepository
from storage.repositories.schedules import ScheduleRepository

SUSPENDED_EVENT = "host.suspended"
# 运行中墙钟相对单调时钟的跳变上限（docs/08 时钟与 NTP）。
_JUMP_THRESHOLD_SECONDS = float(default("host", "clock_jump_seconds"))


@dataclass(frozen=True, slots=True)
class SupervisorOptions:
    """装配选项：数据根、日志、磁盘探针、停机判定与宽限终止入口；缺省按默认装配。"""

    data_root: Path
    log: Logger | None = None
    disk_free: Callable[[], int | None] | None = None
    shutdown: Callable[[], bool] | None = None
    abort_workers: Callable[[Sequence[str]], None] | None = None


def _assemble_options(options: Mapping[str, object]) -> SupervisorOptions:
    """把装配关键字收敛成选项：缺 data_root、未知键或类型不符按 TypeError 拒绝。"""
    unknown = sorted(set(options) - {f.name for f in fields(SupervisorOptions)})
    if unknown:
        raise TypeError(f"未知装配选项：{', '.join(unknown)}")
    data_root = options.get("data_root")
    if not isinstance(data_root, Path):
        raise TypeError("data_root 缺失或不是 Path")
    log = options.get("log")
    if log is not None and not callable(getattr(log, "error", None)):
        raise TypeError("log 必须实现 Logger 协议")
    disk_free = options.get("disk_free")
    if disk_free is not None and not callable(disk_free):
        raise TypeError("disk_free 必须可调用")
    shutdown = options.get("shutdown")
    if shutdown is not None and not callable(shutdown):
        raise TypeError("shutdown 必须可调用")
    abort_workers = options.get("abort_workers")
    if abort_workers is not None and not callable(abort_workers):
        raise TypeError("abort_workers 必须可调用")
    return SupervisorOptions(
        data_root=data_root,
        # Logger 是结构化 Protocol，无运行时判别可用；上面已按 error 方法鸭子校验。
        log=cast("Logger | None", log),
        disk_free=disk_free,
        shutdown=shutdown,
        abort_workers=abort_workers,
    )


class Supervisor:
    """单进程内核循环：tick 推进一轮，request_cancel 处理叫停。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        registry: PluginRegistry,
        **options: object,
    ) -> None:
        """装配常驻内核循环；可注入项见 SupervisorOptions，未注入的按默认装配。

        Args:
            connection: tick 线程专用的文件库连接。
            clock: 全部时间的来源。
            registry: 插件注册表，供降级路由与环境动作解析 provider。
            **options: 装配选项；data_root 必填，缺项、未知键或类型不符按 TypeError 拒绝。

        Raises:
            ValueError: 连接指向内存库或临时库（叫停入口需要文件数据库）。
            TypeError: 选项缺 data_root、含未知键或类型不符。
        """
        self._connection = connection
        self._database = _database_path(connection)
        self._options = _assemble_options(options)
        self._clock = clock
        self._registry = registry
        self._wire(connection)
        self._wire_supervision(connection)

    def _wire(self, connection: sqlite3.Connection) -> None:
        """装配默认值与数据底座：日志、停机判定、磁盘探针、事件与运行调度租约仓储。"""
        options = self._options
        self._log: Logger = NullLogger() if options.log is None else options.log
        self._shutdown: Callable[[], bool] = (
            (lambda: False) if options.shutdown is None else options.shutdown
        )
        self._disk_free: Callable[[], int | None] = (
            (lambda: disk_free_bytes(options.data_root))
            if options.disk_free is None
            else options.disk_free
        )
        self._events = EventLog(connection)
        # 跳变检测的基线：首轮只记录不判定，两轮之间才可算跳变量。
        self._last_wall: datetime | None = None
        self._last_mono: float | None = None
        self._runs = JobRunService(connection, self._clock, disk_free=self._disk_free)
        self._scanner = ScheduleScanner(connection, self._clock, self._runs)
        self._schedules = ScheduleRepository(connection)
        self._templates = JobTemplateRepository(connection)
        self._leases = LeaseService(connection, self._clock)

    def _wire_supervision(self, connection: sqlite3.Connection) -> None:
        """装配各看护：磁盘水位、租约、启动恢复、环境、路由留痕与执行线程登记。

        执行线程看护最后装配：它拿数据库路径与停机终止入口，收口宽限终止。
        """
        self._disk_watermark = DiskWatermark(self._disk_free, self._log, self._emit_global)
        self._lease_watch = LeaseSupervision(
            LeaseSupervisionDeps(self._runs, self._leases, self._templates),
            self._clock,
            log=self._log,
        )
        self._startup_recovery = StartupRecovery(
            self._runs, self._leases, self._clock, log=self._log
        )
        self._environment_watch = EnvironmentSupervision(
            EnvironmentService(connection, self._clock),
            self._registry,
            self._clock,
            io=SupervisionIo(
                data_root=self._options.data_root,
                log=self._log,
                emit_global=self._emit_global,
            ),
        )
        self._route_announcer = RouteAnnouncer(
            self._log, self._emit_global, self._registry.degradation_reason
        )
        self._executors = ExecutorSupervision(
            ExecutorSupervisionDeps(
                runs=self._runs,
                templates=self._templates,
                registry=self._registry,
                clock=self._clock,
                database=self._database,
                data_root=self._options.data_root,
                log=self._log,
                shutdown=self._shutdown,
                abort_workers=self._options.abort_workers,
                emit_global=self._emit_global,
            )
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

    def join_executors(self, timeout_seconds: float, *, abort_grace_seconds: float = 0.0) -> None:
        """等待全部执行线程收尾；宽限到点仍未收尾的，先强制终止其 worker（docs/08）。

        机制在 supervisor_executors：宽限与强制终止的口径见 ``ExecutorSupervision.join``。

        Args:
            timeout_seconds: 全部线程共享的等待上限秒数。
            abort_grace_seconds: 收尾宽限秒数，超过才强制终止；0 表示不终止。
        """
        self._executors.join(timeout_seconds, abort_grace_seconds=abort_grace_seconds)

    def request_cancel(self, run_id: str, reason: str = "user_cancel") -> JobRunRecord:
        """记录叫停：未开始的立即 cancelled；执行中的置位标记，由 worker 在阶段边界收尾。"""
        record = self._runs.request_cancel(run_id, reason)
        self._executors.signal_cancel(run_id)
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
                self._executors.signal_cancel(run_id)
        finally:
            connection.close()

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
            if due_waiting and not self._executors.is_inflight(run.id):
                self._execute(run)
                return
            due_retry = run.state is RunState.RECOVERING and retry_due(run, now)
            if due_retry and not self._executors.is_inflight(run.id):
                self._execute(run)
                return

    def _queued_candidates(self, queued: tuple[JobRunRecord, ...]) -> tuple[RouteCandidate, ...]:
        """把排队运行映射成路由候选；模板缺失按空插件 id 交执行器按配置错误收尾。

        已有执行线程在推进的运行不进候选：同一段派发里不重复启动一次运行（docs/08）。
        """
        candidates: list[RouteCandidate] = []
        for run in queued:
            if self._executors.is_inflight(run.id):
                continue
            template = self._templates.get(run.template_id)
            plugin_id = template.plugin_id if template is not None else ""
            candidates.append(RouteCandidate(run=run, plugin_id=plugin_id))
        return tuple(candidates)

    def _execute(self, run: JobRunRecord) -> None:
        """为运行派一个执行线程：tick 只登记与启动，不等执行（docs/08 停机与阶段边界）。

        保温窗口来自运行所属计划；登记与线程推进在 supervisor_executors。
        """
        schedule = None if run.schedule_id is None else self._schedules.get(run.schedule_id)
        keep_warm = 0 if schedule is None else schedule.keep_warm_minutes
        self._executors.launch(run.id, keep_warm)

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


__all__ = ["Supervisor", "SupervisorOptions"]
