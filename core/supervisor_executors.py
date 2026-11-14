"""Supervisor 的执行线程看护：登记派发、叫停置位、停机排水与宽限终止。

Supervisor 只保留 tick 的推进顺序与调度决策；执行线程的登记与注销、取消标记的
取放、收尾等待与强制终止归本模块（docs/08 停机与阶段边界、docs/11 并发）。执行
线程自开连接推进一次运行，意外异常在此兜底记账，不冒泡进 tick。
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from contracts import Clock, EventType, Logger
from core.jobs.errors import RunNotFoundError
from core.jobs.run_service import JobRunService
from core.plugins.registry import PluginRegistry
from core.resources.errors import ResourceBusyError
from core.supervisor_shutdown import ShutdownAbort
from core.worker.executor import CancelFlag, WorkerExecutor
from storage import open_database
from storage.repositories.events import EventLog
from storage.repositories.job_templates import JobTemplateRepository


@dataclass(frozen=True, slots=True)
class ExecutorSupervisionDeps:
    """执行线程看护的协作对象：运行与模板仓储、注册表、时钟、库路径与终止入口。"""

    runs: JobRunService
    templates: JobTemplateRepository
    registry: PluginRegistry
    clock: Clock
    database: Path
    data_root: Path
    log: Logger
    shutdown: Callable[[], bool]
    abort_workers: Callable[[Sequence[str]], None] | None
    emit_global: Callable[[str, Mapping[str, Any]], None]


class ExecutorSupervision:
    """执行线程的登记与收尾：派发启动、叫停置位、停机排水与宽限终止。"""

    def __init__(self, deps: ExecutorSupervisionDeps) -> None:
        self._runs = deps.runs
        self._templates = deps.templates
        self._registry = deps.registry
        self._clock = deps.clock
        self._database = deps.database
        self._data_root = deps.data_root
        self._log = deps.log
        self._shutdown = deps.shutdown
        self._emit_global = deps.emit_global
        # 执行线程的登记与取消标记共用一把锁：登记随派发建立、随线程收尾注销，
        # tick 查重、请求线程置位、停机收尾 join 互不交错。
        self._executors_lock = threading.Lock()
        self._cancel_flags: dict[str, CancelFlag] = {}
        self._executor_threads: dict[str, threading.Thread] = {}
        # 宽限到点的强制终止件：serve 装配时传宿主的 abort_workers 作终止入口；
        # 未注入或非停机退场时，宽限到点只继续等待，不碰 worker 进程。
        self._shutdown_abort = ShutdownAbort(
            log=deps.log,
            resolve_plugin=self._plugin_of,
            abort=deps.abort_workers,
            emit=deps.emit_global,
        )

    def launch(self, run_id: str, keep_warm: int) -> None:
        """为运行登记并派一个执行线程：tick 只登记与启动，不等执行（docs/08）。

        Raises:
            RuntimeError: 线程无法启动，属程序错误，交上层按退出处置。
        """
        cancel = self._cancel_for(run_id)
        # 叫停先落库再置位：标记不存在期间收到的请求，靠事件在这里补上。
        if self._runs.cancel_requested(run_id):
            cancel.cancel()
        self._spawn_executor(run_id, keep_warm, cancel)

    def join(self, timeout_seconds: float, *, abort_grace_seconds: float = 0.0) -> None:
        """等待全部执行线程收尾；宽限到点仍未收尾的，先强制终止其 worker（docs/08）。

        serve 循环退出前的收口点：线程各自持有连接与租约收尾口径，这里只等与终止
        worker 进程，不动数据库里的运行状态。宽限期内自己收尾的线程不碰 worker；
        强制终止只在停机收尾路径发生（磁盘退场与未注入终止入口时不动作），终止有账：
        一条日志与一条全局事件。随后继续等满剩余收尾时间，仍卡死的线程不再等待，
        运行状态与租约留给下次启动回收。

        Args:
            timeout_seconds: 全部线程共享的等待上限秒数。
            abort_grace_seconds: 收尾宽限秒数，超过才强制终止；0 表示不终止。
        """
        deadline = self._clock.monotonic() + timeout_seconds
        with self._executors_lock:
            threads = tuple(self._executor_threads.values())
        self._join_until(threads, min(deadline, self._clock.monotonic() + abort_grace_seconds))
        if abort_grace_seconds > 0:
            self._force_abort(self._alive_run_ids())
        self._join_until(threads, deadline)
        stuck = self._alive_run_ids()
        if not stuck:
            return
        self._log.error({"event": "supervisor.executor_join_timeout", "run_ids": stuck})
        self._emit_global("supervisor.executor_join_timeout", {"run_ids": stuck})

    def signal_cancel(self, run_id: str) -> None:
        """置位该运行的取消标记；标记不存在（waiting、两次 tick 之间）时不动。"""
        with self._executors_lock:
            flag = self._cancel_flags.get(run_id)
            if flag is not None:
                flag.cancel()

    def is_inflight(self, run_id: str) -> bool:
        """该运行是否已有执行线程在推进；登记在派发时建立、线程收尾时注销。"""
        with self._executors_lock:
            return run_id in self._executor_threads

    def _join_until(self, threads: tuple[threading.Thread, ...], deadline: float) -> None:
        """按绝对单调时刻等线程收尾；时刻已过立即返回。"""
        for thread in threads:
            thread.join(max(0.0, deadline - self._clock.monotonic()))

    def _alive_run_ids(self) -> list[str]:
        """仍在推进的运行 id，按 id 排序；登记在派发时建立、线程收尾时注销。"""
        with self._executors_lock:
            return sorted(
                run_id for run_id, thread in self._executor_threads.items() if thread.is_alive()
            )

    def _force_abort(self, stuck_run_ids: Sequence[str]) -> None:
        """强制终止承载在途调用的 worker，让卡死的调用立即失败返回。

        只在停机收尾路径发生：非停机的退场不终止 worker（docs/08）；解析、终止与
        记账在 supervisor_shutdown。运行状态、租约与收尾事件都归执行线程，这里不碰。
        """
        if not stuck_run_ids or not self._shutdown():
            return
        self._shutdown_abort.abort(stuck_run_ids)

    def _plugin_of(self, run_id: str) -> str | None:
        """卡死运行对应的插件 id；库故障或模板缺失按解不出处置，终止尽力而为。"""
        try:
            run = self._runs.get(run_id)
        except (RunNotFoundError, sqlite3.Error):
            return None
        try:
            template = self._templates.get(run.template_id)
        except sqlite3.Error:
            return None
        return None if template is None else template.plugin_id

    def _cancel_for(self, run_id: str) -> CancelFlag:
        """取该运行的取消标记；执行期间同一运行复用同一个对象。"""
        with self._executors_lock:
            return self._cancel_flags.setdefault(run_id, CancelFlag())

    def _drop_cancel(self, run_id: str) -> None:
        """执行收尾后清走标记；之后再来的叫停靠 run_events 在下一轮生效。"""
        with self._executors_lock:
            self._cancel_flags.pop(run_id, None)

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


__all__ = ["ExecutorSupervision", "ExecutorSupervisionDeps"]
