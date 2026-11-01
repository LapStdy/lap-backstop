"""停机收尾的强制终止件：结束承载卡死调用的 worker 进程树并落账。

时机与「是否停机」的判断在 ``Supervisor.join_executors``（docs/08 停机与阶段边界）；
这里只做动作：解析卡死运行对应的插件、调用注入的终止入口、落一条 ERROR 日志与一条
全局事件 ``serve.shutdown_force_abort``。解不出插件的运行跳过终止，仍在账目里留
run_id，等待上限兜底。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from contracts import Logger
from core.logging import NullLogger

SHUTDOWN_FORCE_ABORT_EVENT = "serve.shutdown_force_abort"

ResolvePlugin = Callable[[str], str | None]
AbortWorkers = Callable[[Sequence[str]], None]
EmitGlobal = Callable[[str, Mapping[str, Any]], None]


class ShutdownAbort:
    """强制终止的执行件：插件解析、worker 终止与日志事件记账在此收口。"""

    def __init__(
        self,
        *,
        log: Logger | None,
        resolve_plugin: ResolvePlugin,
        abort: AbortWorkers | None,
        emit: EmitGlobal,
    ) -> None:
        self._log: Logger = NullLogger() if log is None else log
        self._resolve_plugin = resolve_plugin
        self._abort = abort
        self._emit = emit

    def abort(self, stuck_run_ids: Sequence[str]) -> None:
        """终止承载卡死调用的 worker 并记账；未注入终止入口或无卡死运行时不动作。

        插件按运行顺序去重；解不出插件的运行（模板已删、库故障）跳过终止，run_id
        仍进账目，总等待上限兜底。
        """
        if not stuck_run_ids or self._abort is None:
            return
        plugins: list[str] = []
        for run_id in stuck_run_ids:
            plugin_id = self._resolve_plugin(run_id)
            if plugin_id is not None and plugin_id not in plugins:
                plugins.append(plugin_id)
        self._abort(plugins)
        payload = {"plugin_ids": plugins, "run_ids": list(stuck_run_ids)}
        self._log.error({"event": SHUTDOWN_FORCE_ABORT_EVENT, **payload})
        self._emit(SHUTDOWN_FORCE_ABORT_EVENT, payload)


__all__ = ["SHUTDOWN_FORCE_ABORT_EVENT", "ShutdownAbort"]
