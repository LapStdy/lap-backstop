"""Supervisor 的派发路由：排队候选按插件降级实时取舍。

Supervisor 只保留 tick 的推进顺序与调度决策；降级跳过、全部降级的错误口径与
路由留痕归本模块（docs/05 多插件路由与降级）。降级原因每次路由实时读取注册表，
不缓存跨请求的通道状态。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final

from contracts import Logger
from storage.repositories.job_runs import JobRunRecord

# 一次路由最多查看的排队运行数；窗口内全是同一降级插件的运行时按现状派发队首。
ROUTE_WINDOW: Final = 8
# 全部候选插件都降级时的错误码；沿用既有插件故障约定（contracts/errors.py）。
ALL_DEGRADED_CODE: Final = "E_PLUGIN"
ROUTE_SKIPPED_EVENT: Final = "core.route.skipped"
ROUTE_ALL_DEGRADED_EVENT: Final = "core.route.all_degraded"


@dataclass(frozen=True, slots=True)
class RouteCandidate:
    """一个排队运行与其目标插件。"""

    run: JobRunRecord
    plugin_id: str


@dataclass(frozen=True, slots=True)
class RouteSkip:
    """因降级未参与本轮派发的候选。"""

    run_id: str
    plugin_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class RouteDecision:
    """一轮路由的结论：派发哪个候选、哪些候选被跳过、是否全部降级。"""

    chosen: RouteCandidate | None
    skipped: tuple[RouteSkip, ...]
    error_code: str | None


def route_dispatch(
    candidates: tuple[RouteCandidate, ...],
    degradation_of: Callable[[str], str | None],
) -> RouteDecision:
    """按出队顺序选第一个可用插件的候选；全部降级且不止一个插件时不派发。

    Args:
        candidates: 按出队顺序排列的排队候选。
        degradation_of: 插件 id 到降级原因的实时查询；None 表示可用。

    Returns:
        RouteDecision：chosen 为空且 error_code 非空表示全部降级，候选原地等待
        降级窗口过去；单插件降级时维持现状派发队首，交执行器按既有恢复路径收尾。
    """
    skips: list[RouteSkip] = []
    for candidate in candidates:
        reason = degradation_of(candidate.plugin_id)
        if reason is None:
            return RouteDecision(candidate, tuple(skips), None)
        skips.append(RouteSkip(candidate.run.id, candidate.plugin_id, reason))
    if len({candidate.plugin_id for candidate in candidates}) > 1:
        return RouteDecision(None, tuple(skips), ALL_DEGRADED_CODE)
    return RouteDecision(candidates[0], (), None)


class RouteAnnouncer:
    """路由留痕：每次决策写调试日志，降级参与情况按边沿写全局事件。"""

    def __init__(
        self,
        log: Logger,
        emit_global: Callable[[str, Mapping[str, Any]], None],
        degradation_of: Callable[[str], str | None],
    ) -> None:
        self._log = log
        self._emit_global = emit_global
        self._degradation_of = degradation_of
        self._reported: set[str] = set()

    def record(self, decision: RouteDecision) -> None:
        """留痕一次路由决策；同一插件的降级只在参与集变化时补写事件。"""
        self._log.debug(
            {
                "event": "supervisor.route",
                "chosen_run_id": decision.chosen.run.id if decision.chosen else "",
                "skipped_run_ids": [skip.run_id for skip in decision.skipped],
                "error_code": decision.error_code or "",
            }
        )
        current = {skip.plugin_id for skip in decision.skipped}
        for plugin_id in sorted(self._reported - current):
            # 插件仍在降级但本轮没参与路由时不解除武装；真恢复才允许再次上报。
            if self._degradation_of(plugin_id) is None:
                self._reported.discard(plugin_id)
        if not current - self._reported:
            return
        self._reported |= current - self._reported
        plugins = [
            {"run_id": skip.run_id, "plugin_id": skip.plugin_id, "reason": skip.reason}
            for skip in decision.skipped
        ]
        if decision.error_code is not None:
            self._emit_global(
                ROUTE_ALL_DEGRADED_EVENT,
                {"plugins": plugins, "error_code": decision.error_code},
            )
        elif decision.chosen is not None:
            self._emit_global(
                ROUTE_SKIPPED_EVENT,
                {
                    "plugins": plugins,
                    "chosen_run_id": decision.chosen.run.id,
                    "chosen_plugin_id": decision.chosen.plugin_id,
                },
            )


__all__ = [
    "ALL_DEGRADED_CODE",
    "ROUTE_ALL_DEGRADED_EVENT",
    "ROUTE_SKIPPED_EVENT",
    "ROUTE_WINDOW",
    "RouteAnnouncer",
    "RouteCandidate",
    "RouteDecision",
    "RouteSkip",
    "route_dispatch",
]
