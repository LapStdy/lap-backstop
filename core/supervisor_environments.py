"""Supervisor 的环境动作：启动探测、到期保温关闭与手动预热/复位。

Supervisor 只保留 tick 的推进顺序与调度决策；无运行归属的环境动作、
给插件的最小上下文与证据拒绝归本模块（docs/09）。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from contracts import (
    Clock,
    EnvironmentRef,
    Logger,
    PhaseResult,
    RunContext,
)
from core.environments.service import EnvironmentCall, EnvironmentService
from core.plugins.registry import PluginRegistry
from core.worker.executor import CancelFlag
from storage.repositories.environments import EnvironmentRow


class EnvironmentSupervision:
    """无运行归属的环境动作：启动探测、保温到期关闭与手动预热/复位。"""

    def __init__(
        self,
        environments: EnvironmentService,
        registry: PluginRegistry,
        clock: Clock,
        *,
        data_root: Path,
        log: Logger,
        emit_global: Callable[[str, Mapping[str, Any]], None],
    ) -> None:
        self._environments = environments
        self._registry = registry
        self._clock = clock
        self._data_root = data_root
        self._log = log
        self._emit_global = emit_global

    def prepare(self, environment_id: str) -> PhaseResult:
        """手动预热：与 tick 调度无关，不建运行、不改运行状态（docs/09 预热）。

        Raises:
            EnvironmentNotFoundError: 环境不存在。
            EnvironmentStateError: 状态机拒绝，如环境 busy。
            PluginError: provider 缺失或未声明 environment 能力。
        """
        row = self._environments.get(environment_id)
        return self._environments.prepare(self._environment_call(row, "prepare"))

    def reset(self, environment_id: str) -> PhaseResult:
        """手动复位：任何状态先回 booting，成功后仍等探针确认 ready。

        Raises:
            EnvironmentNotFoundError: 环境不存在。
            PluginError: provider 缺失或未声明 environment 能力。
        """
        row = self._environments.get(environment_id)
        return self._environments.reset(self._environment_call(row, "reset"))

    def probe_environments(self) -> None:
        """启动时对每个环境各探一次，探针结果按状态机迁移。

        崩溃与唤醒都会让持久状态脱离现实，off 与 ready 也不能当作还成立；探针与
        当前状态相同时只刷新观测时间。只挂在启动扫描上，tick 不探测；探后仍
        unknown 的等下一次启动再判，本轮不补探。
        """
        now = self._clock.utc_now()
        for row in self._environments.list_all():
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

    def close_warm(self, now: datetime) -> None:
        """关闭保温窗口到期的环境，由 tick 每轮调用。"""
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

    def _environment_call(self, row: EnvironmentRow, action: str) -> EnvironmentCall:
        """环境动作的调用件：provider 按环境行解析，上下文无运行归属。"""
        return EnvironmentCall(
            environment_id=row.id,
            provider=self._registry.environment(row.provider_id),
            ref=EnvironmentRef(id=row.id, provider_id=row.provider_id),
            ctx=self._standalone_context(row, self._clock.utc_now(), action),
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


def _reject_evidence(kind: str, path: Path, summary: str) -> str:
    """拒绝证据写入：无运行归属的动作没有可挂靠的运行与证据目录。"""
    raise ValueError("无运行归属的内核动作不接收证据")


__all__ = ["EnvironmentSupervision"]
