"""worker 执行器的配置投影：开跑前按插件配置构造整份目标并下发。

desired → apply → config.applied 的时序在这里收口；apply 失败先按合法修订
回滚一次并记事件，再交现有恢复策略处置（docs/03 配置投影）。通道异常与
ok=false 走同一收口，不把异常抛给 supervisor。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from contracts import ApplyResult, ConfigProjection, EventType, RunContext
from core.plugins.errors import PluginError
from core.worker.run_scope import RunScope
from storage.repositories.job_runs import JobRunRecord

if TYPE_CHECKING:
    from core.plugins.registry import PluginRegistry
    from core.worker.executor import ExecutorServices
    from core.worker.executor_settlement import RunSettlement


class ConfigProjector:
    """运行开跑前的配置投影：desired → apply，失败回滚一次再交恢复策略。"""

    def __init__(
        self,
        registry: PluginRegistry,
        services: ExecutorServices,
        settlement: RunSettlement,
    ) -> None:
        self._registry = registry
        self._events = services.events
        self._clock = services.clock
        self._log = services.log
        self._settlement = settlement

    def project(self, scope: RunScope) -> JobRunRecord | None:
        """开跑前按插件配置构造整份目标并下发；返回 None 表示继续开跑。

        没有投影能力的插件或开关关闭时跳过；通道异常与 ok=false 走同一收口，
        不把异常抛给 supervisor。
        """
        projection = self._registry.get(scope.template.plugin_id).config_projection
        if projection is None or _projection_disabled(scope.context.config):
            return None
        try:
            target = projection.desired(scope.context, scope.context.config)
            result = projection.apply(scope.context, target)
        except PluginError as error:  # 通道超时与取消同属一族，错误码随通道走
            return self._projection_failed(scope, projection, error.code, None)
        if result.ok:
            self._emit_config_applied(scope, result)
            return None
        return self._projection_failed(
            scope, projection, result.error_code or "E_CONFIG", _revision_in(result.meta)
        )

    def _projection_failed(
        self,
        scope: RunScope,
        projection: ConfigProjection,
        error_code: str,
        revision: str | None,
    ) -> JobRunRecord:
        """投影失败收口：有合法修订先回滚一次并记事件，再交现有恢复策略处置。

        收口错误码始终取本次 apply 的码：回滚调用再遇通道异常时转成失败回执，
        不让回滚的码覆盖 apply 的码。
        """
        if revision is not None:
            rolled = _rollback_once(projection, scope.context, revision)
            self._log.warn(
                {
                    "event": "executor.config_projection_failed",
                    "run_id": scope.run_id,
                    "error_code": error_code,
                    "revision": revision,
                    "rolled_back": rolled.ok,
                }
            )
            self._events.append_run(
                scope.run_id,
                EventType.CONFIG_ROLLED_BACK,
                {
                    "run_id": scope.run_id,
                    "revision": revision,
                    "ok": rolled.ok,
                    "error_code": rolled.error_code,
                },
                at=self._clock.utc_now(),
            )
        return self._settlement.recover(scope, error_code, "config_projection")

    def _emit_config_applied(self, scope: RunScope, result: ApplyResult) -> None:
        """apply 成功记 config.applied；修订与哈希取插件回执，未写盘时为空。"""
        self._events.append_run(
            scope.run_id,
            EventType.CONFIG_APPLIED,
            {
                "run_id": scope.run_id,
                "plugin_id": scope.template.plugin_id,
                "revision": result.meta.get("revision"),
                "sha256": result.meta.get("sha256"),
                "changed": bool(result.meta.get("changed")),
            },
            at=self._clock.utc_now(),
        )


__all__ = ["ConfigProjector"]


def _projection_disabled(config: Mapping[str, Any]) -> bool:
    """插件配置的 config_projection.enabled 为 false 时投影关闭；缺省开启。"""
    settings = config.get("config_projection")
    return isinstance(settings, Mapping) and settings.get("enabled") is False


def _rollback_once(
    projection: ConfigProjection,
    ctx: RunContext,
    revision: str,
) -> ApplyResult:
    """回滚一次；通道异常转成失败回执，收口错误码仍由调用方按 apply 的码定。"""
    try:
        return projection.rollback(ctx, revision)
    except PluginError as error:
        return ApplyResult(ok=False, error_code=error.code, detail=f"回滚调用失败：{error}")


def _revision_in(meta: Mapping[str, Any]) -> str | None:
    """apply 回执里的修订号；非空字符串才算合法，插件私有格式由插件自校。"""
    revision = meta.get("revision")
    return revision if isinstance(revision, str) and revision else None
