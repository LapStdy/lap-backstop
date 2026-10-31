"""插件宿主：登记能力对象并按能力路由。

内核只拿到 contracts 里的 Protocol，不导入插件内部模块；注册表由 worker 侧的加载器填充。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from contracts import (
    Advisor,
    Capability,
    ConfigProjection,
    EnvironmentProvider,
    Notifier,
    ScriptAdapter,
    Verifier,
)
from core.plugins.errors import PluginCapabilityError, PluginNotFoundError

# 插件 id 到当前降级原因的实时查询；None 表示可用（docs/05 多插件路由与降级）。
DegradationReason = Callable[[str], str | None]


@dataclass(frozen=True, slots=True)
class PluginCapabilities:
    """一个插件声明的全部能力；没声明的能力为 None。"""

    plugin_id: str
    environment: EnvironmentProvider | None = None
    script: ScriptAdapter | None = None
    verifier: Verifier | None = None
    config_projection: ConfigProjection | None = None
    notifier: Notifier | None = None
    advisor: Advisor | None = None

    def declared(self) -> tuple[Capability, ...]:
        """已实现的能力名，按固定顺序。"""
        pairs = (
            (Capability.ENVIRONMENT, self.environment),
            (Capability.SCRIPT, self.script),
            (Capability.VERIFIER, self.verifier),
            (Capability.CONFIG_PROJECTION, self.config_projection),
            (Capability.NOTIFIER, self.notifier),
            (Capability.ADVISOR, self.advisor),
        )
        return tuple(name for name, value in pairs if value is not None)


class PluginRegistry:
    """内存中的插件能力表；加载时机与持久化由上层决定。"""

    def __init__(self, degradation: DegradationReason | None = None) -> None:
        self._plugins: dict[str, PluginCapabilities] = {}
        self._degradation = degradation

    def register(self, plugin: PluginCapabilities) -> None:
        """登记插件；id 重复时抛 ``PluginCapabilityError``。"""
        if plugin.plugin_id in self._plugins:
            raise PluginCapabilityError(f"插件已注册：{plugin.plugin_id}")
        self._plugins[plugin.plugin_id] = plugin

    def degradation_reason(self, plugin_id: str) -> str | None:
        """插件的当前降级原因；未降级或没有降级视图时返回 None。

        每次调用实时读取，不缓存跨请求的通道状态。
        """
        if self._degradation is None:
            return None
        return self._degradation(plugin_id)

    def get(self, plugin_id: str) -> PluginCapabilities:
        """取插件能力表；未注册抛 ``PluginNotFoundError``。"""
        try:
            return self._plugins[plugin_id]
        except KeyError as error:
            raise PluginNotFoundError(f"插件未注册：{plugin_id}") from error

    def plugin_ids(self) -> tuple[str, ...]:
        """已注册的插件 id，按字典序。"""
        return tuple(sorted(self._plugins))

    def environment(self, plugin_id: str) -> EnvironmentProvider:
        """取环境能力；未声明抛 ``PluginCapabilityError``。"""
        provider = self.get(plugin_id).environment
        if provider is None:
            raise PluginCapabilityError(f"插件 {plugin_id} 没有 environment 能力")
        return provider

    def script(self, plugin_id: str) -> ScriptAdapter:
        """取脚本能力；未声明抛 ``PluginCapabilityError``。"""
        adapter = self.get(plugin_id).script
        if adapter is None:
            raise PluginCapabilityError(f"插件 {plugin_id} 没有 script 能力")
        return adapter

    def verifier(self, plugin_id: str) -> Verifier:
        """取验证能力；未声明抛 ``PluginCapabilityError``。"""
        verifier = self.get(plugin_id).verifier
        if verifier is None:
            raise PluginCapabilityError(f"插件 {plugin_id} 没有 verifier 能力")
        return verifier
