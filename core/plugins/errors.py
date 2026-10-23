"""插件宿主错误：插件未注册或缺少内核需要的能力。"""

from __future__ import annotations


class PluginError(ValueError):
    """插件领域的可预期错误。"""

    code: str = "E_PLUGIN"


class PluginNotFoundError(PluginError):
    """插件没有注册。"""

    code = "E_NOT_FOUND"


class PluginCapabilityError(PluginError):
    """插件未声明该能力，或能力对象缺失。"""

    code = "E_PLUGIN"
