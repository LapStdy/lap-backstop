"""按目录加载插件入口：``plugins/<id>/plugin.py`` 导出 ``PLUGIN``。

加载只发生在 worker 子进程（ADR 0010）；内核进程不执行插件模块的导入。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from core.plugins.errors import PluginError


def load_plugin(plugins_root: Path, plugin_id: str) -> object:
    """加载插件入口对象；入口缺失、无法导入或未导出 ``PLUGIN`` 时抛 ``PluginError``。

    Raises:
        PluginError: 入口文件不存在、模块无法执行或没有 ``PLUGIN``。
    """
    entry = plugins_root / plugin_id / "plugin.py"
    if not entry.is_file():
        raise PluginError(f"插件入口不存在：{entry}")
    module_name = f"lap_backstop_plugin_{plugin_id}"
    spec = importlib.util.spec_from_file_location(module_name, entry)
    if spec is None or spec.loader is None:
        raise PluginError(f"无法加载插件入口：{entry}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as error:
        sys.modules.pop(module_name, None)
        raise PluginError(f"插件入口执行失败：{entry}：{error}") from error
    plugin = getattr(module, "PLUGIN", None)
    if plugin is None:
        raise PluginError(f"插件入口没有导出 PLUGIN：{entry}")
    return plugin
