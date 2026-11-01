"""按目录加载插件：先校验 manifest，再导入 ``plugins/<id>/plugin.py`` 导出的 ``PLUGIN``。

加载只发生在 worker 子进程（ADR 0010）；内核进程不执行插件模块的导入。
manifest 校验口径见 docs/05：结构缺失拒绝用 E_PLUGIN_MANIFEST，插件声明的契约
范围不覆盖内核版本拒绝用 E_PLUGIN_VERSION；任一失败入口模块都不导入。
"""

from __future__ import annotations

import importlib.util
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from contracts.errors import ContractError, ErrorCode
from contracts.manifest import KERNEL_CONTRACT_VERSION, Manifest, kernel_rejects
from core.plugins.errors import PluginContractError, PluginError


def load_plugin(plugins_root: Path, plugin_id: str) -> object:
    """加载插件入口对象；manifest 校验通过后导入入口模块。

    Raises:
        PluginContractError: manifest 读不到、不是合法 JSON、字面规则不通过、
            id 与目录名不一致，或 requires.contract 不覆盖内核契约版本。
        PluginError: 入口文件不存在、模块无法执行或没有 ``PLUGIN``。
    """
    _load_manifest(plugins_root, plugin_id)
    return _import_entry(plugins_root, plugin_id)


def _load_manifest(plugins_root: Path, plugin_id: str) -> Manifest:
    """按 docs/05 的顺序校验 manifest：可解析 → 字面规则 → id 一致 → 契约版本覆盖。"""
    manifest = _validated_manifest(_read_manifest(plugins_root, plugin_id), plugin_id)
    _check_identity(manifest, plugin_id)
    _check_contract_version(manifest, plugin_id)
    return manifest


def _read_manifest(plugins_root: Path, plugin_id: str) -> Mapping[str, Any]:
    """读 plugin.json 并解析成对象；文件读不到与 JSON 非法分开报告，方便排障。"""
    path = plugins_root / plugin_id / "plugin.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise _rejection(
            ErrorCode.PLUGIN_MANIFEST, plugin_id, f"manifest 读不到：{error}"
        ) from error
    except ValueError as error:
        raise _rejection(
            ErrorCode.PLUGIN_MANIFEST, plugin_id, f"manifest 不是合法 JSON：{error}"
        ) from error
    if not isinstance(data, Mapping):
        raise _rejection(ErrorCode.PLUGIN_MANIFEST, plugin_id, "manifest 根必须是对象")
    return data


def _validated_manifest(data: Mapping[str, Any], plugin_id: str) -> Manifest:
    """字面规则与自洽校验；契约包的失败消息带上插件名，内核日志能定位到插件。"""
    try:
        return Manifest.from_dict(data)
    except ContractError as error:
        raise _rejection(error.code, plugin_id, error.message) from error


def _check_identity(manifest: Manifest, plugin_id: str) -> None:
    """manifest id 必须与目录名一致；不一致会让调用与事件归属到另一个身份。"""
    if manifest.id != plugin_id:
        raise _rejection(
            ErrorCode.PLUGIN_MANIFEST,
            plugin_id,
            f"manifest id 是 {manifest.id!r}，与目录名不一致",
        )


def _check_contract_version(manifest: Manifest, plugin_id: str) -> None:
    """插件声明的契约范围必须覆盖内核支持的版本，否则按版本不兼容拒绝。"""
    if kernel_rejects(manifest, KERNEL_CONTRACT_VERSION):
        raise PluginContractError(
            ErrorCode.PLUGIN_VERSION,
            f"插件 {plugin_id} 的 requires.contract={manifest.requires.contract!r} "
            f"不覆盖内核契约版本 {KERNEL_CONTRACT_VERSION}",
        )


def _rejection(code: ErrorCode, plugin_id: str, reason: str) -> PluginContractError:
    """把一次拒绝折成契约错误，消息统一以插件名开头。"""
    return PluginContractError(code, f"插件 {plugin_id}：{reason}")


def _import_entry(plugins_root: Path, plugin_id: str) -> object:
    """导入入口模块并取 PLUGIN；缺失、无法执行或未导出都按 PluginError 拒绝。"""
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
