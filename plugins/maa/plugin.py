"""MAA 插件入口：导出 PLUGIN，装配 manifest 与四个能力（docs/05）。

插件只依赖 contracts；包内 capabilities/ 模块不被内核导入，能力经 worker
子进程按 Protocol 调用。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from contracts import (
    ConfigProjection,
    EnvironmentProvider,
    Manifest,
    ScriptAdapter,
    Verifier,
)
from plugins.maa.capabilities.config_projection import MaaConfigProjection
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.capabilities.script import MaaScript
from plugins.maa.capabilities.verifier import MaaVerifier

_MANIFEST_PATH = Path(__file__).with_name("plugin.json")


@dataclass(frozen=True, slots=True)
class MaaPlugin:
    """插件入口对象：manifest 与按能力分组的实现。"""

    manifest: Manifest
    environment: EnvironmentProvider
    script: ScriptAdapter
    verifier: Verifier
    config_projection: ConfigProjection


def load_manifest(path: Path = _MANIFEST_PATH) -> Manifest:
    """读取并校验 plugin.json。"""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("plugin.json 根必须是对象")
    return Manifest.from_dict(data)


PLUGIN = MaaPlugin(
    manifest=load_manifest(),
    environment=MaaEnvironment(),
    script=MaaScript(),
    verifier=MaaVerifier(),
    config_projection=MaaConfigProjection(),
)
