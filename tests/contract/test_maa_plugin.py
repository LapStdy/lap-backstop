"""MAA 插件的 manifest 与配置 schema 契约（docs/plugins/maa.md）。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from contracts import Capability, Manifest
from contracts.manifest import range_covers

PLUGIN_DIR = Path(__file__).resolve().parents[2] / "plugins" / "maa"

# 真源是 scripts/maa_config.py 的 TASK_TYPES；MAA 更新白名单时两处同步。
TASK_TYPES = {
    "StartUpTask",
    "CloseDownTask",
    "FightTask",
    "OperProgressTask",
    "AwardTask",
    "MallTask",
    "InfrastTask",
    "RecruitTask",
    "RoguelikeTask",
    "CopilotTask",
    "SSSCopilotTask",
    "SingleStepTask",
    "DepotTask",
    "OperBoxTask",
    "UserDataUpdateTask",
    "ReclamationTask",
    "DepotMaintainTask",
    "SwitchThemeTask",
    "CustomTask",
}


def load_manifest() -> Manifest:
    """读取并校验 plugin.json；违反 manifest 规则时在解析里抛出。"""
    data = json.loads((PLUGIN_DIR / "plugin.json").read_text(encoding="utf-8"))
    return Manifest.from_dict(data)


def load_schema() -> dict[str, Any]:
    return json.loads((PLUGIN_DIR / "config.schema.json").read_text(encoding="utf-8"))


def test_manifest_loads_with_documented_identity() -> None:
    """docs/plugins/maa.md manifest 节：id、版本与展示名。"""
    manifest = load_manifest()
    assert manifest.id == "maa"
    assert manifest.version == "1.0.0"
    assert manifest.api_version == 1
    assert manifest.name == "MAA · 明日方舟"


def test_manifest_declares_four_capabilities_in_doc_order() -> None:
    """docs/plugins/maa.md manifest 节：四能力按文档顺序声明。"""
    assert load_manifest().capabilities == (
        Capability.ENVIRONMENT,
        Capability.SCRIPT,
        Capability.VERIFIER,
        Capability.CONFIG_PROJECTION,
    )


def test_manifest_contract_range_covers_api_version() -> None:
    """docs/05 manifest 节：api_version 必须落在 requires.contract 覆盖范围内。"""
    manifest = load_manifest()
    assert range_covers(manifest.requires.contract, manifest.api_version)


def test_config_schema_locks_documented_top_level_fields() -> None:
    """docs/plugins/maa.md 参数 schema 表：13 个配置字段、Draft 2020-12、根封闭。"""
    schema = load_schema()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["x-schema-version"] == 1
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == {
        "maa_dir",
        "mode",
        "mumu_dir",
        "adb_address",
        "profile",
        "tasks",
        "launch_timeout_s",
        "wake_timeout_s",
        "startup_timeout_s",
        "sample_interval_s",
        "keep_warm_minutes",
        "config_projection",
    }


def test_config_schema_defaults_match_plugin_doc() -> None:
    """docs/plugins/maa.md 参数 schema 表：默认值逐项一致。"""
    props = load_schema()["properties"]
    assert props["mode"]["default"] == "gui"
    assert props["maa_dir"]["default"] == "H:/MAA"
    assert props["mumu_dir"]["default"] == "H:/MuMuPlayer"
    assert props["adb_address"]["default"] == "127.0.0.1:16384"
    assert props["launch_timeout_s"]["default"] == 180
    assert props["wake_timeout_s"]["default"] == 90
    assert props["startup_timeout_s"]["default"] == 300
    assert props["sample_interval_s"]["default"] == 30
    assert props["keep_warm_minutes"]["default"] == 30
    projection = props["config_projection"]["properties"]
    assert projection["enabled"]["default"] is True
    assert projection["backup_keep"]["default"] == 20


def test_task_type_enum_follows_maa_config_whitelist() -> None:
    """docs/plugins/maa.md 配置投影节：任务 $type 白名单与 scripts/maa_config.py 同源。"""
    items = load_schema()["properties"]["tasks"]["items"]
    assert set(items["properties"]["type"]["enum"]) == set(TASK_TYPES)
    assert items["required"] == ["type"]
