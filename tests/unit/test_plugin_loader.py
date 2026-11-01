"""单元：插件加载的 manifest 校验。

结构缺失以 E_PLUGIN_MANIFEST 拒绝，契约版本不覆盖内核以 E_PLUGIN_VERSION 拒绝；
校验失败时插件入口不导入（docs/05 加载校验）。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from core.plugins.channel import WorkerChannel
from core.plugins.errors import PluginContractError, PluginError, WorkerCrashError
from core.plugins.loader import load_plugin
from core.plugins.worker_options import WorkerConfig, WorkerLimits

VALID_MANIFEST: dict[str, Any] = {
    "id": "sample",
    "name": "sample · 样例插件",
    "version": "1.0.0",
    "api_version": 1,
    "capabilities": ["environment", "script", "verifier"],
    "config_schema": "config.schema.json",
    "requires": {"contract": ">=1 <2", "platform": "windows"},
}

_PLUGIN_BODY = "PLUGIN = object()\n"


def _make_plugin(
    plugins_root: Path,
    plugin_id: str,
    manifest: Mapping[str, Any] | str = VALID_MANIFEST,
    body: str = _PLUGIN_BODY,
) -> None:
    """在临时插件根下落一个插件目录；manifest 传字符串时原样写入。"""
    directory = plugins_root / plugin_id
    directory.mkdir(parents=True)
    text = manifest if isinstance(manifest, str) else json.dumps(manifest, ensure_ascii=False)
    (directory / "plugin.json").write_text(text, encoding="utf-8")
    (directory / "plugin.py").write_text(body, encoding="utf-8")


def test_load_plugin_with_valid_manifest_returns_entry_object(tmp_path: Path) -> None:
    """合法插件正常加载：返回入口导出的 PLUGIN 对象。"""
    _make_plugin(tmp_path, "sample")
    plugin = load_plugin(tmp_path, "sample")
    assert plugin is not None


def test_load_plugin_rejects_contract_range_not_covering_kernel(tmp_path: Path) -> None:
    """requires.contract 不覆盖内核契约版本：按 E_PLUGIN_VERSION 拒绝，文案指向版本。"""
    manifest = {
        **VALID_MANIFEST,
        "api_version": 2,
        "requires": {"contract": ">=2 <3", "platform": "windows"},
    }
    _make_plugin(tmp_path, "sample", manifest)
    with pytest.raises(PluginContractError) as rejected:
        load_plugin(tmp_path, "sample")
    assert rejected.value.code == "E_PLUGIN_VERSION"
    message = str(rejected.value)
    assert "sample" in message
    assert "内核契约版本" in message


def test_load_plugin_rejects_empty_capability_set(tmp_path: Path) -> None:
    """capabilities 为空：按 E_PLUGIN_MANIFEST 拒绝，文案点名字段。"""
    manifest = {**VALID_MANIFEST, "capabilities": []}
    _make_plugin(tmp_path, "sample", manifest)
    with pytest.raises(PluginContractError) as rejected:
        load_plugin(tmp_path, "sample")
    assert rejected.value.code == "E_PLUGIN_MANIFEST"
    assert "capabilities" in str(rejected.value)


def test_load_plugin_rejects_unknown_capability_with_name(tmp_path: Path) -> None:
    """capabilities 含未知能力：拒绝并指出具体能力名。"""
    manifest = {**VALID_MANIFEST, "capabilities": ["environment", "telepathy"]}
    _make_plugin(tmp_path, "sample", manifest)
    with pytest.raises(PluginContractError) as rejected:
        load_plugin(tmp_path, "sample")
    assert rejected.value.code == "E_PLUGIN_MANIFEST"
    assert "telepathy" in str(rejected.value)


@pytest.mark.parametrize(
    "schema_path",
    ["../config.schema.json", "C:/pkg/config.schema.json", ""],
)
def test_load_plugin_rejects_illegal_config_schema_shape(
    tmp_path: Path,
    schema_path: str,
) -> None:
    """config_schema 不是包内相对路径：按 E_PLUGIN_MANIFEST 拒绝。"""
    manifest = {**VALID_MANIFEST, "config_schema": schema_path}
    _make_plugin(tmp_path, "sample", manifest)
    with pytest.raises(PluginContractError) as rejected:
        load_plugin(tmp_path, "sample")
    assert rejected.value.code == "E_PLUGIN_MANIFEST"
    assert "config_schema" in str(rejected.value)


def test_load_plugin_rejects_missing_required_field_with_field_name(tmp_path: Path) -> None:
    """manifest 缺必填字段：按 E_PLUGIN_MANIFEST 拒绝，文案含字段名，不指向版本。"""
    manifest = {key: value for key, value in VALID_MANIFEST.items() if key != "api_version"}
    _make_plugin(tmp_path, "sample", manifest)
    with pytest.raises(PluginContractError) as rejected:
        load_plugin(tmp_path, "sample")
    assert rejected.value.code == "E_PLUGIN_MANIFEST"
    message = str(rejected.value)
    assert "api_version" in message
    assert "E_PLUGIN_VERSION" not in message


def test_load_plugin_rejects_unparseable_manifest_json(tmp_path: Path) -> None:
    """plugin.json 不是合法 JSON：按 E_PLUGIN_MANIFEST 拒绝。"""
    _make_plugin(tmp_path, "sample", "{")
    with pytest.raises(PluginContractError) as rejected:
        load_plugin(tmp_path, "sample")
    assert rejected.value.code == "E_PLUGIN_MANIFEST"


def test_load_plugin_rejects_manifest_id_mismatching_directory(tmp_path: Path) -> None:
    """manifest id 与目录名不一致：按 E_PLUGIN_MANIFEST 拒绝，文案含两个 id。"""
    manifest = {**VALID_MANIFEST, "id": "other"}
    _make_plugin(tmp_path, "sample", manifest)
    with pytest.raises(PluginContractError) as rejected:
        load_plugin(tmp_path, "sample")
    assert rejected.value.code == "E_PLUGIN_MANIFEST"
    message = str(rejected.value)
    assert "other" in message
    assert "sample" in message


def test_load_plugin_reports_missing_export_as_structure_problem(tmp_path: Path) -> None:
    """plugin.py 缺 PLUGIN 导出：错误文案是结构语义，不声称版本不兼容。"""
    _make_plugin(tmp_path, "sample", body="")
    with pytest.raises(PluginError) as rejected:
        load_plugin(tmp_path, "sample")
    message = str(rejected.value)
    assert "PLUGIN" in message
    assert "E_PLUGIN_VERSION" not in message


def test_worker_process_reports_version_rejection_with_code(tmp_path: Path) -> None:
    """真子进程链路：manifest 版本校验失败经 FAULT 回内核，文案带 E_PLUGIN_VERSION。"""
    plugins_root = tmp_path / "plugins"
    plugins_root.mkdir()
    manifest = {
        **VALID_MANIFEST,
        "id": "late",
        "api_version": 2,
        "requires": {"contract": ">=2 <3", "platform": "windows"},
    }
    _make_plugin(plugins_root, "late", manifest)
    config = WorkerConfig(
        plugins_root=plugins_root,
        limits=WorkerLimits(
            call_timeout_seconds=2.0,
            start_timeout_seconds=10.0,
            cancel_grace_seconds=1.0,
            max_restarts=2,
            degraded_retry_seconds=1.0,
        ),
    )
    channel = WorkerChannel("late", config)
    try:
        with pytest.raises(WorkerCrashError) as rejected:
            channel.start()
    finally:
        channel.close()
    assert "E_PLUGIN_VERSION" in str(rejected.value)
