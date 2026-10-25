"""MAA 插件的 manifest 与配置 schema 契约（docs/plugins/maa.md）。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import (
    Capability,
    ConfigDiffKind,
    ConfigProjection,
    EnvironmentProbe,
    EnvironmentProvider,
    EnvironmentRef,
    EnvironmentState,
    RecoveryAction,
    RunContext,
    ScriptAdapter,
    ScriptHandle,
    ScriptRef,
    ScriptState,
    StopMode,
    VerdictStatus,
    Verifier,
)
from contracts.manifest import range_covers
from plugins.maa.capabilities.config_projection import MaaConfigProjection
from plugins.maa.capabilities.environment import MaaEnvironment
from plugins.maa.capabilities.script import MaaScript
from plugins.maa.capabilities.verifier import MaaVerifier
from plugins.maa.plugin import PLUGIN, load_manifest

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


NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


class _Clock:
    """虚拟时钟：时间恒定，测试不读系统时间。"""

    def monotonic(self) -> float:
        return 0.0

    def utc_now(self) -> datetime:
        return NOW


class _CancelFlag:
    """取消令牌假件：永不取消。"""

    def is_cancelled(self) -> bool:
        return False


class _Logger:
    """结构化日志假件：丢弃全部输出。"""

    def debug(self, fields: Mapping[str, Any]) -> None:
        return None

    def info(self, fields: Mapping[str, Any]) -> None:
        return None

    def warn(self, fields: Mapping[str, Any]) -> None:
        return None

    def error(self, fields: Mapping[str, Any]) -> None:
        return None


def make_context(
    config: Mapping[str, Any] | None = None,
    evidence_dir: Path | None = None,
) -> RunContext:
    """构造契约测试用的 RunContext；证据 id 恒为 evidence-1。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={} if config is None else dict(config),
        work_dir=Path("."),
        evidence_dir=Path(".") if evidence_dir is None else evidence_dir,
        deadline_at=NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=_Logger(),
        emit=lambda event, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


def env_ref() -> EnvironmentRef:
    return EnvironmentRef(id="env-1", provider_id="maa")


def script_ref() -> ScriptRef:
    return ScriptRef(id="script-1", plugin_id="maa")


def test_plugin_exports_capabilities_matching_protocols() -> None:
    """docs/05 契约测试表 manifest 行：声明的能力与实现一致。"""
    assert isinstance(PLUGIN.environment, EnvironmentProvider)
    assert isinstance(PLUGIN.script, ScriptAdapter)
    assert isinstance(PLUGIN.verifier, Verifier)
    assert isinstance(PLUGIN.config_projection, ConfigProjection)
    assert PLUGIN.manifest.id == "maa"


def test_probe_returns_legal_state_with_clock_timestamp() -> None:
    """docs/05 契约测试表 environment 行：probe 返回合法状态，时间来自注入时钟。"""
    probe = MaaEnvironment().probe(env_ref(), make_context())
    assert isinstance(probe, EnvironmentProbe)
    assert probe.state is EnvironmentState.UNKNOWN
    assert probe.observed_at == NOW


def test_environment_actions_are_repeatable_and_fail_closed() -> None:
    """docs/05 契约测试表 environment 行：动作可重复；失败关闭且无副作用。"""
    provider = MaaEnvironment()
    ctx = make_context()
    ref = env_ref()
    for _ in range(2):
        for result in (
            provider.prepare(ref, ctx),
            provider.reset(ref, ctx),
            provider.cleanup(ref, ctx, keep_warm=False),
            provider.warm(ref, ctx, minutes=30),
        ):
            assert result.ok is False
            assert result.error_code == "E_PLUGIN"
            assert result.started_at == NOW
            assert result.ended_at == NOW


def test_launch_returns_handle_and_observe_is_repeatable() -> None:
    """docs/05 契约测试表 script 行：launch 返回句柄；observe 只读且可重复。"""
    adapter = MaaScript()
    ctx = make_context()
    handle = adapter.launch(script_ref(), ctx)
    assert isinstance(handle, ScriptHandle)
    assert handle.pid is None
    assert handle.meta == {"launched": False}
    first = adapter.observe(handle, ctx)
    second = adapter.observe(handle, ctx)
    assert first.state is ScriptState.EXITED
    assert first.signals == ()
    assert first == second


def test_stop_is_idempotent_and_recover_fails_closed() -> None:
    """docs/05 契约测试表 script 行：stop 可重复；recover 对四种内核动作都不执行。"""
    adapter = MaaScript()
    ctx = make_context()
    handle = adapter.launch(script_ref(), ctx)
    assert adapter.stop(handle, ctx, StopMode.GRACEFUL).ok
    assert adapter.stop(handle, ctx, StopMode.FORCE).ok
    for action in RecoveryAction:
        result = adapter.recover(handle, ctx, action)
        assert result.ok is False
        assert result.error_code == "E_PLUGIN"


def test_verifier_returns_unknown_with_evidence_stably(tmp_path: Path) -> None:
    """docs/05 契约测试表 verifier 行：三态之一、evidence_ids 非空、同输入结果稳定。"""
    verifier = MaaVerifier()
    first = verifier.verify(make_context(evidence_dir=tmp_path))
    second = verifier.verify(make_context(evidence_dir=tmp_path))
    with_policy = verifier.verify(make_context({"policy_id": "tpl-1"}, evidence_dir=tmp_path))
    assert first.status is VerdictStatus.UNKNOWN
    assert first.evidence_ids
    assert first.status == second.status
    assert first.policy_id == second.policy_id == "maa-policy"
    assert with_policy.policy_id == "tpl-1"
    assert (tmp_path / "verify.log").is_file()


def test_config_projection_diff_reports_nested_changes_in_order() -> None:
    """docs/05 契约测试表 config_projection 行：diff 逐字段、按键名稳定排序。"""
    projection = MaaConfigProjection()
    ctx = make_context()
    desired = {"Gui": {"PostActions": "ExitSelf"}, "Current": "日常"}
    actual = {"Gui": {"PostActions": "None"}, "Timers": []}
    diffs = projection.diff(ctx, desired, actual)
    assert [(d.path, d.kind) for d in diffs] == [
        ("Current", ConfigDiffKind.ADDED),
        ("Gui.PostActions", ConfigDiffKind.CHANGED),
        ("Timers", ConfigDiffKind.REMOVED),
    ]


def test_config_projection_diff_is_symmetric() -> None:
    """docs/05 契约测试表 config_projection 行：交换 desired 与 actual 得到镜像差异。"""
    projection = MaaConfigProjection()
    ctx = make_context()
    desired = {"Gui": {"PostActions": "ExitSelf"}, "Current": "日常"}
    actual = {"Gui": {"PostActions": "None"}, "Timers": []}
    backward = projection.diff(ctx, actual, desired)
    assert [(d.path, d.kind) for d in backward] == [
        ("Current", ConfigDiffKind.REMOVED),
        ("Gui.PostActions", ConfigDiffKind.CHANGED),
        ("Timers", ConfigDiffKind.ADDED),
    ]
    assert [(d.path, d.kind) for d in projection.diff(ctx, desired, desired)] == []


def test_config_projection_apply_and_rollback_refuse_writes() -> None:
    """docs/05 契约测试表 config_projection 行：写入动作失败关闭且可重复。"""
    projection = MaaConfigProjection()
    ctx = make_context()
    for _ in range(2):
        assert projection.apply(ctx, {}).error_code == "E_CONFIG"
        assert projection.rollback(ctx, "20261025-120000").error_code == "E_CONFIG"
