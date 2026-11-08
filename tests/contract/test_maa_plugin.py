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
from plugins.maa.capabilities.script import DEFAULT_MAA_DIR, MaaScript
from plugins.maa.capabilities.verifier import DEFAULT_POLICY_ID, EVIDENCE_NOTE_NAME, MaaVerifier
from plugins.maa.plugin import PLUGIN, load_manifest
from plugins.maa.windows_host import ScreenSize
from tests.config_store_support import (
    FakeConfigStore,
    config_path,
    encode,
    platform_config,
    sample_config,
)

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
    """docs/plugins/maa.md 参数 schema 表：19 个配置字段、Draft 2020-12、根封闭。"""
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
        "emulator_window_title",
        "game_package",
        "screen_size",
        "mumu_instance_index",
        "mumu_boot_timeout_s",
        "game_launch_timeout_s",
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
    # schema 是默认值真源；script.py 的兜底常量与此处同步（两处同源维护）。
    assert props["maa_dir"]["default"] == DEFAULT_MAA_DIR
    assert props["mumu_dir"]["default"] == "H:/MuMuPlayer"
    assert props["adb_address"]["default"] == "127.0.0.1:16384"
    assert props["emulator_window_title"]["default"] == "MuMu"
    assert props["game_package"]["default"] == "com.hypergryph.arknights"
    assert props["screen_size"]["default"] == "1280x720"
    assert props["mumu_instance_index"]["default"] == 0
    assert props["mumu_boot_timeout_s"]["default"] == 180
    assert props["game_launch_timeout_s"]["default"] == 60
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
    evidence_dir: Path | None = None,
    config: Mapping[str, Any] | None = None,
    work_dir: Path | None = None,
) -> RunContext:
    """构造契约测试用的 RunContext；证据 id 恒为 evidence-1。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config={} if config is None else config,
        work_dir=Path(".") if work_dir is None else work_dir,
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


class _StubHost:
    """固定真机读数的假件：窗口在、adb 在线、游戏在前台、分辨率正确。"""

    def window_titles(self) -> tuple[str, ...]:
        return ("MuMuPlayer-12.0",)

    def adb_device_state(self) -> str:
        return "device"

    def screencap_size(self) -> ScreenSize:
        return ScreenSize(width=1280, height=720)

    def foreground_package(self) -> str | None:
        return "com.hypergryph.arknights"


def test_probe_returns_legal_state_with_clock_timestamp() -> None:
    """docs/05 契约测试表 environment 行：probe 返回合法状态，时间来自注入时钟。"""
    provider = MaaEnvironment(host=_StubHost())
    first = provider.probe(env_ref(), make_context())
    second = provider.probe(env_ref(), make_context())
    assert isinstance(first, EnvironmentProbe)
    assert isinstance(first.state, EnvironmentState)
    assert first.state is EnvironmentState.READY
    assert first.observed_at == NOW
    assert first == second


class _StubActions:
    """假模拟器动作端口：记录调用，什么都不做。"""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def start_instance(self, instance_index: int) -> None:
        self.calls.append(f"start:{instance_index}")

    def connect_adb(self) -> None:
        self.calls.append("connect")

    def disconnect_adb(self) -> None:
        self.calls.append("disconnect")

    def is_package_running(self, package: str) -> bool:
        return True

    def launch_package(self, package: str) -> None:
        self.calls.append(f"launch:{package}")

    def stop_package(self, package: str) -> None:
        self.calls.append(f"stop:{package}")

    def shutdown_instance(self, instance_index: int) -> None:
        self.calls.append(f"shutdown:{instance_index}")


def test_environment_actions_are_repeatable_and_idempotent() -> None:
    """docs/05 契约测试表 environment 行：四个动作可重复调用且都成功。"""
    provider = MaaEnvironment(host=_StubHost(), actions=_StubActions())
    ctx = make_context()
    ref = env_ref()
    for _ in range(2):
        assert provider.prepare(ref, ctx).ok is True
        assert provider.reset(ref, ctx).ok is True
        assert provider.cleanup(ref, ctx, keep_warm=False).ok is True
        assert provider.warm(ref, ctx, minutes=30).ok is True


class _StubProcessHost:
    """假进程表：拉起返回固定 pid，探活按已退出处理，不发真实命令。"""

    def start(self, executable: Path, working_dir: Path) -> int:
        return 4321

    def is_running(self, pid: int) -> bool:
        return False

    def running_pids(self, image_name: str) -> tuple[int, ...]:
        return ()

    def request_stop(self, pid: int) -> bool:
        return False

    def terminate(self, pid: int) -> None:
        return None


def test_launch_returns_handle_and_observe_is_repeatable(tmp_path: Path) -> None:
    """docs/05 契约测试表 script 行：launch 返回句柄；observe 只读且可重复。"""
    adapter = MaaScript(host=_StubProcessHost())
    ctx = make_context(config={"maa_dir": str(tmp_path)}, work_dir=tmp_path)
    handle = adapter.launch(script_ref(), ctx)
    assert isinstance(handle, ScriptHandle)
    assert handle.pid == 4321
    assert handle.meta["launched"] is True
    assert handle.meta["log_dir"] == str(tmp_path / "debug")
    first = adapter.observe(handle, ctx)
    second = adapter.observe(handle, ctx)
    assert first.state is ScriptState.RUNNING
    assert first.signals == ()
    assert first == second


def test_stop_is_idempotent_and_recover_fails_closed(tmp_path: Path) -> None:
    """docs/05 契约测试表 script 行：stop 可重复；recover 对四种内核动作都不执行。"""
    adapter = MaaScript(host=_StubProcessHost())
    ctx = make_context(config={"maa_dir": str(tmp_path)}, work_dir=tmp_path)
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
    assert first.status is VerdictStatus.UNKNOWN
    assert first.evidence_ids
    assert first.status == second.status
    assert first.policy_id == second.policy_id == DEFAULT_POLICY_ID
    assert (tmp_path / EVIDENCE_NOTE_NAME).is_file()


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


def test_config_projection_apply_then_diff_is_empty(tmp_path: Path) -> None:
    """docs/05 契约测试表 config_projection 行：apply 后 diff 为空，rollback 恢复原值。"""
    path = config_path(tmp_path)
    store = FakeConfigStore({path: encode(sample_config())})
    projection = MaaConfigProjection(store=store)
    ctx = make_context(config={"maa_dir": str(tmp_path)})
    desired = platform_config()
    desired["Configurations"]["Default"]["TaskQueue"] = []
    applied = projection.apply(ctx, desired)
    assert applied.ok is True
    actual = json.loads(store.files[path].decode("utf-8"))
    assert projection.diff(ctx, desired, actual) == []
    rolled = projection.rollback(ctx, str(applied.meta["revision"]))
    assert rolled.ok is True
    assert json.loads(store.files[path].decode("utf-8")) == sample_config()
