"""fake 插件的 manifest、能力签名与七种行为形态。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import (
    EnvironmentProbe,
    EnvironmentProvider,
    EnvironmentRef,
    EnvironmentState,
    RecoveryAction,
    RunContext,
    ScriptAdapter,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    StopMode,
    VerdictStatus,
    Verifier,
)
from plugins.fake import plugin as fake
from plugins.fake.plugin import (
    SCENARIOS,
    FakeEnvironmentProvider,
    FakeScriptAdapter,
    FakeVerifier,
    load_manifest,
)

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)


class _CancelFlag:
    def is_cancelled(self) -> bool:
        return False


class _Logger:
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
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="fake",
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


class _Clock:
    def monotonic(self) -> float:
        return 0.0

    def utc_now(self) -> datetime:
        return NOW


def env_ref() -> EnvironmentRef:
    return EnvironmentRef(id="env-1", provider_id="fake")


def test_manifest_is_valid() -> None:
    manifest = load_manifest()
    assert manifest.id == "fake"
    assert manifest.api_version == 1
    assert manifest.capabilities == ("environment", "script", "verifier")


def test_capabilities_match_protocols() -> None:
    assert isinstance(fake.PLUGIN.environment, EnvironmentProvider)
    assert isinstance(fake.PLUGIN.script, ScriptAdapter)
    assert isinstance(fake.PLUGIN.verifier, Verifier)


def test_config_schema_lists_all_scenarios() -> None:
    schema = json.loads(
        Path(fake.__file__).with_name("config.schema.json").read_text(encoding="utf-8")
    )
    assert set(schema["properties"]["scenario"]["enum"]) == SCENARIOS


def test_instant_probe_is_ready() -> None:
    probe = FakeEnvironmentProvider().probe(env_ref(), make_context({"scenario": "instant"}))
    assert probe.state is EnvironmentState.READY


def test_slow_probe_becomes_ready_after_threshold() -> None:
    provider = FakeEnvironmentProvider()
    ctx = make_context({"scenario": "slow", "slow_probes": 2})
    states = [provider.probe(env_ref(), ctx).state for _ in range(3)]
    assert states == [
        EnvironmentState.BOOTING,
        EnvironmentState.BOOTING,
        EnvironmentState.READY,
    ]


def test_never_ready_prepare_fails_with_wake_timeout() -> None:
    ctx = make_context({"scenario": "never-ready"})
    result = FakeEnvironmentProvider().prepare(env_ref(), ctx)
    assert not result.ok
    assert result.error_code == "E_ENV_WAKE_TIMEOUT"


def test_script_instant_completes() -> None:
    ctx = make_context({"scenario": "instant"})
    adapter = FakeScriptAdapter()
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="fake"), ctx)
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.EXITED
    assert observation.last_signal is not None
    assert observation.last_signal.kind == "completed"


def test_script_fail_reports_task_failure() -> None:
    ctx = make_context({"scenario": "fail"})
    adapter = FakeScriptAdapter()
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="fake"), ctx)
    observation = adapter.observe(handle, ctx)
    assert observation.last_signal is not None
    assert observation.last_signal.kind == "task_failed"


def test_script_hang_reports_hung() -> None:
    ctx = make_context({"scenario": "hang"})
    adapter = FakeScriptAdapter()
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="fake"), ctx)
    observation = adapter.observe(handle, ctx)
    assert observation.state is ScriptState.HUNG


def test_script_crash_raises() -> None:
    with pytest.raises(RuntimeError):
        FakeScriptAdapter().launch(
            ScriptRef(id="script-1", plugin_id="fake"),
            make_context({"scenario": "crash"}),
        )


def test_verifier_returns_three_states(tmp_path: Path) -> None:
    expected = {
        "instant": VerdictStatus.COMPLETED,
        "fail": VerdictStatus.FAILED,
        "partial": VerdictStatus.UNKNOWN,
        "hang": VerdictStatus.UNKNOWN,
    }
    for scenario, status in expected.items():
        verdict = FakeVerifier().verify(make_context({"scenario": scenario}, evidence_dir=tmp_path))
        assert verdict.status is status
        assert verdict.evidence_ids == ("evidence-1",)


def test_stop_and_recover_are_idempotent() -> None:
    ctx = make_context()
    adapter = FakeScriptAdapter()
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="fake"), ctx)
    assert adapter.stop(handle, ctx, StopMode.GRACEFUL).ok
    assert adapter.recover(handle, ctx, RecoveryAction.RETRY).ok


def test_probe_observed_at_comes_from_clock() -> None:
    probe = FakeEnvironmentProvider().probe(env_ref(), make_context())
    assert isinstance(probe, EnvironmentProbe)
    assert probe.observed_at == NOW


def test_observation_type_contract() -> None:
    ctx = make_context()
    adapter = FakeScriptAdapter()
    handle = adapter.launch(ScriptRef(id="script-1", plugin_id="fake"), ctx)
    assert isinstance(adapter.observe(handle, ctx), ScriptObservation)
