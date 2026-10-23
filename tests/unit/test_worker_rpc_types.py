"""worker 协议的契约类型编解码：往返与非法载荷。"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from contracts import (
    CheckResult,
    CheckStatus,
    EnvironmentProbe,
    EnvironmentRef,
    EnvironmentState,
    PhaseResult,
    RecoveryAction,
    RecoveryResult,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    Signal,
    StopMode,
    StopResult,
    Verdict,
    VerdictStatus,
)
from core.plugins import rpc_types
from core.plugins.errors import WorkerPayloadError

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)


def test_environment_probe_round_trip() -> None:
    probe = EnvironmentProbe(state=EnvironmentState.READY, detail="就绪", observed_at=NOW)
    assert rpc_types.decode_environment_probe(rpc_types.encode_environment_probe(probe)) == probe


def test_phase_result_round_trip() -> None:
    result = PhaseResult(ok=False, error_code="E_ENV_WAKE_TIMEOUT", detail="没起来")
    assert rpc_types.decode_phase_result(rpc_types.encode_phase_result(result)) == result


def test_script_handle_round_trip() -> None:
    handle = ScriptHandle(id="h-1", pid=None, meta={"scenario": "instant"})
    assert rpc_types.decode_script_handle(rpc_types.encode_script_handle(handle)) == handle


def test_script_observation_round_trip() -> None:
    signal = Signal(kind="completed", payload={"scenario": "instant"}, at=NOW)
    observation = ScriptObservation(
        state=ScriptState.EXITED,
        signals=(signal,),
        last_signal=signal,
        progress=1.0,
        since=NOW,
    )
    encoded = rpc_types.encode_script_observation(observation)
    assert rpc_types.decode_script_observation(encoded) == observation


def test_verdict_round_trip() -> None:
    check = CheckResult(
        name="fake",
        status=CheckStatus.OK,
        detail="完成",
        evidence_ids=("evidence-1",),
        at=NOW,
    )
    verdict = Verdict(
        status=VerdictStatus.COMPLETED,
        policy_id="fake-policy",
        confidence=1.0,
        reason="任务完成",
        checks=(check,),
        evidence_ids=("evidence-1",),
        created_at=NOW,
    )
    assert rpc_types.decode_verdict(rpc_types.encode_verdict(verdict)) == verdict


def test_stop_and_recovery_results_round_trip() -> None:
    stop = StopResult(ok=True, detail="已停止", meta={"mode": "force"})
    recovery = RecoveryResult(ok=False, error_code="E_SCRIPT", detail="重启失败")
    assert rpc_types.decode_stop_result(rpc_types.encode_stop_result(stop)) == stop
    assert rpc_types.decode_recovery_result(rpc_types.encode_recovery_result(recovery)) == recovery


def test_stop_mode_and_recovery_action_round_trip() -> None:
    for mode in StopMode:
        assert rpc_types.decode_stop_mode(rpc_types.encode_stop_mode(mode)) is mode
    for action in RecoveryAction:
        assert rpc_types.decode_recovery_action(rpc_types.encode_recovery_action(action)) is action


def test_reference_round_trips() -> None:
    env = EnvironmentRef(id="env-1", provider_id="fake", config={"a": 1})
    script = ScriptRef(id="script-1", plugin_id="fake", config={"b": [1, 2]})
    assert rpc_types.decode_environment_ref(rpc_types.encode_environment_ref(env)) == env
    assert rpc_types.decode_script_ref(rpc_types.encode_script_ref(script)) == script


def test_decode_rejects_unknown_enum() -> None:
    encoded = rpc_types.encode_environment_probe(EnvironmentProbe(state=EnvironmentState.READY))
    encoded["state"] = "sleeping"
    with pytest.raises(WorkerPayloadError, match="sleeping"):
        rpc_types.decode_environment_probe(encoded)


def test_decode_rejects_wrong_scalar_type() -> None:
    encoded = rpc_types.encode_script_observation(ScriptObservation(state=ScriptState.RUNNING))
    encoded["progress"] = "很快"
    with pytest.raises(WorkerPayloadError, match="progress"):
        rpc_types.decode_script_observation(encoded)


def test_decode_rejects_missing_required_field() -> None:
    encoded = rpc_types.encode_verdict(
        Verdict(status=VerdictStatus.COMPLETED, policy_id="p", confidence=1.0, reason="ok")
    )
    del encoded["policy_id"]
    with pytest.raises(WorkerPayloadError, match="policy_id"):
        rpc_types.decode_verdict(encoded)


def test_decode_ignores_unknown_fields() -> None:
    encoded = rpc_types.encode_script_handle(ScriptHandle(id="h-1"))
    encoded["kernel_hint"] = "忽略"
    assert rpc_types.decode_script_handle(encoded) == ScriptHandle(id="h-1")


def test_encode_rejects_unsupported_object() -> None:
    with pytest.raises(WorkerPayloadError):
        rpc_types.encode_value(object())
