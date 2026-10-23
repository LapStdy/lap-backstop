"""契约包的行为测试：manifest 校验、版本范围、事件 id、错误处置。"""

from __future__ import annotations

import dataclasses

import pytest

from contracts.errors import (
    ContractError,
    ErrorCode,
    category_of,
    default_recovery,
    http_status,
)
from contracts.events import (
    GLOBAL_EVENTS,
    RUN_SCOPED_EVENTS,
    EventType,
    global_event_id,
    parse_event_id,
    run_event_id,
)
from contracts.manifest import Manifest, kernel_rejects, range_covers
from contracts.types import ErrorCategory, RecoveryAction, Verdict, VerdictStatus

MANIFEST: dict[str, object] = {
    "id": "maa",
    "name": "MAA · 明日方舟",
    "version": "1.0.0",
    "api_version": 1,
    "description": "驱动 MAA 完成明日方舟日常并验证结果",
    "capabilities": ["environment", "script", "verifier", "config_projection"],
    "config_schema": "config.schema.json",
    "requires": {
        "contract": ">=1 <2",
        "platform": "windows",
        "python": ">=3.12",
        "executables": [{"name": "MAA.exe", "min_version": "6.0.0"}],
    },
    "config_migrations": [{"from": 1, "to": 2, "handler": "migrate_config_1_to_2"}],
    "error_map": {
        "E_ENV_NOT_READY": {
            "category": "environment",
            "retryable": True,
            "recovery": "reset_environment",
            "message": "环境未就绪",
        }
    },
}


def test_manifest_parses_documented_example() -> None:
    manifest = Manifest.from_dict(MANIFEST)
    assert manifest.id == "maa"
    assert manifest.capabilities == ("environment", "script", "verifier", "config_projection")
    assert manifest.requires.executables[0].name == "MAA.exe"
    assert manifest.error_map["E_ENV_NOT_READY"].recovery is RecoveryAction.RESET_ENVIRONMENT


def test_manifest_rejects_bad_id() -> None:
    with pytest.raises(ContractError) as excinfo:
        Manifest.from_dict({**MANIFEST, "id": "MAA"})
    assert ErrorCode.PLUGIN_MANIFEST.value in str(excinfo.value)


def test_manifest_rejects_unknown_capability() -> None:
    with pytest.raises(ContractError):
        Manifest.from_dict({**MANIFEST, "capabilities": ["environment", "telepathy"]})


def test_manifest_rejects_api_version_outside_own_range() -> None:
    with pytest.raises(ContractError):
        Manifest.from_dict({**MANIFEST, "api_version": 3})


def test_manifest_rejects_escaping_schema_path() -> None:
    with pytest.raises(ContractError):
        Manifest.from_dict({**MANIFEST, "config_schema": "../config.schema.json"})


@pytest.mark.parametrize(
    ("spec", "version", "expected"),
    [
        (">=1 <2", 1, True),
        (">=1 <2", 2, False),
        (">=1,<2", 1, True),
        ("==1", 1, True),
        ("1", 1, True),
        ("", 7, True),
    ],
)
def test_range_covers(spec: str, version: int, expected: bool) -> None:
    assert range_covers(spec, version) is expected


def test_kernel_rejects_when_contract_not_covered() -> None:
    manifest = Manifest.from_dict(MANIFEST)
    assert kernel_rejects(manifest, kernel_contract_version=1) is False
    assert kernel_rejects(manifest, kernel_contract_version=2) is True


def test_event_ids_roundtrip() -> None:
    run_id = "0b5f3f1e-0000-4000-8000-000000000001"
    scope, ref, seq = parse_event_id(run_event_id(run_id, 7))
    assert (scope, ref, seq) == ("r", run_id, 7)
    scope, ref, seq = parse_event_id(global_event_id(42))
    assert (scope, ref, seq) == ("g", "", 42)


def test_event_scope_partition_is_total() -> None:
    assert frozenset(EventType) == RUN_SCOPED_EVENTS | GLOBAL_EVENTS
    assert frozenset() == RUN_SCOPED_EVENTS & GLOBAL_EVENTS


def test_unknown_event_id_rejected() -> None:
    with pytest.raises(ValueError):
        parse_event_id("x:1")


def test_http_status_mapping() -> None:
    assert http_status(ErrorCode.BAD_REQUEST) == 400
    assert http_status(ErrorCode.AUTH) == 401
    assert http_status(ErrorCode.CONFLICT) == 409
    assert http_status(ErrorCode.BUSY) == 429
    assert http_status(ErrorCode.DISK) == 507
    assert http_status(ErrorCode.INTERNAL) == 500


def test_default_recovery_follows_category() -> None:
    assert category_of(ErrorCode.ENV_DISCONNECTED) is ErrorCategory.ENVIRONMENT
    assert default_recovery(ErrorCode.ENV_DISCONNECTED) is RecoveryAction.RESET_ENVIRONMENT
    assert default_recovery(ErrorCode.SCRIPT_EXITED) is RecoveryAction.RESTART_SCRIPT
    assert default_recovery(ErrorCode.BUSY) is RecoveryAction.RETRY
    assert default_recovery(ErrorCode.NOTIFY) is None


def test_verdict_is_frozen() -> None:
    verdict = Verdict(status=VerdictStatus.COMPLETED, policy_id="p", confidence=1.0, reason="ok")
    with pytest.raises(dataclasses.FrozenInstanceError):
        verdict.reason = "changed"  # type: ignore[misc]
