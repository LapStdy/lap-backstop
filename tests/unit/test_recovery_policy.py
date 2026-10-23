"""恢复策略：动作路由、退避序列与预算耗尽。"""

from __future__ import annotations

import pytest

from contracts import RecoveryAction
from core.recovery.policy import (
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    RecoveryPolicy,
    RecoveryPolicyError,
)


def test_defaults() -> None:
    policy = RecoveryPolicy()
    assert policy.max_attempts == DEFAULT_MAX_ATTEMPTS
    assert policy.backoff_seconds == DEFAULT_BACKOFF_SECONDS
    assert policy.reset_environment
    assert policy.restart_script


def test_backoff_progression_and_exhaustion() -> None:
    policy = RecoveryPolicy()
    first = policy.decide(error_code="E_ENV_WAKE_TIMEOUT", attempt=0)
    assert (first.action, first.next_attempt, first.delay_seconds, first.exhausted) == (
        RecoveryAction.RESET_ENVIRONMENT,
        1,
        30,
        False,
    )
    second = policy.decide(error_code="E_ENV_WAKE_TIMEOUT", attempt=1)
    assert (second.delay_seconds, second.exhausted) == (120, False)
    third = policy.decide(error_code="E_ENV_WAKE_TIMEOUT", attempt=2)
    assert third.exhausted
    assert third.action is RecoveryAction.ABORT
    assert third.delay_seconds == 0


@pytest.mark.parametrize(
    ("error_code", "action"),
    [
        ("E_ENV_WAKE_TIMEOUT", RecoveryAction.RESET_ENVIRONMENT),
        ("E_PREPARE_TIMEOUT", RecoveryAction.RESET_ENVIRONMENT),
        ("E_SCRIPT_TASK_FAILED", RecoveryAction.RESTART_SCRIPT),
        ("E_PLUGIN_CRASH", RecoveryAction.RESTART_SCRIPT),
        ("E_CONFIG", RecoveryAction.ABORT),
        ("E_VERIFY_UNKNOWN", RecoveryAction.ABORT),
        ("E_BUSY", RecoveryAction.RETRY),
        ("E_STORAGE", RecoveryAction.ABORT),
        ("E_DEADLINE", RecoveryAction.ABORT),
        ("E_NOT_A_CODE", RecoveryAction.ABORT),
    ],
)
def test_default_action_by_error_code(error_code: str, action: RecoveryAction) -> None:
    policy = RecoveryPolicy(max_attempts=5)
    decision = policy.decide(error_code=error_code, attempt=0)
    assert decision.action is action


def test_disabled_reset_falls_back_to_retry() -> None:
    policy = RecoveryPolicy(reset_environment=False, max_attempts=5)
    decision = policy.decide(error_code="E_ENV_WAKE_TIMEOUT", attempt=0)
    assert decision.action is RecoveryAction.RETRY


def test_disabled_restart_falls_back_to_retry() -> None:
    policy = RecoveryPolicy(restart_script=False, max_attempts=5)
    decision = policy.decide(error_code="E_SCRIPT_TASK_FAILED", attempt=0)
    assert decision.action is RecoveryAction.RETRY


def test_custom_policy_parsing() -> None:
    policy = RecoveryPolicy.from_mapping(
        {
            "max_attempts": 5,
            "backoff_seconds": [1, 2],
            "reset_environment": False,
            "restart_script": False,
        }
    )
    assert policy.max_attempts == 5
    assert policy.backoff_seconds == (1, 2)
    assert not policy.reset_environment
    assert not policy.restart_script
    assert policy.delay_for(3) == 2


def test_empty_mapping_uses_defaults() -> None:
    assert RecoveryPolicy.from_mapping(None) == RecoveryPolicy()
    assert RecoveryPolicy.from_mapping({}) == RecoveryPolicy()


@pytest.mark.parametrize(
    "raw",
    [
        {"max_attempts": 0},
        {"max_attempts": "3"},
        {"backoff_seconds": "30"},
        {"backoff_seconds": [30, -1]},
        {"reset_environment": "yes"},
        {"restart_script": 1},
    ],
)
def test_invalid_policy_is_rejected(raw: dict[str, object]) -> None:
    with pytest.raises(RecoveryPolicyError):
        RecoveryPolicy.from_mapping(raw)
