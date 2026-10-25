"""错误码处置表：类别归属与默认恢复动作逐码断言。

判据是 docs/12 单元测试「错误码用例」与 docs/03「错误处置」表：
每个码按处置表断言类别与默认恢复动作；未归类的码按 system 处置；
通知失败（E_NOTIFY）走渠道重试，不触发运行的恢复动作。
"""

from __future__ import annotations

from typing import Final

import pytest

from contracts import ErrorCategory, ErrorCode, RecoveryAction, category_of, default_recovery

# docs/03「错误处置」表逐行：类别 → 默认恢复动作 → 归入本类的码。
_CATEGORY_ROWS: Final[tuple[tuple[ErrorCategory, RecoveryAction, tuple[ErrorCode, ...]], ...]] = (
    (
        ErrorCategory.ENVIRONMENT,
        RecoveryAction.RESET_ENVIRONMENT,
        (
            ErrorCode.ENV_RESOLUTION,
            ErrorCode.ENV_DISCONNECTED,
            ErrorCode.ENV_UNREACHABLE,
            ErrorCode.ENV_WAKE_TIMEOUT,
            ErrorCode.ENV_NOT_READY,
            ErrorCode.PREPARE_TIMEOUT,
        ),
    ),
    (
        ErrorCategory.SCRIPT,
        RecoveryAction.RESTART_SCRIPT,
        (
            ErrorCode.SCRIPT_TASK_FAILED,
            ErrorCode.SCRIPT_EXITED,
            ErrorCode.SCRIPT_RESOURCE_TIMEOUT,
            ErrorCode.SCRIPT,
            ErrorCode.PLUGIN_CRASH,
        ),
    ),
    (
        ErrorCategory.CONFIG,
        RecoveryAction.ABORT,
        (
            ErrorCode.CONFIG,
            ErrorCode.CONFIG_LOCKED,
            ErrorCode.CONFIG_CONFLICT,
            ErrorCode.CONFIG_INVALID,
            ErrorCode.PLUGIN_VERSION,
        ),
    ),
    (
        ErrorCategory.VERIFY,
        RecoveryAction.ABORT,
        (
            ErrorCode.VERIFY_UNKNOWN_SCREEN,
            ErrorCode.VERIFY_NO_EVIDENCE,
            ErrorCode.VERIFY_UNKNOWN,
        ),
    ),
    (
        ErrorCategory.RESOURCE,
        RecoveryAction.RETRY,
        (ErrorCode.RESOURCE, ErrorCode.RESOURCE_LEASE_LOST, ErrorCode.BUSY),
    ),
    (
        ErrorCategory.SYSTEM,
        RecoveryAction.ABORT,
        (ErrorCode.INTERNAL, ErrorCode.STORAGE, ErrorCode.DISK),
    ),
)

# docs/03：HTTP 专用码、插件基础码与流程终止码不在处置表内，未归类按 system 处置。
_UNCLASSIFIED: Final[tuple[ErrorCode, ...]] = (
    ErrorCode.BAD_REQUEST,
    ErrorCode.AUTH,
    ErrorCode.NOT_FOUND,
    ErrorCode.CONFLICT,
    ErrorCode.PLUGIN,
    ErrorCode.PLUGIN_MANIFEST,
    ErrorCode.RECOVERY_EXHAUSTED,
    ErrorCode.DEADLINE,
)


@pytest.mark.parametrize(
    ("error_code", "category"),
    [(code, row[0]) for row in _CATEGORY_ROWS for code in row[2]],
)
def test_category_matches_disposition_table(
    error_code: ErrorCode,
    category: ErrorCategory,
) -> None:
    assert category_of(error_code) is category


@pytest.mark.parametrize(
    ("error_code", "recovery"),
    [(code, row[1]) for row in _CATEGORY_ROWS for code in row[2]],
)
def test_default_recovery_matches_disposition_table(
    error_code: ErrorCode,
    recovery: RecoveryAction,
) -> None:
    assert default_recovery(error_code) is recovery


@pytest.mark.parametrize("error_code", _UNCLASSIFIED)
def test_unclassified_code_falls_back_to_system(error_code: ErrorCode) -> None:
    assert category_of(error_code) is None
    assert default_recovery(error_code) is RecoveryAction.ABORT


def test_notify_failure_does_not_trigger_run_recovery() -> None:
    assert category_of(ErrorCode.NOTIFY) is None
    assert default_recovery(ErrorCode.NOTIFY) is None


def test_disposition_table_covers_every_code() -> None:
    """处置表、未归类与通知特例合起来覆盖错误码全集，新增码必须显式归位。"""
    classified = {code for _, _, codes in _CATEGORY_ROWS for code in codes}
    assert classified | set(_UNCLASSIFIED) | {ErrorCode.NOTIFY} == set(ErrorCode)
