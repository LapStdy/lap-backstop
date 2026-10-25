"""错误码、错误类别与默认处置。

唯一真源是 docs/03 的错误码表与「错误处置」一节。
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from contracts.types import ErrorCategory, RecoveryAction


class ErrorCode(StrEnum):
    BAD_REQUEST = "E_BAD_REQUEST"
    AUTH = "E_AUTH"
    NOT_FOUND = "E_NOT_FOUND"
    CONFLICT = "E_CONFLICT"
    BUSY = "E_BUSY"
    PLUGIN = "E_PLUGIN"
    PLUGIN_VERSION = "E_PLUGIN_VERSION"
    PLUGIN_MANIFEST = "E_PLUGIN_MANIFEST"
    PLUGIN_CRASH = "E_PLUGIN_CRASH"
    CONFIG = "E_CONFIG"
    ENV_RESOLUTION = "E_ENV_RESOLUTION"
    ENV_DISCONNECTED = "E_ENV_DISCONNECTED"
    ENV_UNREACHABLE = "E_ENV_UNREACHABLE"
    ENV_WAKE_TIMEOUT = "E_ENV_WAKE_TIMEOUT"
    CONFIG_LOCKED = "E_CONFIG_LOCKED"
    CONFIG_CONFLICT = "E_CONFIG_CONFLICT"
    CONFIG_INVALID = "E_CONFIG_INVALID"
    SCRIPT_TASK_FAILED = "E_SCRIPT_TASK_FAILED"
    SCRIPT_EXITED = "E_SCRIPT_EXITED"
    SCRIPT_RESOURCE_TIMEOUT = "E_SCRIPT_RESOURCE_TIMEOUT"
    VERIFY_UNKNOWN_SCREEN = "E_VERIFY_UNKNOWN_SCREEN"
    VERIFY_NO_EVIDENCE = "E_VERIFY_NO_EVIDENCE"
    ENV_NOT_READY = "E_ENV_NOT_READY"
    PREPARE_TIMEOUT = "E_PREPARE_TIMEOUT"
    SCRIPT = "E_SCRIPT"
    VERIFY_UNKNOWN = "E_VERIFY_UNKNOWN"
    RECOVERY_EXHAUSTED = "E_RECOVERY_EXHAUSTED"
    DEADLINE = "E_DEADLINE"
    RESOURCE = "E_RESOURCE"
    RESOURCE_LEASE_LOST = "E_RESOURCE_LEASE_LOST"
    STORAGE = "E_STORAGE"
    DISK = "E_DISK"
    NOTIFY = "E_NOTIFY"
    INTERNAL = "E_INTERNAL"


# 基础码只作兜底：能用场景码的用场景码。
BASE_CODES: Final[frozenset[ErrorCode]] = frozenset(
    {ErrorCode.PLUGIN, ErrorCode.CONFIG, ErrorCode.SCRIPT}
)

SCENARIO_CODES: Final[dict[str, ErrorCode]] = {
    "manifest": ErrorCode.PLUGIN_MANIFEST,
    "plugin_crash": ErrorCode.PLUGIN_CRASH,
    "env_disconnected": ErrorCode.ENV_DISCONNECTED,
    "env_wake_timeout": ErrorCode.ENV_WAKE_TIMEOUT,
    "script_resource_timeout": ErrorCode.SCRIPT_RESOURCE_TIMEOUT,
    "config_locked": ErrorCode.CONFIG_LOCKED,
}

_CATEGORY: Final[dict[ErrorCode, ErrorCategory]] = {
    ErrorCode.ENV_RESOLUTION: ErrorCategory.ENVIRONMENT,
    ErrorCode.ENV_DISCONNECTED: ErrorCategory.ENVIRONMENT,
    ErrorCode.ENV_UNREACHABLE: ErrorCategory.ENVIRONMENT,
    ErrorCode.ENV_WAKE_TIMEOUT: ErrorCategory.ENVIRONMENT,
    ErrorCode.ENV_NOT_READY: ErrorCategory.ENVIRONMENT,
    ErrorCode.PREPARE_TIMEOUT: ErrorCategory.ENVIRONMENT,
    ErrorCode.SCRIPT_TASK_FAILED: ErrorCategory.SCRIPT,
    ErrorCode.SCRIPT_EXITED: ErrorCategory.SCRIPT,
    ErrorCode.SCRIPT_RESOURCE_TIMEOUT: ErrorCategory.SCRIPT,
    ErrorCode.SCRIPT: ErrorCategory.SCRIPT,
    ErrorCode.PLUGIN_CRASH: ErrorCategory.SCRIPT,
    ErrorCode.CONFIG: ErrorCategory.CONFIG,
    ErrorCode.CONFIG_LOCKED: ErrorCategory.CONFIG,
    ErrorCode.CONFIG_CONFLICT: ErrorCategory.CONFIG,
    ErrorCode.CONFIG_INVALID: ErrorCategory.CONFIG,
    ErrorCode.PLUGIN_VERSION: ErrorCategory.CONFIG,
    ErrorCode.VERIFY_UNKNOWN_SCREEN: ErrorCategory.VERIFY,
    ErrorCode.VERIFY_NO_EVIDENCE: ErrorCategory.VERIFY,
    ErrorCode.VERIFY_UNKNOWN: ErrorCategory.VERIFY,
    ErrorCode.RESOURCE: ErrorCategory.RESOURCE,
    ErrorCode.RESOURCE_LEASE_LOST: ErrorCategory.RESOURCE,
    ErrorCode.BUSY: ErrorCategory.RESOURCE,
    ErrorCode.INTERNAL: ErrorCategory.SYSTEM,
    ErrorCode.STORAGE: ErrorCategory.SYSTEM,
    ErrorCode.DISK: ErrorCategory.SYSTEM,
}

DEFAULT_RECOVERY: Final[dict[ErrorCategory, RecoveryAction]] = {
    ErrorCategory.ENVIRONMENT: RecoveryAction.RESET_ENVIRONMENT,
    ErrorCategory.SCRIPT: RecoveryAction.RESTART_SCRIPT,
    ErrorCategory.CONFIG: RecoveryAction.ABORT,
    ErrorCategory.VERIFY: RecoveryAction.ABORT,
    ErrorCategory.RESOURCE: RecoveryAction.RETRY,
    ErrorCategory.SYSTEM: RecoveryAction.ABORT,
}

# 通知失败走渠道重试，不触发运行恢复。
_NO_RECOVERY_CODES: Final[frozenset[ErrorCode]] = frozenset({ErrorCode.NOTIFY})

_HTTP: Final[dict[ErrorCode, int]] = {
    ErrorCode.BAD_REQUEST: 400,
    ErrorCode.CONFIG: 400,
    ErrorCode.AUTH: 401,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.CONFLICT: 409,
    ErrorCode.PLUGIN_VERSION: 409,
    ErrorCode.BUSY: 429,
    ErrorCode.DISK: 507,
}
DEFAULT_HTTP_STATUS: Final[int] = 500


def category_of(code: ErrorCode) -> ErrorCategory | None:
    """错误码的类别；未归类的码按 system 处置。"""
    return _CATEGORY.get(code)


def default_recovery(code: ErrorCode) -> RecoveryAction | None:
    """默认恢复动作；通知类失败不触发恢复，返回 None。"""
    if code in _NO_RECOVERY_CODES:
        return None
    return DEFAULT_RECOVERY[category_of(code) or ErrorCategory.SYSTEM]


def http_status(code: ErrorCode) -> int:
    """错误码的 HTTP 状态码；未映射的码按 500 兜底。"""
    return _HTTP.get(code, DEFAULT_HTTP_STATUS)


class ContractError(Exception):
    """契约校验失败的基类；携带错误码供内核落库与通知。"""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(f"{code.value}: {message}")
        self.code = code
        self.message = message
