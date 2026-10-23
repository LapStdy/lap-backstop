"""恢复策略：按错误类别选动作、按 attempt 算退避、预算耗尽即停手。

策略结构来自作业模板的 recovery_policy：
``{"max_attempts": 3, "backoff_seconds": [30, 120, 300],
"reset_environment": true, "restart_script": true}``。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from contracts import ErrorCode, RecoveryAction, default_recovery
from storage.defaults import default

DEFAULT_MAX_ATTEMPTS = int(default("recovery", "max_attempts"))
DEFAULT_BACKOFF_SECONDS: tuple[int, ...] = tuple(
    int(seconds) for seconds in default("recovery", "backoff_seconds")
)


class RecoveryPolicyError(ValueError):
    """recovery_policy 不合法，对应错误码 E_CONFIG。"""

    code = "E_CONFIG"


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    """一次恢复决策：动作、下一次 attempt 序号与等待秒数。"""

    action: RecoveryAction
    next_attempt: int
    delay_seconds: int
    exhausted: bool


@dataclass(frozen=True, slots=True)
class RecoveryPolicy:
    """作业模板声明的恢复预算与动作开关。"""

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    backoff_seconds: tuple[int, ...] = DEFAULT_BACKOFF_SECONDS
    reset_environment: bool = True
    restart_script: bool = True

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> RecoveryPolicy:
        """解析模板里的 recovery_policy；缺省用默认预算。

        Raises:
            RecoveryPolicyError: 字段类型或取值不合法。
        """
        if raw is None:
            return cls()
        max_attempts = raw.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool):
            raise RecoveryPolicyError("recovery_policy.max_attempts 必须是整数")
        if max_attempts < 1:
            raise RecoveryPolicyError("recovery_policy.max_attempts 必须大于 0")
        backoff = _parse_backoff(raw.get("backoff_seconds", list(DEFAULT_BACKOFF_SECONDS)))
        reset = _parse_bool(raw.get("reset_environment", True), "reset_environment")
        restart = _parse_bool(raw.get("restart_script", True), "restart_script")
        return cls(
            max_attempts=max_attempts,
            backoff_seconds=backoff,
            reset_environment=reset,
            restart_script=restart,
        )

    def decide(self, *, error_code: str, attempt: int) -> RecoveryDecision:
        """按错误码与当前 attempt 给出决策。

        ``attempt`` 从 0 起；下一次 attempt 达到 ``max_attempts`` 时预算耗尽。
        """
        if attempt < 0:
            raise ValueError("attempt 不能为负")
        next_attempt = attempt + 1
        if next_attempt >= self.max_attempts:
            return RecoveryDecision(
                action=RecoveryAction.ABORT,
                next_attempt=next_attempt,
                delay_seconds=0,
                exhausted=True,
            )
        action = _default_action(error_code)
        blocked_reset = action is RecoveryAction.RESET_ENVIRONMENT and not self.reset_environment
        blocked_restart = action is RecoveryAction.RESTART_SCRIPT and not self.restart_script
        if blocked_reset or blocked_restart:
            action = RecoveryAction.RETRY
        return RecoveryDecision(
            action=action,
            next_attempt=next_attempt,
            delay_seconds=self.delay_for(attempt),
            exhausted=False,
        )

    def delay_for(self, attempt: int) -> int:
        """第 ``attempt`` 次失败后的退避秒数；超出表长度取最后一个。"""
        if not self.backoff_seconds:
            return 0
        index = min(attempt, len(self.backoff_seconds) - 1)
        return self.backoff_seconds[index]


def _default_action(error_code: str) -> RecoveryAction:
    """把错误码映射到默认恢复动作；未归类按 system 处置。"""
    try:
        code = ErrorCode(error_code)
    except ValueError:
        return RecoveryAction.ABORT
    return default_recovery(code) or RecoveryAction.ABORT


def _parse_backoff(raw: object) -> tuple[int, ...]:
    if not isinstance(raw, list):
        raise RecoveryPolicyError("recovery_policy.backoff_seconds 必须是数组")
    values: list[int] = []
    for item in raw:
        if not isinstance(item, int) or isinstance(item, bool) or item < 0:
            raise RecoveryPolicyError("backoff_seconds 必须是非负整数")
        values.append(item)
    return tuple(values)


def _parse_bool(raw: object, name: str) -> bool:
    if not isinstance(raw, bool):
        raise RecoveryPolicyError(f"recovery_policy.{name} 必须是布尔")
    return raw


__all__ = [
    "DEFAULT_BACKOFF_SECONDS",
    "DEFAULT_MAX_ATTEMPTS",
    "RecoveryDecision",
    "RecoveryPolicy",
    "RecoveryPolicyError",
]
