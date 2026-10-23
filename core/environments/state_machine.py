"""环境状态机：迁移表来自 docs/02。

每条显式边之外，任何状态都能因休眠恢复进入 unknown、因 reset 进入 booting；
迁移到自身不算迁移，直接拒绝，避免写出没有变化的状态事件。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from contracts import EnvironmentState
from core.environments.errors import EnvironmentStateError

EXPLICIT_TRANSITIONS: Final[Mapping[EnvironmentState, frozenset[EnvironmentState]]] = {
    EnvironmentState.UNKNOWN: frozenset(
        {EnvironmentState.OFF, EnvironmentState.BOOTING, EnvironmentState.FAILED}
    ),
    EnvironmentState.OFF: frozenset({EnvironmentState.BOOTING}),
    EnvironmentState.BOOTING: frozenset({EnvironmentState.READY, EnvironmentState.FAILED}),
    EnvironmentState.READY: frozenset(
        {
            EnvironmentState.BUSY,
            EnvironmentState.DEGRADED,
            EnvironmentState.OFF,
            EnvironmentState.FAILED,
        }
    ),
    EnvironmentState.BUSY: frozenset(
        {EnvironmentState.READY, EnvironmentState.DEGRADED, EnvironmentState.FAILED}
    ),
    EnvironmentState.DEGRADED: frozenset({EnvironmentState.READY, EnvironmentState.FAILED}),
    EnvironmentState.FAILED: frozenset({EnvironmentState.BOOTING, EnvironmentState.OFF}),
}

_ANY_STATE_TARGETS: Final[frozenset[EnvironmentState]] = frozenset(
    {EnvironmentState.UNKNOWN, EnvironmentState.BOOTING}
)


def allowed_targets(state: EnvironmentState) -> frozenset[EnvironmentState]:
    """当前状态的合法迁出集合；不含自身。"""
    return (EXPLICIT_TRANSITIONS.get(state, frozenset()) | _ANY_STATE_TARGETS) - {state}


def can_transition(source: EnvironmentState, target: EnvironmentState) -> bool:
    """判断一次环境状态迁移是否被允许。"""
    return target in allowed_targets(source)


def require_transition(source: EnvironmentState, target: EnvironmentState) -> None:
    """迁移合法时返回，非法时抛 ``EnvironmentStateError``。"""
    if not can_transition(source, target):
        raise EnvironmentStateError(f"环境不允许从 {source.value} 迁到 {target.value}")
