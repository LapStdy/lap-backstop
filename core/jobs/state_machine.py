"""JobRun 状态机：迁移表来自 docs/02。

非法迁移由调用方写一条被拒的 ``run.state.changed`` 事件，再把 E_CONFLICT 交给上层。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from contracts import TERMINAL_RUN_STATES, RunState
from core.jobs.errors import IllegalRunTransitionError

ALLOWED_TRANSITIONS: Final[Mapping[RunState, frozenset[RunState]]] = {
    RunState.SCHEDULED: frozenset({RunState.QUEUED, RunState.CANCELLED}),
    RunState.QUEUED: frozenset({RunState.PREPARING, RunState.CANCELLED, RunState.FAILED}),
    RunState.PREPARING: frozenset(
        {
            RunState.WAITING,
            RunState.EXECUTING,
            RunState.RECOVERING,
            RunState.CANCELLED,
            RunState.FAILED,
            RunState.UNKNOWN,
        }
    ),
    RunState.WAITING: frozenset(
        {
            RunState.EXECUTING,
            RunState.RECOVERING,
            RunState.CANCELLED,
            RunState.FAILED,
            RunState.UNKNOWN,
        }
    ),
    RunState.EXECUTING: frozenset(
        {RunState.VERIFYING, RunState.RECOVERING, RunState.CLEANING, RunState.UNKNOWN}
    ),
    RunState.VERIFYING: frozenset({RunState.CLEANING, RunState.RECOVERING, RunState.UNKNOWN}),
    RunState.CLEANING: frozenset(
        {
            RunState.SUCCEEDED,
            RunState.FAILED,
            RunState.UNKNOWN,
            RunState.CANCELLED,
        }
    ),
    RunState.RECOVERING: frozenset(
        {
            RunState.QUEUED,
            RunState.PREPARING,
            RunState.FAILED,
            RunState.CANCELLED,
            RunState.UNKNOWN,
        }
    ),
    RunState.SUCCEEDED: frozenset(),
    RunState.FAILED: frozenset(),
    RunState.UNKNOWN: frozenset(),
    RunState.CANCELLED: frozenset(),
}


def allowed_targets(state: RunState) -> frozenset[RunState]:
    """当前状态允许迁出的目标集合；终态返回空集。"""
    return ALLOWED_TRANSITIONS.get(state, frozenset())


def can_transition(source: RunState, target: RunState) -> bool:
    """判断一次迁移是否被状态机允许。"""
    return target in allowed_targets(source)


def require_transition(source: RunState, target: RunState) -> None:
    """迁移合法时返回，非法时抛 ``IllegalRunTransitionError``。"""
    if not can_transition(source, target):
        raise IllegalRunTransitionError(f"JobRun 不允许从 {source.value} 迁到 {target.value}")


def is_terminal(state: RunState) -> bool:
    """终态不再迁出。"""
    return state in TERMINAL_RUN_STATES
