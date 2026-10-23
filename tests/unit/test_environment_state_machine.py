"""环境状态机的显式边与「任意状态」边。"""

from __future__ import annotations

import pytest

from contracts import EnvironmentState
from core.environments.errors import EnvironmentStateError
from core.environments.state_machine import (
    EXPLICIT_TRANSITIONS,
    allowed_targets,
    can_transition,
    require_transition,
)

DOCUMENTED_EDGES = {
    EnvironmentState.UNKNOWN: {
        EnvironmentState.OFF,
        EnvironmentState.BOOTING,
        EnvironmentState.FAILED,
    },
    EnvironmentState.OFF: {EnvironmentState.BOOTING},
    EnvironmentState.BOOTING: {EnvironmentState.READY, EnvironmentState.FAILED},
    EnvironmentState.READY: {
        EnvironmentState.BUSY,
        EnvironmentState.DEGRADED,
        EnvironmentState.OFF,
        EnvironmentState.FAILED,
    },
    EnvironmentState.BUSY: {
        EnvironmentState.READY,
        EnvironmentState.DEGRADED,
        EnvironmentState.FAILED,
    },
    EnvironmentState.DEGRADED: {EnvironmentState.READY, EnvironmentState.FAILED},
    EnvironmentState.FAILED: {EnvironmentState.BOOTING, EnvironmentState.OFF},
}

ANY_STATE_TARGETS = {EnvironmentState.UNKNOWN, EnvironmentState.BOOTING}


def expected_targets(state: EnvironmentState) -> set[EnvironmentState]:
    return (DOCUMENTED_EDGES[state] | ANY_STATE_TARGETS) - {state}


def test_transition_table_matches_docs() -> None:
    assert {state: set(targets) for state, targets in EXPLICIT_TRANSITIONS.items()} == (
        DOCUMENTED_EDGES
    )


@pytest.mark.parametrize("state", list(EnvironmentState))
def test_allowed_targets_add_any_state_edges(state: EnvironmentState) -> None:
    assert allowed_targets(state) == expected_targets(state)


@pytest.mark.parametrize("source", list(EnvironmentState))
def test_every_other_pair_is_rejected(source: EnvironmentState) -> None:
    for target in EnvironmentState:
        if target in expected_targets(source):
            assert can_transition(source, target)
            require_transition(source, target)
        else:
            assert not can_transition(source, target)
            with pytest.raises(EnvironmentStateError):
                require_transition(source, target)


@pytest.mark.parametrize("state", list(EnvironmentState))
def test_self_transition_is_rejected(state: EnvironmentState) -> None:
    assert not can_transition(state, state)
