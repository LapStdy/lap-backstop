"""JobRun 状态机的每一条边。"""

from __future__ import annotations

import pytest

from contracts import TERMINAL_RUN_STATES, RunState
from core.jobs.errors import IllegalRunTransitionError
from core.jobs.state_machine import (
    ALLOWED_TRANSITIONS,
    allowed_targets,
    can_transition,
    is_terminal,
    require_transition,
)

LEGAL_EDGES = {
    RunState.SCHEDULED: {RunState.QUEUED, RunState.CANCELLED},
    RunState.QUEUED: {RunState.PREPARING, RunState.CANCELLED, RunState.FAILED},
    RunState.PREPARING: {
        RunState.WAITING,
        RunState.EXECUTING,
        RunState.RECOVERING,
        RunState.CANCELLED,
        RunState.FAILED,
        RunState.UNKNOWN,
    },
    RunState.WAITING: {
        RunState.EXECUTING,
        RunState.RECOVERING,
        RunState.CANCELLED,
        RunState.FAILED,
        RunState.UNKNOWN,
    },
    RunState.EXECUTING: {
        RunState.VERIFYING,
        RunState.RECOVERING,
        RunState.CLEANING,
        RunState.UNKNOWN,
    },
    RunState.VERIFYING: {RunState.CLEANING, RunState.RECOVERING, RunState.UNKNOWN},
    RunState.CLEANING: {
        RunState.SUCCEEDED,
        RunState.FAILED,
        RunState.UNKNOWN,
        RunState.CANCELLED,
    },
    RunState.RECOVERING: {
        RunState.QUEUED,
        RunState.PREPARING,
        RunState.FAILED,
        RunState.CANCELLED,
        RunState.UNKNOWN,
    },
    RunState.SUCCEEDED: set(),
    RunState.FAILED: set(),
    RunState.UNKNOWN: set(),
    RunState.CANCELLED: set(),
}


def test_transition_table_matches_docs() -> None:
    assert {state: set(targets) for state, targets in ALLOWED_TRANSITIONS.items()} == LEGAL_EDGES


@pytest.mark.parametrize(("source", "targets"), LEGAL_EDGES.items())
def test_legal_edges_are_allowed(source: RunState, targets: set[RunState]) -> None:
    for target in targets:
        assert can_transition(source, target)
        assert target in allowed_targets(source)
        require_transition(source, target)


@pytest.mark.parametrize("source", list(RunState))
def test_every_other_pair_is_rejected(source: RunState) -> None:
    for target in RunState:
        if target in LEGAL_EDGES[source]:
            continue
        assert not can_transition(source, target)
        with pytest.raises(IllegalRunTransitionError):
            require_transition(source, target)


@pytest.mark.parametrize("state", list(RunState))
def test_terminal_states_have_no_outgoing_edges(state: RunState) -> None:
    assert is_terminal(state) is (state in TERMINAL_RUN_STATES)
    if is_terminal(state):
        assert allowed_targets(state) == frozenset()
