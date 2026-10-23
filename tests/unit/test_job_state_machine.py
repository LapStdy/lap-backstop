"""JobRun 状态机与 docs/02 的 JobRun 迁移表逐条对应。"""

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
from tests.state_tables import run_table

TABLE = run_table()


def test_code_table_declares_every_documented_state() -> None:
    assert set(RunState) == set(TABLE.edges)
    assert set(ALLOWED_TRANSITIONS) == set(TABLE.edges)


def test_code_edges_match_docs() -> None:
    assert {state: set(targets) for state, targets in ALLOWED_TRANSITIONS.items()} == {
        state: set(TABLE.edges[state]) for state in TABLE.states
    }


@pytest.mark.parametrize(("source", "target"), TABLE.legal_edges())
def test_documented_edge_is_allowed(source: RunState, target: RunState) -> None:
    assert can_transition(source, target)
    assert target in allowed_targets(source)
    require_transition(source, target)


@pytest.mark.parametrize(("source", "target"), TABLE.rejected_edges())
def test_undocumented_edge_is_rejected(source: RunState, target: RunState) -> None:
    assert not can_transition(source, target)
    with pytest.raises(IllegalRunTransitionError):
        require_transition(source, target)


@pytest.mark.parametrize("state", list(RunState))
def test_terminal_states_have_no_outgoing_edges(state: RunState) -> None:
    assert is_terminal(state) is (state in TERMINAL_RUN_STATES)
    if is_terminal(state):
        assert allowed_targets(state) == frozenset()
        assert TABLE.edges[state] == frozenset()


@pytest.mark.parametrize("state", list(RunState))
def test_self_transition_is_idempotent_not_an_edge(state: RunState) -> None:
    assert not can_transition(state, state)
    with pytest.raises(IllegalRunTransitionError):
        require_transition(state, state)
