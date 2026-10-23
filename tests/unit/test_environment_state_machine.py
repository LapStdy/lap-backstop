"""环境状态机与 docs/02 的 Environment 表逐条对应。"""

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
from tests.state_tables import environment_table

TABLE = environment_table()


def test_code_table_declares_every_documented_state() -> None:
    assert set(EnvironmentState) == set(TABLE.edges)
    assert set(EXPLICIT_TRANSITIONS) == set(TABLE.edges)


def test_code_explicit_edges_match_docs() -> None:
    assert {state: set(targets) for state, targets in EXPLICIT_TRANSITIONS.items()} == {
        state: set(TABLE.edges[state]) for state in TABLE.states
    }


@pytest.mark.parametrize("state", list(EnvironmentState))
def test_allowed_targets_follow_documented_edges(state: EnvironmentState) -> None:
    assert allowed_targets(state) == TABLE.targets(state)


@pytest.mark.parametrize(("source", "target"), TABLE.legal_edges())
def test_documented_edge_is_allowed(source: EnvironmentState, target: EnvironmentState) -> None:
    assert can_transition(source, target)
    assert target in allowed_targets(source)
    require_transition(source, target)


@pytest.mark.parametrize(("source", "target"), TABLE.rejected_edges())
def test_undocumented_edge_is_rejected(
    source: EnvironmentState,
    target: EnvironmentState,
) -> None:
    assert not can_transition(source, target)
    with pytest.raises(EnvironmentStateError):
        require_transition(source, target)


@pytest.mark.parametrize("state", list(EnvironmentState))
def test_self_transition_is_not_a_transition(state: EnvironmentState) -> None:
    assert not can_transition(state, state)
    with pytest.raises(EnvironmentStateError):
        require_transition(state, state)
