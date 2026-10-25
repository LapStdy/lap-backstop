"""事件回放器：状态重建、attempt 与阶段视图、流完整性校验。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from contracts import EventType, RunState
from core.events.replay import (
    DuplicateEventError,
    EmptyEventStreamError,
    EventStreamConflictError,
    EventStreamGapError,
    OutOfOrderEventError,
    replay_run,
)
from storage.repositories.events import StoredEvent

T0 = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)
RUN_ID = "run-1"


def make_event(
    seq: int,
    event_type: EventType,
    payload: dict[str, Any] | None = None,
    *,
    run_id: str = RUN_ID,
) -> StoredEvent:
    return StoredEvent(
        id=f"r:{run_id}:{seq}",
        run_id=run_id,
        seq=seq,
        type=event_type.value,
        at=T0,
        payload=dict(payload or {}),
    )


def changed(seq: int, source: str, target: str, reason: str = "") -> StoredEvent:
    return make_event(
        seq,
        EventType.RUN_STATE_CHANGED,
        {"run_id": RUN_ID, "from_state": source, "to_state": target, "reason": reason},
    )


def _phase(name: str, attempt: int) -> dict[str, Any]:
    return {"run_id": RUN_ID, "phase": name, "attempt": attempt}


def full_lifecycle() -> list[StoredEvent]:
    """一次成功运行的完整事件流，顺序与 worker 执行器的写侧一致。"""
    return [
        make_event(1, EventType.RUN_CREATED, {"run_id": RUN_ID}),
        changed(2, "scheduled", "queued", "enqueue"),
        make_event(3, EventType.RUN_QUEUED, {"run_id": RUN_ID, "priority": 0}),
        changed(4, "queued", "preparing"),
        make_event(5, EventType.RUN_PHASE_STARTED, _phase("prepare", 0)),
        make_event(6, EventType.RUN_PHASE_FINISHED, {**_phase("prepare", 0), "status": "ok"}),
        changed(7, "preparing", "waiting"),
        changed(8, "waiting", "executing"),
        make_event(9, EventType.RUN_PHASE_STARTED, _phase("execute", 0)),
        make_event(10, EventType.RUN_PHASE_FINISHED, {**_phase("execute", 0), "status": "ok"}),
        changed(11, "executing", "verifying"),
        make_event(12, EventType.RUN_PHASE_STARTED, _phase("verify", 0)),
        make_event(13, EventType.RUN_PHASE_FINISHED, {**_phase("verify", 0), "status": "ok"}),
        make_event(14, EventType.RUN_VERDICT, {"run_id": RUN_ID, "status": "completed"}),
        changed(15, "verifying", "cleaning"),
        make_event(16, EventType.RUN_PHASE_STARTED, _phase("cleanup", 0)),
        make_event(17, EventType.RUN_PHASE_FINISHED, {**_phase("cleanup", 0), "status": "ok"}),
        changed(18, "cleaning", "succeeded", "verdict"),
        make_event(19, EventType.RUN_FINISHED, {"run_id": RUN_ID, "status": "succeeded"}),
    ]


def test_replay_rejects_empty_stream() -> None:
    with pytest.raises(EmptyEventStreamError):
        replay_run(())


def test_replay_rebuilds_full_lifecycle() -> None:
    snapshot = replay_run(full_lifecycle())
    assert snapshot.run_id == RUN_ID
    assert snapshot.state is RunState.SUCCEEDED
    assert snapshot.phases == {
        "prepare": "ok",
        "execute": "ok",
        "verify": "ok",
        "cleanup": "ok",
    }
    assert snapshot.verdict_status == "completed"
    assert snapshot.finished is True
    assert snapshot.last_seq == 19
    assert snapshot.attempt == 0


def test_replay_rebuilds_attempt_from_recovery_events() -> None:
    events = [
        make_event(1, EventType.RUN_CREATED),
        changed(2, "scheduled", "queued", "enqueue"),
        changed(3, "queued", "preparing"),
        changed(4, "preparing", "recovering", "E_SCRIPT_TASK_FAILED"),
        make_event(
            5,
            EventType.RECOVERY_STARTED,
            {"run_id": RUN_ID, "action": "retry", "attempt": 2},
        ),
    ]
    snapshot = replay_run(events)
    assert snapshot.state is RunState.RECOVERING
    assert snapshot.attempt == 2


def test_replay_keeps_state_on_rejected_transition_record() -> None:
    events = [
        make_event(1, EventType.RUN_CREATED),
        changed(2, "scheduled", "queued", "enqueue"),
        changed(3, "queued", "succeeded", "illegal_transition"),
    ]
    snapshot = replay_run(events)
    assert snapshot.state is RunState.QUEUED


def test_replay_tracks_phase_states() -> None:
    events = [
        make_event(1, EventType.RUN_CREATED),
        changed(2, "scheduled", "queued"),
        changed(3, "queued", "preparing"),
        make_event(4, EventType.RUN_PHASE_STARTED, _phase("prepare", 1)),
        make_event(5, EventType.RUN_PHASE_STARTED, _phase("execute", 1)),
        make_event(6, EventType.RUN_PHASE_FINISHED, {**_phase("prepare", 1), "status": "ok"}),
    ]
    snapshot = replay_run(events)
    assert snapshot.phases == {"prepare": "ok", "execute": "running"}
    assert snapshot.attempt == 1


def test_replay_skips_informational_events() -> None:
    events = [
        make_event(1, EventType.RUN_CREATED),
        make_event(2, EventType.RUN_SIGNAL, {"run_id": RUN_ID, "kind": "log", "payload": {}}),
        make_event(
            3,
            EventType.EVIDENCE,
            {"run_id": RUN_ID, "kind": "log", "path": "a.log", "sha256": "00"},
        ),
    ]
    snapshot = replay_run(events)
    assert snapshot.state is RunState.SCHEDULED
    assert snapshot.last_seq == 3
    assert snapshot.finished is False


@pytest.mark.parametrize(
    ("seqs", "error"),
    [
        ((2,), EventStreamGapError),
        ((1, 3), EventStreamGapError),
        ((1, 2, 2), DuplicateEventError),
        ((1, 2, 3, 2), OutOfOrderEventError),
    ],
)
def test_replay_rejects_broken_sequence(seqs: tuple[int, ...], error: type[Exception]) -> None:
    events = [make_event(seq, EventType.RUN_CREATED) for seq in seqs]
    with pytest.raises(error):
        replay_run(events)


@pytest.mark.parametrize(
    "events",
    [
        pytest.param([changed(1, "scheduled", "queued")], id="head_not_created"),
        pytest.param(
            [
                make_event(1, EventType.RUN_CREATED),
                make_event(2, EventType.RUN_CREATED, run_id="run-2"),
            ],
            id="foreign_run",
        ),
        pytest.param(
            [make_event(1, EventType.RUN_CREATED), changed(2, "queued", "preparing")],
            id="source_mismatch",
        ),
        pytest.param(
            [make_event(1, EventType.RUN_CREATED), changed(2, "scheduled", "flying")],
            id="unknown_state",
        ),
        pytest.param(
            [
                make_event(1, EventType.RUN_CREATED),
                make_event(2, EventType.RECOVERY_STARTED, {"run_id": RUN_ID, "attempt": "two"}),
            ],
            id="non_integer_attempt",
        ),
    ],
)
def test_replay_rejects_conflicting_streams(events: list[StoredEvent]) -> None:
    with pytest.raises(EventStreamConflictError):
        replay_run(events)
