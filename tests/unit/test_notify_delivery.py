"""投递重试：次数、退避、停机放弃与工作线程的队列进出。"""

from __future__ import annotations

import threading
import time
from typing import Any

from contracts import Notification, NotificationLevel
from contracts.types import DeliveryResult
from core.notify.delivery import (
    ChannelSnapshot,
    DeliveryWorker,
    PendingDelivery,
    deliver_one,
)

NOTIFICATION = Notification(level=NotificationLevel.INFO, title="t", body="b")
ITEM = PendingDelivery(notification=NOTIFICATION, run_id="r1", channel="serverchan")


def _send_script(results: list[DeliveryResult], calls: list[Notification]) -> Any:
    def send(message: Notification) -> DeliveryResult:
        calls.append(message)
        return results.pop(0) if results else DeliveryResult(ok=True)

    return send


def _snapshot(
    send: Any, attempts: int = 3, backoff: tuple[float, ...] = (5.0, 30.0)
) -> ChannelSnapshot:
    return ChannelSnapshot(send=send, attempts=attempts, backoff_seconds=backoff)


def _fail(code: str = "E_NOTIFY", detail: str = "HTTP 500") -> DeliveryResult:
    return DeliveryResult(ok=False, error_code=code, detail=detail)


def _drain_within(worker: DeliveryWorker, timeout_seconds: float) -> list[Any]:
    """等工作线程把结果入队；超时返回已收到的部分，用例自行断言。"""
    deadline = time.monotonic() + timeout_seconds
    outcomes: list[Any] = []
    while not outcomes and time.monotonic() < deadline:
        outcomes = worker.drain_outcomes()
        if not outcomes:
            time.sleep(0.01)
    return outcomes


def test_success_on_first_attempt_records_no_wait() -> None:
    calls: list[Notification] = []
    waits: list[float] = []
    outcome = deliver_one(
        ITEM,
        snapshot=_snapshot(_send_script([], calls)),
        wait=waits.append,
        stop=threading.Event(),
    )
    assert (outcome.ok, outcome.attempts, outcome.error) == (True, 1, "")
    assert waits == []
    assert calls == [NOTIFICATION]


def test_retry_until_success_with_backoff_sequence() -> None:
    calls: list[Notification] = []
    waits: list[float] = []
    outcome = deliver_one(
        ITEM,
        snapshot=_snapshot(_send_script([_fail(), _fail()], calls)),
        wait=waits.append,
        stop=threading.Event(),
    )
    assert (outcome.ok, outcome.attempts) == (True, 3)
    assert waits == [5.0, 30.0]
    assert len(calls) == 3


def test_backoff_clamps_to_last_entry() -> None:
    waits: list[float] = []
    outcome = deliver_one(
        ITEM,
        snapshot=_snapshot(lambda message: _fail(), attempts=5),
        wait=waits.append,
        stop=threading.Event(),
    )
    assert waits == [5.0, 30.0, 30.0, 30.0]
    assert outcome.attempts == 5


def test_exhaustion_carries_last_error_and_attempts() -> None:
    outcome = deliver_one(
        ITEM,
        snapshot=_snapshot(lambda message: _fail(detail="HTTP 502")),
        wait=lambda seconds: None,
        stop=threading.Event(),
    )
    assert not outcome.ok
    assert outcome.attempts == 3
    assert outcome.error == "E_NOTIFY：HTTP 502"


def test_stop_abandons_pending_retries_and_reports_actual_attempts() -> None:
    calls: list[Notification] = []
    stop = threading.Event()

    def wait(seconds: float) -> None:
        stop.set()

    outcome = deliver_one(
        ITEM,
        snapshot=_snapshot(_send_script([_fail()], calls), attempts=4, backoff=(60.0,)),
        wait=wait,
        stop=stop,
    )
    assert not outcome.ok
    assert outcome.attempts == 1
    assert outcome.error == "E_NOTIFY：HTTP 500"
    assert len(calls) == 1


def test_stop_before_first_attempt_makes_no_call() -> None:
    calls: list[Notification] = []
    stop = threading.Event()
    stop.set()
    outcome = deliver_one(
        ITEM,
        snapshot=_snapshot(_send_script([], calls)),
        wait=lambda seconds: None,
        stop=stop,
    )
    assert not outcome.ok
    assert outcome.attempts == 0
    assert outcome.error == "停机放弃投递"
    assert calls == []


def test_missing_channel_reports_unavailable_without_calling() -> None:
    calls: list[Notification] = []
    outcome = deliver_one(
        ITEM,
        snapshot=_snapshot(None),
        wait=lambda seconds: None,
        stop=threading.Event(),
    )
    assert not outcome.ok
    assert outcome.attempts == 0
    assert calls == []


def test_worker_delivers_and_reports_outcome() -> None:
    stop = threading.Event()
    calls: list[Notification] = []

    def send(message: Notification) -> DeliveryResult:
        calls.append(message)
        return DeliveryResult(ok=True)

    worker = DeliveryWorker(lambda: _snapshot(send), stop=stop, wait=lambda seconds: None)
    worker.start()
    try:
        worker.submit(ITEM)
        outcomes = _drain_within(worker, timeout_seconds=5.0)
        assert len(outcomes) == 1
        assert outcomes[0].ok
        assert (outcomes[0].run_id, outcomes[0].channel, outcomes[0].attempts) == (
            "r1",
            "serverchan",
            1,
        )
    finally:
        worker.close(join_timeout=5.0)
    assert len(calls) == 1


def test_worker_skips_delivery_when_channel_missing() -> None:
    stop = threading.Event()
    worker = DeliveryWorker(lambda: _snapshot(None), stop=stop, wait=lambda seconds: None)
    worker.start()
    worker.submit(ITEM)
    worker.submit(ITEM)
    worker.close(join_timeout=5.0)
    assert worker.drain_outcomes() == []
