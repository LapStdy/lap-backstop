"""虚拟时钟与系统时钟的行为。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from contracts import Clock
from core.clock import SystemClock, VirtualClock

START = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)


def test_system_clock_returns_aware_utc() -> None:
    clock = SystemClock()
    moment = clock.utc_now()
    assert moment.tzinfo is UTC
    assert clock.monotonic() <= clock.monotonic()


def test_virtual_clock_advances_both_readings() -> None:
    clock = VirtualClock(START)
    clock.advance(timedelta(minutes=90))
    assert clock.utc_now() == START + timedelta(minutes=90)
    assert clock.monotonic() == pytest.approx(5400.0)


def test_virtual_clock_rejects_backwards_motion() -> None:
    clock = VirtualClock(START)
    with pytest.raises(ValueError):
        clock.advance(timedelta(seconds=-1))
    with pytest.raises(ValueError):
        clock.jump_to(START - timedelta(seconds=1))


def test_virtual_clock_rejects_naive_start() -> None:
    with pytest.raises(ValueError):
        VirtualClock(datetime(2026, 10, 23, 12, 0))


def test_virtual_clock_jump_keeps_monotonic_delta() -> None:
    clock = VirtualClock(START)
    clock.jump_to(START + timedelta(hours=2))
    assert clock.utc_now() == START + timedelta(hours=2)
    assert clock.monotonic() == pytest.approx(7200.0)


def test_virtual_clock_satisfies_contract_protocol() -> None:
    assert isinstance(VirtualClock(START), Clock)
