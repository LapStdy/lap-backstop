"""serve 循环的心跳节流：间隔按单调时钟判定，与墙钟 UTC 无关。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from core.clock import VirtualClock
from core.hostops.serve import HeartbeatWriter, ServeLoop, ServeOptions
from core.jobs.run_service import JobRunService
from core.supervisor import Supervisor
from storage import migrate, open_database

NOW = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)


class _StubSupervisor:
    """只计数 tick 的最小 Supervisor。"""

    def __init__(self) -> None:
        self.ticks = 0

    def tick(self) -> None:
        self.ticks += 1

    @property
    def is_disk_critical(self) -> bool:
        return False


class _RecordingWriter:
    """只记写入次数与内容，不落文件，避免依赖文件 mtime 精度。"""

    def __init__(self) -> None:
        self.writes: list[tuple[datetime, str | None]] = []

    def write(self, *, last_tick_at: datetime, active_run: str | None) -> None:
        self.writes.append((last_tick_at, active_run))


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    database = open_database(tmp_path / "backstop.sqlite")
    migrate(database)
    try:
        yield database
    finally:
        database.close()


def make_loop(
    connection: sqlite3.Connection, clock: VirtualClock
) -> tuple[ServeLoop, _RecordingWriter]:
    writer = _RecordingWriter()
    loop = ServeLoop(
        cast(Supervisor, _StubSupervisor()),
        JobRunService(connection, clock),
        cast(HeartbeatWriter, writer),
        ServeOptions(clock=clock),
    )
    return loop, writer


def test_heartbeat_interval_uses_monotonic_clock(
    connection: sqlite3.Connection,
) -> None:
    clock = VirtualClock(NOW)
    loop, writer = make_loop(connection, clock)

    loop._write_heartbeat_if_due()
    assert writer.writes == [(NOW, None)]

    clock.advance(timedelta(seconds=59))
    loop._write_heartbeat_if_due()
    assert len(writer.writes) == 1

    clock.advance(timedelta(seconds=1))
    loop._write_heartbeat_if_due()
    assert [last_tick_at for last_tick_at, _active in writer.writes] == [
        NOW,
        NOW + timedelta(seconds=60),
    ]
