"""事件流生成器：注入旋钮下的心跳节拍（docs/03 SSE 心跳每 15 秒）。"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from pathlib import Path

from contracts import EventType
from core.api.events_api import StreamOptions, stream_frames
from core.clock import VirtualClock
from storage import migrate, open_database
from storage.repositories.events import EventLog

NOW = datetime(2026, 10, 26, 10, 0, tzinfo=UTC)


def test_heartbeat_emits_per_interval(tmp_path: Path) -> None:
    """心跳按注入的间隔重复出现；零间隔下每轮轮询后都该有一帧心跳。"""
    database = tmp_path / "state.db"
    boot = open_database(database)
    try:
        migrate(boot)
    finally:
        boot.close()
    clock = VirtualClock(NOW)
    writer = open_database(database)
    stop = threading.Event()
    frames = stream_frames(
        database,
        StreamOptions(
            clock=clock,
            stop=stop,
            poll_interval_seconds=0.0,
            heartbeat_interval_seconds=0.0,
        ),
    )
    try:
        head = frames.__next__()
        assert b"event: heartbeat" in head
        EventLog(writer).append_global(
            EventType.CORE_EVENT,
            {"event": "core.config_migrated", "detail": "e0"},
            at=clock.utc_now(),
        )
        assert b"id: g:1" in frames.__next__()
        for _ in range(2):
            assert b"event: heartbeat" in frames.__next__()
    finally:
        stop.set()
        writer.close()
