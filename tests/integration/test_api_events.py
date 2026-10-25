"""SSE 事件流：/api/v1/events 的帧格式、事件 id、断线续传与 reset（docs/12 SSE 一节）。"""

from __future__ import annotations

import json
import socket
import sqlite3
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import EventType
from core.api.server import ApiOptions, ApiServer, start_api_server
from core.api.store import TOKEN_KEY, put_settings
from core.clock import VirtualClock
from storage import migrate, open_database
from storage.repositories.events import EventLog

NOW = datetime(2026, 10, 26, 10, 0, tzinfo=UTC)
TOKEN = "secret-token"
READ_TIMEOUT = 5.0
Frame = tuple[str | None, str, dict[str, Any]]
Env = tuple[ApiServer, sqlite3.Connection, VirtualClock]


class SseClient:
    """测试用 SSE 客户端：裸套接字逐帧收，可带 Last-Event-ID 重连。"""

    def __init__(
        self,
        handle: ApiServer,
        *,
        last_event_id: str | None = None,
        token: str = TOKEN,
    ) -> None:
        host, port = handle.address
        self._socket = socket.create_connection((host, port), timeout=READ_TIMEOUT)
        lines = ["GET /api/v1/events HTTP/1.0", f"Host: {host}"]
        lines.append(f"Authorization: Bearer {token}")
        if last_event_id is not None:
            lines.append(f"Last-Event-ID: {last_event_id}")
        self._socket.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("utf-8"))
        self._buffer = b""
        self.status_line = self._read_line()
        while self._read_line():
            pass

    def next_frame(self, timeout: float = READ_TIMEOUT) -> Frame:
        """读下一完整帧，返回 (id, event, data)；id 为 None 表示 advisory 帧。"""
        deadline = time.monotonic() + timeout
        while b"\n\n" not in self._buffer:
            if time.monotonic() > deadline:
                raise AssertionError(f"{timeout:g} 秒内没有完整帧；缓冲：{self._buffer!r}")
            chunk = self._recv()
            self._buffer += chunk
        raw, self._buffer = self._buffer.split(b"\n\n", 1)
        return parse_frame(raw)

    def read_until_closed(self, timeout: float = READ_TIMEOUT) -> list[Frame]:
        """读到服务端关连接为止；关连接时的残余字节必须恰好是完整帧。"""
        frames: list[Frame] = []
        self._socket.settimeout(timeout)
        try:
            while True:
                chunk = self._socket.recv(4096)
                if not chunk:
                    break
                self._buffer += chunk
                while b"\n\n" in self._buffer:
                    raw, self._buffer = self._buffer.split(b"\n\n", 1)
                    frames.append(parse_frame(raw))
        except TimeoutError:
            raise AssertionError(
                f"{timeout:g} 秒内服务端未关闭连接；缓冲：{self._buffer!r}"
            ) from None
        assert self._buffer == b"", f"连接关闭时留下半帧：{self._buffer!r}"
        return frames

    def close(self) -> None:
        self._socket.close()

    def _recv(self) -> bytes:
        chunk = self._socket.recv(4096)
        if not chunk:
            raise AssertionError(f"连接已关闭；缓冲：{self._buffer!r}")
        return chunk

    def _read_line(self) -> str:
        while b"\r\n" not in self._buffer:
            self._buffer += self._recv()
        line, self._buffer = self._buffer.split(b"\r\n", 1)
        return line.decode("utf-8", errors="replace")


def parse_frame(raw: bytes) -> Frame:
    event_id: str | None = None
    event_type = ""
    data = ""
    for line in raw.decode("utf-8").split("\n"):
        if line.startswith("id: "):
            event_id = line[len("id: ") :]
        elif line.startswith("event: "):
            event_type = line[len("event: ") :]
        elif line.startswith("data: "):
            data = line[len("data: ") :]
    return event_id, event_type, json.loads(data)


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    database = tmp_path / "state.db"
    boot = open_database(database)
    try:
        migrate(boot)
        put_settings(boot, {TOKEN_KEY: TOKEN}, at=NOW)
        _insert_run(boot)
    finally:
        boot.close()
    clock = VirtualClock(NOW)
    writer = open_database(database)
    handle = start_api_server(writer, ApiOptions(host="127.0.0.1", port=0, clock=clock))
    try:
        yield handle, writer, clock
    finally:
        handle.shutdown()
        writer.close()


def _insert_run(connection: sqlite3.Connection) -> None:
    """落一条 run_events 外键需要的模板与运行行。"""
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES ('template-1', '日常', 'fake', 'env-1', '{}', '1', "
        "'{}', '{}', 1, ?, ?)",
        (NOW.isoformat(), NOW.isoformat()),
    )
    connection.execute(
        "INSERT INTO job_runs (id, template_id, schedule_id, scheduled_at, created_at, "
        "idempotency_key, state) VALUES ('run-1', 'template-1', NULL, ?, ?, 'sse-key', 'queued')",
        (NOW.isoformat(), NOW.isoformat()),
    )


def global_event(writer: sqlite3.Connection, clock: VirtualClock, detail: str) -> None:
    EventLog(writer).append_global(
        EventType.CORE_EVENT,
        {"event": "core.config_migrated", "detail": detail},
        at=clock.utc_now(),
    )


def run_event(writer: sqlite3.Connection, clock: VirtualClock, run_id: str) -> None:
    clock.advance(timedelta(seconds=1))
    EventLog(writer).append_run(
        run_id,
        EventType.RUN_STATE_CHANGED,
        {"run_id": run_id, "from_state": "queued", "to_state": "executing", "reason": "test"},
        at=clock.utc_now(),
    )


def test_first_frame_is_heartbeat(env: Env) -> None:
    handle, _, _ = env
    client = SseClient(handle)
    try:
        started = time.monotonic()
        event_id, event_type, data = client.next_frame()
        assert time.monotonic() - started < 1.0
        assert event_type == "heartbeat"
        assert event_id is None
        datetime.fromisoformat(data["ts"])
    finally:
        client.close()


def test_stream_without_token_gets_401(env: Env) -> None:
    handle, _, _ = env
    client = SseClient(handle, token="wrong-token")
    try:
        assert client.status_line.startswith("HTTP/1.0 401")
    finally:
        client.close()


def test_events_carry_documented_ids(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle)
    try:
        client.next_frame()
        global_event(writer, clock, "first")
        run_event(writer, clock, "run-1")
        assert client.next_frame() == (
            "g:1",
            "core.event",
            {"event": "core.config_migrated", "detail": "first"},
        )
        assert client.next_frame() == (
            "r:run-1:1",
            "run.state.changed",
            {"run_id": "run-1", "from_state": "queued", "to_state": "executing", "reason": "test"},
        )
    finally:
        client.close()


def test_reconnect_backfills_only_missing_segment(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle)
    try:
        client.next_frame()
        for index in range(3):
            global_event(writer, clock, f"e{index}")
        assert [client.next_frame()[0] for _ in range(3)] == ["g:1", "g:2", "g:3"]
    finally:
        client.close()
    client = SseClient(handle, last_event_id="g:3")
    try:
        assert client.next_frame()[0] is None
        global_event(writer, clock, "e3")
        global_event(writer, clock, "e4")
        assert [client.next_frame()[0] for _ in range(2)] == ["g:4", "g:5"]
    finally:
        client.close()


def test_reconnect_with_run_anchor_backfills_cross_source(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle)
    try:
        client.next_frame()
        run_event(writer, clock, "run-1")
        assert client.next_frame()[0] == "r:run-1:1"
    finally:
        client.close()
    # 掉线期间两表都有新事件；r 锚点按锚点时刻补两段，锚点同刻帧重发、客户端按 id 去重
    global_event(writer, clock, "while-away")
    run_event(writer, clock, "run-1")
    client = SseClient(handle, last_event_id="r:run-1:1")
    try:
        assert client.next_frame()[0] is None
        ids = [client.next_frame()[0] for _ in range(3)]
        assert ids == ["g:1", "r:run-1:1", "r:run-1:2"]
    finally:
        client.close()


def test_reconnect_reset_when_retention_cannot_cover(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle)
    try:
        client.next_frame()
        for index in range(3):
            global_event(writer, clock, f"e{index}")
        for _ in range(3):
            client.next_frame()
    finally:
        client.close()
    writer.execute("DELETE FROM events_global WHERE seq <= 3")
    client = SseClient(handle, last_event_id="g:3")
    try:
        assert client.next_frame()[0] is None
        _, event_type, data = client.next_frame()
        assert event_type == "reset"
        assert "/api/v1/status" in data["reason"]
        global_event(writer, clock, "after-reset")
        assert client.next_frame()[0] == "g:4"
    finally:
        client.close()


def test_malformed_last_event_id_gets_400(env: Env) -> None:
    handle, _, _ = env
    client = SseClient(handle, last_event_id="not-an-event-id")
    try:
        assert client.status_line.startswith("HTTP/1.0 400")
    finally:
        client.close()


def test_midstream_reset_when_retention_prunes_unseen_events(env: Env) -> None:
    """连接存续期间保留窗口跳变：发 reset 提示后继续追新，客户端重拉快照。"""
    handle, writer, clock = env
    client = SseClient(handle)
    try:
        client.next_frame()
        for index in range(3):
            global_event(writer, clock, f"e{index}")
        for _ in range(3):
            client.next_frame()
        # g:4 落库但从未投递就被清出保留窗口：下一条到达时检测到缺口
        global_event(writer, clock, "pruned")
        writer.execute("DELETE FROM events_global WHERE seq <= 4")
        global_event(writer, clock, "after-prune")
        _, event_type, _ = client.next_frame()
        assert event_type == "reset"
        assert client.next_frame()[0] == "g:5"
        run_event(writer, clock, "run-1")
        run_event(writer, clock, "run-1")
        for _ in range(2):
            client.next_frame()
        # 运行事件整表清空后 rowid 从头计：水位失效，同样只发 reset 再追新
        writer.execute("DELETE FROM run_events")
        run_event(writer, clock, "run-1")
        _, event_type, _ = client.next_frame()
        assert event_type == "reset"
    finally:
        client.close()


def test_shutdown_flushes_committed_events_without_partial_frames(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle)
    client.next_frame()
    for index in range(4):
        global_event(writer, clock, f"e{index}")
    handle.shutdown()
    frames = client.read_until_closed()
    ids = [frame[0] for frame in frames if frame[0] is not None]
    assert ids == ["g:1", "g:2", "g:3", "g:4"]
