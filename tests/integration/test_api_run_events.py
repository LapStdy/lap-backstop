"""单运行 SSE 事件流：/api/v1/runs/{id}/events 的隔离、续传、reset 与错误口径（docs/03）。"""

from __future__ import annotations

import json
import socket
import sqlite3
import threading
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
THREAD_RELEASE_TIMEOUT = 5.0
Frame = tuple[str | None, str, dict[str, Any]]
Env = tuple[ApiServer, sqlite3.Connection, VirtualClock]


class SseClient:
    """测试用 SSE 客户端：裸套接字逐帧收，可指定流路径与 Last-Event-ID。"""

    def __init__(
        self,
        handle: ApiServer,
        *,
        path: str,
        last_event_id: str | None = None,
        token: str | None = TOKEN,
    ) -> None:
        host, port = handle.address
        self._socket = socket.create_connection((host, port), timeout=READ_TIMEOUT)
        lines = [f"GET {path} HTTP/1.0", f"Host: {host}"]
        if token is not None:
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
            self._buffer += self._recv()
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


def run_stream_path(run_id: str) -> str:
    return f"/api/v1/runs/{run_id}/events"


@pytest.fixture
def env(tmp_path: Path) -> Iterator[Env]:
    database = tmp_path / "state.db"
    boot = open_database(database)
    try:
        migrate(boot)
        put_settings(boot, {TOKEN_KEY: TOKEN}, at=NOW)
        _insert_runs(boot)
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


def _insert_runs(connection: sqlite3.Connection) -> None:
    """落 run_events 外键需要的模板与两条运行行。"""
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES ('template-1', '日常', 'fake', 'env-1', '{}', '1', "
        "'{}', '{}', 1, ?, ?)",
        (NOW.isoformat(), NOW.isoformat()),
    )
    for run_id, key in (("run-1", "sse-key-1"), ("run-2", "sse-key-2")):
        connection.execute(
            "INSERT INTO job_runs (id, template_id, schedule_id, scheduled_at, created_at, "
            "idempotency_key, state) VALUES (?, 'template-1', NULL, ?, ?, ?, 'queued')",
            (run_id, NOW.isoformat(), NOW.isoformat(), key),
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


def test_run_stream_pushes_only_that_run(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle, path=run_stream_path("run-1"))
    try:
        assert client.next_frame()[1] == "heartbeat"
        run_event(writer, clock, "run-1")
        assert client.next_frame() == (
            "r:run-1:1",
            "run.state.changed",
            {"run_id": "run-1", "from_state": "queued", "to_state": "executing", "reason": "test"},
        )
        # 别的运行与全局事件都落库后，本流下一帧仍是自己的事件，不串流
        global_event(writer, clock, "noise")
        run_event(writer, clock, "run-2")
        run_event(writer, clock, "run-1")
        assert client.next_frame()[0] == "r:run-1:2"
    finally:
        client.close()


def test_run_stream_resume_backfills_only_that_run(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle, path=run_stream_path("run-1"))
    try:
        client.next_frame()
        run_event(writer, clock, "run-1")
        assert client.next_frame()[0] == "r:run-1:1"
    finally:
        client.close()
    # 掉线期间三个来源都有新事件；重连只补本运行缺失段
    global_event(writer, clock, "while-away")
    run_event(writer, clock, "run-2")
    run_event(writer, clock, "run-1")
    run_event(writer, clock, "run-1")
    client = SseClient(
        handle,
        path=run_stream_path("run-1"),
        last_event_id="r:run-1:1",
    )
    try:
        assert client.next_frame()[0] is None
        assert [client.next_frame()[0] for _ in range(2)] == ["r:run-1:2", "r:run-1:3"]
        run_event(writer, clock, "run-1")
        assert client.next_frame()[0] == "r:run-1:4"
    finally:
        client.close()


def test_run_stream_reset_when_anchor_missing(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle, path=run_stream_path("run-1"))
    try:
        client.next_frame()
        for _ in range(3):
            run_event(writer, clock, "run-1")
        assert [client.next_frame()[0] for _ in range(3)] == [
            "r:run-1:1",
            "r:run-1:2",
            "r:run-1:3",
        ]
    finally:
        client.close()
    writer.execute("DELETE FROM run_events WHERE run_id = 'run-1' AND seq <= 1")
    client = SseClient(
        handle,
        path=run_stream_path("run-1"),
        last_event_id="r:run-1:1",
    )
    try:
        assert client.next_frame()[0] is None
        _, event_type, data = client.next_frame()
        assert event_type == "reset"
        assert "/api/v1/status" in data["reason"]
        run_event(writer, clock, "run-1")
        assert client.next_frame()[0] == "r:run-1:4"
    finally:
        client.close()


def test_run_stream_reset_for_anchor_of_other_source(env: Env) -> None:
    """g 锚点与别的运行的 r 锚点都不属于本流：一律 reset，不借它源补帧。"""
    handle, writer, clock = env
    run_event(writer, clock, "run-2")
    for last_event_id in ("g:1", "r:run-2:1"):
        client = SseClient(
            handle,
            path=run_stream_path("run-1"),
            last_event_id=last_event_id,
        )
        try:
            assert client.next_frame()[0] is None
            assert client.next_frame()[1] == "reset"
            run_event(writer, clock, "run-1")
            assert client.next_frame()[0] == "r:run-1:1"
        finally:
            client.close()
        writer.execute("DELETE FROM run_events WHERE run_id = 'run-1'")


def test_unknown_run_gets_404(env: Env) -> None:
    handle, _, _ = env
    client = SseClient(handle, path=run_stream_path("ghost"))
    try:
        assert client.status_line.startswith("HTTP/1.0 404")
    finally:
        client.close()


def test_run_stream_without_token_gets_401(env: Env) -> None:
    handle, _, _ = env
    client = SseClient(handle, path=run_stream_path("run-1"), token="wrong-token")
    try:
        assert client.status_line.startswith("HTTP/1.0 401")
    finally:
        client.close()


def test_run_stream_without_credentials_gets_401(env: Env) -> None:
    handle, _, _ = env
    client = SseClient(handle, path=run_stream_path("run-1"), token=None)
    try:
        assert client.status_line.startswith("HTTP/1.0 401")
    finally:
        client.close()


def test_run_stream_malformed_last_event_id_gets_400(env: Env) -> None:
    handle, _, _ = env
    client = SseClient(
        handle,
        path=run_stream_path("run-1"),
        last_event_id="not-an-event-id",
    )
    try:
        assert client.status_line.startswith("HTTP/1.0 400")
    finally:
        client.close()


def test_empty_run_stream_heartbeats_then_delivers(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle, path=run_stream_path("run-2"))
    try:
        event_id, event_type, _ = client.next_frame()
        assert event_id is None
        assert event_type == "heartbeat"
        run_event(writer, clock, "run-2")
        assert client.next_frame()[0] == "r:run-2:1"
    finally:
        client.close()


def test_run_stream_and_global_stream_coexist(env: Env) -> None:
    handle, writer, clock = env
    run_client = SseClient(handle, path=run_stream_path("run-1"))
    global_client = SseClient(handle, path="/api/v1/events")
    try:
        run_client.next_frame()
        global_client.next_frame()
        global_event(writer, clock, "shared")
        run_event(writer, clock, "run-1")
        run_event(writer, clock, "run-2")
        assert run_client.next_frame()[0] == "r:run-1:1"
        assert [global_client.next_frame()[0] for _ in range(3)] == [
            "g:1",
            "r:run-1:1",
            "r:run-2:1",
        ]
    finally:
        run_client.close()
        global_client.close()


def test_shutdown_flushes_run_events_then_closes(env: Env) -> None:
    handle, writer, clock = env
    client = SseClient(handle, path=run_stream_path("run-1"))
    client.next_frame()
    for _ in range(2):
        run_event(writer, clock, "run-1")
    handle.shutdown()
    frames = client.read_until_closed()
    assert [frame[0] for frame in frames if frame[0] is not None] == ["r:run-1:1", "r:run-1:2"]


def test_client_disconnect_releases_stream_thread(env: Env) -> None:
    handle, writer, clock = env
    baseline = threading.active_count()
    client = SseClient(handle, path=run_stream_path("run-1"))
    try:
        client.next_frame()
    finally:
        client.close()
    # 断开后继续落事件逼出对已关套接字的写失败，处理线程随之退出
    for _ in range(3):
        run_event(writer, clock, "run-1")
    deadline = time.monotonic() + THREAD_RELEASE_TIMEOUT
    while threading.active_count() > baseline and time.monotonic() < deadline:
        time.sleep(0.05)
    assert threading.active_count() <= baseline, "流处理线程在客户端断开后仍未退出"
