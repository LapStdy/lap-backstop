"""SSE 事件流：/api/v1/events 的帧编码、断线续传定位与轮询生成。

docs/03 把事件分落两张表：运行事件 ``r:<run_id>:<seq>`` 在 run_events，全局
事件 ``g:<seq>`` 在 events_global。流的两份来源各自严格按 seq/落库序交付；
重连按 Last-Event-ID 锚点时刻补两表，同刻已交付的事件会重发，客户端按 id
去重吸收；保留窗口盖不住锚点时发 reset，客户端重拉 GET /api/v1/status。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from contracts import Clock, global_event_id, parse_event_id
from core.api.wire import encode_json
from core.clock import SystemClock
from storage import open_database
from storage.repositories.events import StoredEvent
from storage.repositories.iso_time import format_at, parse_at

HEARTBEAT_EVENT = "heartbeat"
RESET_EVENT = "reset"
# 前缀 STREAM_ 与 server.POLL_INTERVAL_SECONDS（accept 循环唤醒）区分：两者量级与用途不同。
STREAM_HEARTBEAT_INTERVAL_SECONDS = 15.0
STREAM_POLL_INTERVAL_SECONDS = 0.5
RESET_REASON_RESUME = "Last-Event-ID 超出事件保留窗口，请重拉 /api/v1/status 重建快照"
RESET_REASON_LIVE = "事件保留窗口前进过多，请重拉 /api/v1/status 重建快照"


def sse_frame(
    event_type: str,
    payload: Mapping[str, Any],
    *,
    event_id: str | None = None,
) -> bytes:
    """编码一帧 SSE；advisory 帧（心跳、reset）不写 id，不推动客户端游标。"""
    lines = [] if event_id is None else [f"id: {event_id}"]
    lines.append(f"event: {event_type}")
    lines.append(f"data: {encode_json(payload).decode('utf-8')}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


def _event_frames(events: Iterable[StoredEvent]) -> list[bytes]:
    """一批事件到帧；补发与轮询共用同一组装。"""
    return [sse_frame(e.type, e.payload, event_id=e.id) for e in events]


@dataclass(frozen=True, slots=True)
class StreamCursors:
    """两份来源各自的已交付水位：全局事件按 seq，运行事件按落库 rowid。"""

    global_seq: int
    run_rowid: int


@dataclass(frozen=True, slots=True)
class StreamOptions:
    """事件流的注入项：时钟、停机事件与循环旋钮；生产路径全用默认值。"""

    clock: Clock = field(default_factory=SystemClock)
    stop: threading.Event = field(default_factory=threading.Event)
    last_event_id: str | None = None
    poll_interval_seconds: float = STREAM_POLL_INTERVAL_SECONDS
    heartbeat_interval_seconds: float = STREAM_HEARTBEAT_INTERVAL_SECONDS


def stream_frames(database: Path, options: StreamOptions) -> Iterator[bytes]:
    """事件流帧迭代器：连接即发心跳，可续传则先补发，随后轮询两表到停机。

    停机时最后冲刷一轮已落库事件再结束；此后落库的事件由客户端重连补发。
    """
    connection = open_database(database)
    try:
        # 水位在发出首帧前定死：连接时刻已落库的事件不属于本连接，由重连补发。
        cursors = _current_cursors(connection)
        yield sse_frame(HEARTBEAT_EVENT, {"ts": format_at(options.clock.utc_now())})
        if options.last_event_id is not None:
            backfill = _resume_backfill(connection, options.last_event_id)
            if backfill is None:
                yield sse_frame(RESET_EVENT, {"reason": RESET_REASON_RESUME})
            else:
                yield from backfill.frames
                cursors = replace(
                    cursors,
                    global_seq=max(cursors.global_seq, backfill.global_seq),
                    run_rowid=max(cursors.run_rowid, backfill.run_rowid),
                )
        yield from _poll_frames(connection, options, cursors)
    finally:
        connection.close()


@dataclass(frozen=True, slots=True)
class Backfill:
    """一次续传补发的结果：帧与其覆盖到的两表水位。"""

    frames: tuple[bytes, ...]
    global_seq: int
    run_rowid: int


def _resume_backfill(
    connection: sqlite3.Connection,
    last_event_id: str,
) -> Backfill | None:
    """按锚点事件定位续传起点；补不齐返回 None。

    锚点同源的段按精确序号补（g 锚点的全局事件从 ``seq > N`` 起）；跨源的段
    没有共同序号，从 ``ts >= 锚点 ts`` 起补，多补的同刻帧由客户端按 id 去重，
    少补则是真丢失。返回的水位用于衔接轮询，补过的帧不再重发。
    """
    try:
        scope, ref, seq = parse_event_id(last_event_id)
    except ValueError:
        return None
    anchor_ts = _anchor_ts(connection, scope, ref, seq)
    if anchor_ts is None:
        return None
    oldest_global = _scalar(connection, "SELECT MIN(ts) FROM events_global")
    if oldest_global is not None and str(oldest_global) > anchor_ts:
        return None
    global_events = (
        _global_after_seq(connection, seq)
        if scope == "g"
        else _global_from_ts(connection, anchor_ts)
    )
    run_rows = _run_events_from_ts(connection, anchor_ts)
    frames = _event_frames(global_events)
    frames.extend(_event_frames(event for _, event in run_rows))
    return Backfill(
        frames=tuple(frames),
        global_seq=global_events[-1].seq if global_events else 0,
        run_rowid=run_rows[-1][0] if run_rows else 0,
    )


def _poll_frames(
    connection: sqlite3.Connection,
    options: StreamOptions,
    cursors: StreamCursors,
) -> Iterator[bytes]:
    """轮询循环：每轮交出新增帧；停机事件置位后再冲刷一轮才结束。"""
    clock = options.clock
    next_heartbeat_at = clock.monotonic() + options.heartbeat_interval_seconds
    while True:
        frames, cursors, reset = _drain(connection, cursors)
        if reset:
            yield sse_frame(RESET_EVENT, {"reason": RESET_REASON_LIVE})
        yield from frames
        if options.stop.wait(options.poll_interval_seconds):
            frames, _, _ = _drain(connection, cursors)
            yield from frames
            return
        now = clock.monotonic()
        if now >= next_heartbeat_at:
            yield sse_frame(HEARTBEAT_EVENT, {"ts": format_at(clock.utc_now())})
            next_heartbeat_at = now + options.heartbeat_interval_seconds


def _drain(
    connection: sqlite3.Connection,
    cursors: StreamCursors,
) -> tuple[list[bytes], StreamCursors, bool]:
    """读两表水位之后的事件成帧并推进水位；保留窗口跳变时报告需要 reset。"""
    reset = False
    global_rows = _global_after_seq(connection, cursors.global_seq)
    run_rows = _run_events_after_rowid(connection, cursors.run_rowid)
    min_seq = _int_scalar(connection, "SELECT MIN(seq) FROM events_global")
    if cursors.global_seq > 0 and min_seq is not None and min_seq > cursors.global_seq + 1:
        reset = True
        if not global_rows:
            max_seq = _int_scalar(connection, "SELECT MAX(seq) FROM events_global")
            cursors = replace(cursors, global_seq=0 if max_seq is None else max_seq)
    max_rowid = _int_scalar(connection, "SELECT MAX(rowid) FROM run_events")
    if max_rowid is not None and max_rowid < cursors.run_rowid:
        # run_events 的 rowid 不是 AUTOINCREMENT，整表清空后序号从头计，水位先归位再追新。
        reset = True
        if not run_rows:
            cursors = replace(cursors, run_rowid=max_rowid)
    if global_rows:
        cursors = replace(cursors, global_seq=global_rows[-1].seq)
    if run_rows:
        cursors = replace(cursors, run_rowid=run_rows[-1][0])
    frames = _event_frames(global_rows)
    frames.extend(_event_frames(event for _, event in run_rows))
    return frames, cursors, reset


def _anchor_ts(connection: sqlite3.Connection, scope: str, ref: str, seq: int) -> str | None:
    """锚点事件的落库时刻；已被清理或本来不存在返回 None。"""
    if scope == "g":
        row = connection.execute("SELECT ts FROM events_global WHERE seq = ?", (seq,)).fetchone()
    else:
        row = connection.execute(
            "SELECT ts FROM run_events WHERE run_id = ? AND seq = ?",
            (ref, seq),
        ).fetchone()
    return None if row is None else str(row[0])


def _current_cursors(connection: sqlite3.Connection) -> StreamCursors:
    """两张表当前的尾水位；新连接从这里追新，不重放历史。"""
    seq = _int_scalar(connection, "SELECT MAX(seq) FROM events_global")
    rowid = _int_scalar(connection, "SELECT MAX(rowid) FROM run_events")
    return StreamCursors(
        global_seq=0 if seq is None else seq,
        run_rowid=0 if rowid is None else rowid,
    )


def _global_after_seq(connection: sqlite3.Connection, seq: int) -> tuple[StoredEvent, ...]:
    rows = connection.execute(
        "SELECT seq, type, ts, payload FROM events_global WHERE seq > ? ORDER BY seq",
        (seq,),
    ).fetchall()
    return tuple(_global_event(row) for row in rows)


def _global_from_ts(connection: sqlite3.Connection, ts: str) -> tuple[StoredEvent, ...]:
    rows = connection.execute(
        "SELECT seq, type, ts, payload FROM events_global WHERE ts >= ? ORDER BY seq",
        (ts,),
    ).fetchall()
    return tuple(_global_event(row) for row in rows)


def _run_events_after_rowid(
    connection: sqlite3.Connection,
    rowid: int,
) -> tuple[tuple[int, StoredEvent], ...]:
    rows = connection.execute(
        "SELECT rowid, id, run_id, seq, type, ts, payload FROM run_events "
        "WHERE rowid > ? ORDER BY rowid",
        (rowid,),
    ).fetchall()
    return tuple((int(row["rowid"]), _run_event(row)) for row in rows)


def _run_events_from_ts(
    connection: sqlite3.Connection,
    ts: str,
) -> tuple[tuple[int, StoredEvent], ...]:
    rows = connection.execute(
        "SELECT rowid, id, run_id, seq, type, ts, payload FROM run_events "
        "WHERE ts >= ? ORDER BY rowid",
        (ts,),
    ).fetchall()
    return tuple((int(row["rowid"]), _run_event(row)) for row in rows)


def _global_event(row: sqlite3.Row) -> StoredEvent:
    seq = int(row["seq"])
    return StoredEvent(
        id=global_event_id(seq),
        run_id=None,
        seq=seq,
        type=str(row["type"]),
        at=parse_at(str(row["ts"])),
        payload=_payload(row["payload"]),
    )


def _run_event(row: sqlite3.Row) -> StoredEvent:
    return StoredEvent(
        id=str(row["id"]),
        run_id=str(row["run_id"]),
        seq=int(row["seq"]),
        type=str(row["type"]),
        at=parse_at(str(row["ts"])),
        payload=_payload(row["payload"]),
    )


def _scalar(connection: sqlite3.Connection, sql: str) -> object:
    row = connection.execute(sql).fetchone()
    return None if row is None else row[0]


def _int_scalar(connection: sqlite3.Connection, sql: str) -> int | None:
    """整数聚合的标量读；空表返回 None。"""
    value = _scalar(connection, sql)
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raise sqlite3.DatabaseError(f"期望整数标量，读到：{value!r}")


def _payload(text: object) -> dict[str, Any]:
    payload = json.loads(str(text))
    if not isinstance(payload, dict):
        raise sqlite3.DatabaseError("事件载荷不是 JSON 对象")
    return payload


__all__ = [
    "HEARTBEAT_EVENT",
    "RESET_EVENT",
    "STREAM_HEARTBEAT_INTERVAL_SECONDS",
    "STREAM_POLL_INTERVAL_SECONDS",
    "StreamCursors",
    "StreamOptions",
    "sse_frame",
    "stream_frames",
]
