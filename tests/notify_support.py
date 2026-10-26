"""通知服务集成测试的共用素材：假渠道、fixtures、事件注入与落库断言。"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from contracts import EventType, Notification, RunContext
from contracts.types import DeliveryResult

NOW = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
TEMPLATE_ID = "template-1"
ENABLED: dict[str, Any] = {
    "notify.enabled": True,
    "notify.channel": "serverchan",
    "notify.token": "SCT123",
}


class StubChannel:
    """记录投递并按剧本应答的假渠道；首次 send 置事件供用例等待。"""

    def __init__(self) -> None:
        self.sent: list[Notification] = []
        self.results: list[DeliveryResult] = []
        self.delivered = threading.Event()

    def send(self, ctx: RunContext, message: Notification) -> DeliveryResult:
        self.sent.append(message)
        self.delivered.set()
        if self.results:
            return self.results.pop(0)
        return DeliveryResult(ok=True)


def insert_template(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO job_templates (id, name, plugin_id, environment_id, "
        "config_schema_version, verification_policy, recovery_policy, enabled, "
        "created_at, updated_at) VALUES (?, '日常', 'plugin-1', 'env-1', '1', '{}', "
        "'{}', 1, ?, ?)",
        (TEMPLATE_ID, "2026-10-26T00:00:00+00:00", "2026-10-26T00:00:00+00:00"),
    )


def create_run(connection: sqlite3.Connection, key: str) -> str:
    from storage.repositories.job_runs import JobRunRepository, NewRun

    record, _created = JobRunRepository(connection).create_or_get(
        NewRun(
            template_id=TEMPLATE_ID,
            schedule_id=None,
            scheduled_at=None,
            created_at=NOW,
            idempotency_key=key,
            priority=0,
            deadline_at=None,
        )
    )
    return record.id


def append_run_event(
    connection: sqlite3.Connection, run_id: str, event_type: EventType, payload: dict[str, Any]
) -> None:
    from storage.repositories.events import EventLog

    EventLog(connection).append_run(run_id, event_type, payload, at=NOW)


def append_global_event(
    connection: sqlite3.Connection, event_type: EventType, payload: dict[str, Any]
) -> None:
    from storage.repositories.events import EventLog

    EventLog(connection).append_global(event_type, payload, at=NOW)


def run_event_payloads(
    connection: sqlite3.Connection, event_type: EventType
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT payload FROM run_events WHERE type = ? ORDER BY rowid",
        (event_type.value,),
    ).fetchall()
    return [json.loads(str(row["payload"])) for row in rows]


def core_events(connection: sqlite3.Connection, name: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT payload FROM events_global WHERE type = ? ORDER BY seq",
        (EventType.CORE_EVENT.value,),
    ).fetchall()
    payloads = [json.loads(str(row["payload"])) for row in rows]
    return [payload for payload in payloads if payload.get("event") == name]


def wait_until(predicate: Callable[[], bool], timeout_seconds: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def record_outcomes_within(
    service: Any, predicate: Callable[[], bool], timeout_seconds: float = 2.0
) -> bool:
    """反复 poll 直到结果落库；轮询即推进，规避线程入队与读取的竞态。"""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        service.poll()
        if predicate():
            return True
        time.sleep(0.01)
    return False
