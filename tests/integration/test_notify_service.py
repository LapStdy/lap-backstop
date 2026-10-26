"""通知服务集成：关键事件映射、投递结果簿记与同运行去重。

事件在建服务之后追加（游标从当前末尾起算，不重推历史）；
落库断言用 record_outcomes_within 反复 poll，规避工作线程入队竞态。
fixtures（connection、stub、service_factory）见同目录 conftest.py。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Any

from contracts import EventType, EvidenceKind, EvidenceSource, NotificationLevel
from contracts.types import DeliveryResult
from core.hostops.tailscale import TAILSCALE_DOWN_EVENT
from core.hostops.watchdog import WATCHDOG_GAVE_UP_EVENT
from storage.repositories.evidence import EvidenceRepository, EvidenceRow
from tests.notify_support import (
    NOW,
    append_global_event,
    append_run_event,
    core_events,
    create_run,
    record_outcomes_within,
    run_event_payloads,
    wait_until,
)


def test_run_finished_success_notifies_and_records_sent(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    service = service_factory()
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "succeeded", "duration_ms": 65000},
    )
    service.poll()
    assert stub.delivered.wait(2.0)
    notification = stub.sent[0]
    assert notification.level is NotificationLevel.INFO
    assert "运行成功" in notification.title
    assert "日常" in notification.title
    assert f"运行：{run_id}" in notification.body
    assert "耗时：1 分 5 秒" in notification.body
    assert f"证据：evidence/{run_id}" in notification.body
    assert record_outcomes_within(
        service, lambda: len(run_event_payloads(connection, EventType.NOTIFY_SENT)) == 1
    )
    payload = run_event_payloads(connection, EventType.NOTIFY_SENT)[0]
    assert payload == {"run_id": run_id, "channel": "serverchan", "result": "ok"}


def test_failed_run_carries_error_code_and_evidence_ids(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:failed")
    connection.execute("UPDATE job_runs SET error_code = 'E_DEADLINE' WHERE id = ?", (run_id,))
    EvidenceRepository(connection).insert(
        EvidenceRow(
            id="ev-1",
            run_id=run_id,
            kind=EvidenceKind.LOG,
            source=EvidenceSource.KERNEL,
            path="runs/ev-1.log",
            sha256="0" * 64,
            summary="",
            created_at=NOW,
        )
    )
    service = service_factory()
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1500},
    )
    service.poll()
    assert stub.delivered.wait(2.0)
    notification = stub.sent[0]
    assert notification.level is NotificationLevel.ERROR
    assert "运行失败" in notification.title
    assert "错误码：E_DEADLINE" in notification.body
    assert notification.evidence_ids == ("ev-1",)


def test_unknown_run_notifies_as_warn_and_cancelled_is_skipped(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    service = service_factory()
    unknown_run = create_run(connection, "notify:unknown")
    append_run_event(
        connection,
        unknown_run,
        EventType.RUN_FINISHED,
        {"run_id": unknown_run, "status": "unknown", "duration_ms": None},
    )
    cancelled_run = create_run(connection, "notify:cancelled")
    append_run_event(
        connection,
        cancelled_run,
        EventType.RUN_FINISHED,
        {"run_id": cancelled_run, "status": "cancelled", "duration_ms": None},
    )
    service.poll()
    assert record_outcomes_within(
        service, lambda: len(run_event_payloads(connection, EventType.NOTIFY_SENT)) == 1
    )
    assert len(stub.sent) == 1
    assert stub.sent[0].level is NotificationLevel.WARN
    assert "结果未知" in stub.sent[0].title


def test_exhausted_recovery_dedupes_with_run_end(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    service = service_factory()
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1000},
    )
    append_run_event(
        connection,
        run_id,
        EventType.RECOVERY_FINISHED,
        {"run_id": run_id, "action": "abort", "result": "exhausted"},
    )
    service.poll()
    assert record_outcomes_within(
        service, lambda: len(run_event_payloads(connection, EventType.NOTIFY_SENT)) == 1
    )
    assert len(stub.sent) == 1
    assert "运行失败" in stub.sent[0].title


def test_failed_recovery_action_pushes_once(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    connection.execute(
        "UPDATE job_runs SET started_at = ?, finished_at = ? WHERE id = ?",
        ("2026-10-26T11:58:00+00:00", "2026-10-26T12:00:00+00:00", run_id),
    )
    service = service_factory()
    append_run_event(
        connection,
        run_id,
        EventType.RECOVERY_FINISHED,
        {"run_id": run_id, "action": "restart_script", "result": "failed"},
    )
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1000},
    )
    service.poll()
    assert record_outcomes_within(
        service, lambda: len(run_event_payloads(connection, EventType.NOTIFY_SENT)) == 1
    )
    assert len(stub.sent) == 1
    assert "恢复停手" in stub.sent[0].title
    assert "已停手" in stub.sent[0].body
    assert "耗时：2 分 0 秒" in stub.sent[0].body


def test_exhausted_delivery_records_notify_failed(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    stub.results = [DeliveryResult(ok=False, error_code="E_NOTIFY", detail="HTTP 500")] * 3
    waits: list[float] = []
    service = service_factory(waits=waits)
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1000},
    )
    service.poll()
    assert wait_until(lambda: len(stub.sent) == 3 and len(waits) == 2)
    assert waits == [5.0, 30.0]
    assert record_outcomes_within(
        service, lambda: len(run_event_payloads(connection, EventType.NOTIFY_FAILED)) == 1
    )
    payload = run_event_payloads(connection, EventType.NOTIFY_FAILED)[0]
    assert payload["run_id"] == run_id
    assert payload["attempts"] == 3
    assert "HTTP 500" in payload["error"]
    assert run_event_payloads(connection, EventType.NOTIFY_SENT) == []


def test_watchdog_gave_up_notifies_without_run(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    service = service_factory()
    append_global_event(
        connection,
        EventType.CORE_EVENT,
        {"event": WATCHDOG_GAVE_UP_EVENT, "detail": "30 分钟内 3 次验收失败"},
    )
    service.poll()
    assert stub.delivered.wait(2.0)
    notification = stub.sent[0]
    assert notification.level is NotificationLevel.ERROR
    assert notification.title == "看门狗停手"
    assert "30 分钟内 3 次验收失败" in notification.body
    assert record_outcomes_within(service, lambda: len(core_events(connection, "notify.sent")) == 1)
    assert core_events(connection, "notify.sent")[0]["payload"]["channel"] == "serverchan"
    assert run_event_payloads(connection, EventType.NOTIFY_SENT) == []


def test_tailscale_down_notifies_without_run(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    service = service_factory()
    append_global_event(
        connection,
        EventType.HOST_EVENT,
        {"event": TAILSCALE_DOWN_EVENT, "detail": "NeedsLogin"},
    )
    service.poll()
    assert stub.delivered.wait(2.0)
    notification = stub.sent[0]
    assert notification.level is NotificationLevel.WARN
    assert notification.title == "Tailscale 未运行"
    assert "NeedsLogin" in notification.body
    assert record_outcomes_within(service, lambda: len(core_events(connection, "notify.sent")) == 1)
    assert run_event_payloads(connection, EventType.NOTIFY_SENT) == []


def test_tailscale_down_is_silent_when_channel_disabled(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    service = service_factory(settings={"notify.enabled": False})
    append_global_event(
        connection,
        EventType.HOST_EVENT,
        {"event": TAILSCALE_DOWN_EVENT, "detail": "Stopped"},
    )
    service.poll()
    assert stub.sent == []


def test_environment_failed_notifies(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    service = service_factory()
    append_global_event(
        connection,
        EventType.ENV_STATE_CHANGED,
        {
            "environment_id": "env-1",
            "from_state": "ready",
            "to_state": "failed",
            "probe": {"detail": "adb 掉线"},
        },
    )
    service.poll()
    assert stub.delivered.wait(2.0)
    notification = stub.sent[0]
    assert notification.title == "环境失败：env-1"
    assert "adb 掉线" in notification.body


def test_environment_degraded_does_not_notify(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    service = service_factory()
    append_global_event(
        connection,
        EventType.ENV_STATE_CHANGED,
        {"environment_id": "env-1", "from_state": "ready", "to_state": "degraded"},
    )
    service.poll()
    assert stub.sent == []
