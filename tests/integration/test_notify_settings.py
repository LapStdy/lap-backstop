"""通知服务集成：开关、非法配置、游标起点与连通性测试入口。

fixtures（connection、stub、service_factory）见同目录 conftest.py。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Any

from contracts import EventType
from tests.notify_support import (
    append_run_event,
    core_events,
    create_run,
    record_outcomes_within,
    run_event_payloads,
)


def test_disabled_by_default_sends_nothing(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    service = service_factory(settings={})
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1000},
    )
    service.poll()
    assert stub.sent == []
    service.poll()
    assert run_event_payloads(connection, EventType.NOTIFY_SENT) == []


def test_invalid_config_reports_once_then_recovers(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    settings: dict[str, Any] = {"notify.enabled": True, "notify.channel": "serverchan"}
    service = service_factory(settings=settings)
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1000},
    )
    service.poll()
    service.poll()
    assert len(core_events(connection, "notify.config_invalid")) == 1
    assert stub.sent == []
    settings["notify.token"] = "SCT123"
    service.poll()
    assert stub.delivered.wait(2.0)
    assert len(core_events(connection, "notify.config_invalid")) == 1
    assert record_outcomes_within(
        service, lambda: len(run_event_payloads(connection, EventType.NOTIFY_SENT)) == 1
    )


def test_preexisting_events_are_not_replayed(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1000},
    )
    service = service_factory()
    service.poll()
    assert stub.sent == []


def test_settings_change_disables_delivery(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    run_id = create_run(connection, "notify:main")
    settings: dict[str, Any] = {
        "notify.enabled": True,
        "notify.channel": "serverchan",
        "notify.token": "SCT123",
    }
    service = service_factory(settings=settings)
    append_run_event(
        connection,
        run_id,
        EventType.RUN_FINISHED,
        {"run_id": run_id, "status": "failed", "duration_ms": 1000},
    )
    service.poll()
    assert stub.delivered.wait(2.0)
    settings["notify.enabled"] = False
    other_run = create_run(connection, "notify:other")
    append_run_event(
        connection,
        other_run,
        EventType.RUN_FINISHED,
        {"run_id": other_run, "status": "failed", "duration_ms": 1000},
    )
    service.poll()
    assert len(stub.sent) == 1


def test_send_test_uses_current_settings(
    connection: sqlite3.Connection,
    stub: Any,
    service_factory: Callable[..., Any],
) -> None:
    service = service_factory(settings={})
    result = service.send_test()
    assert not result.ok
    assert result.error_code == "E_CONFIG_INVALID"
    assert "notify.enabled" in result.detail

    service = service_factory()
    result = service.send_test()
    assert result.ok
    assert stub.sent[0].title == "测试通知"
    assert stub.sent[0].level.value == "info"
