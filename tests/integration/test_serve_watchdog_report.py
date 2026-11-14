"""serve 启动序的看门狗停手补报：旧标记、新格式报告、缺证据兜底与损坏状态。

判据是全局事件里的 WATCHDOG_GAVE_UP_EVENT 与载荷各键：补报是取走式，标记清零后
不重复上报；状态文件损坏时无法证明停过手，不产生事件，核心照常启动。
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from core.clock import VirtualClock
from core.hostops import watchdog
from core.hostops.serve import WATCHDOG_GAVE_UP_EVENT, ServeOptions, run_serve
from tests.integration.serve_shell_support import (
    autologon_registry,
    global_event_payloads,
    global_event_rows,
    global_events,
)


def test_run_serve_reports_watchdog_gave_up_marker(tmp_path: Path) -> None:
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    # 旧版状态文件：只有布尔标记，claim 走兜底载荷
    watchdog.write_state(
        state_path,
        {
            "pending_start": None,
            "failures": ["2026-10-25T07:50:00+00:00"],
            "gave_up_pending_report": True,
        },
    )
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        run_timesync=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 0
    assert WATCHDOG_GAVE_UP_EVENT in global_events(tmp_path / "backstop.sqlite")
    assert ("host.event", WATCHDOG_GAVE_UP_EVENT) in global_event_rows(tmp_path / "backstop.sqlite")
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == WATCHDOG_GAVE_UP_EVENT
    )
    assert "1 次验收失败" in payload["detail"]
    assert payload["failed_count"] == 1
    assert payload["last_failure_at"] == "2026-10-25T07:50:00+00:00"
    assert payload["heartbeat"] is None
    assert payload["log_tail"] == []
    # 补报是取走式：标记清零，不会重复上报
    assert watchdog.read_state(state_path)["gave_up_pending_report"] is False


def test_run_serve_reports_stored_watchdog_report_with_evidence(tmp_path: Path) -> None:
    """新格式报告经核心启动补报：事件五个键齐全，证据字段原样透传。"""
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    watchdog.write_state(
        state_path,
        {
            "pending_start": None,
            "failures": ["2026-10-25T07:50:00+00:00"],
            "gave_up_pending_report": True,
            "gave_up_report": {
                "failed_count": 3,
                "last_failure_at": "2026-10-25T07:50:00+00:00",
                "heartbeat": {"pid": 4242, "last_tick": "2026-10-25T07:49:00+00:00"},
                "log_tail": ["first line", "second line"],
            },
        },
    )
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        run_timesync=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 0
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == WATCHDOG_GAVE_UP_EVENT
    )
    assert set(payload) == {
        "event",
        "detail",
        "failed_count",
        "last_failure_at",
        "heartbeat",
        "log_tail",
    }
    assert "3 次验收失败" in payload["detail"]
    assert payload["failed_count"] == 3
    assert payload["last_failure_at"] == "2026-10-25T07:50:00+00:00"
    assert payload["heartbeat"] == {"pid": 4242, "last_tick": "2026-10-25T07:49:00+00:00"}
    assert payload["log_tail"] == ["first line", "second line"]


def test_run_serve_falls_back_when_stored_report_lacks_log_tail(tmp_path: Path) -> None:
    """报告缺 log_tail → 不可解析，走兜底载荷（失败次数从 failures 推）。"""
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    watchdog.write_state(
        state_path,
        {
            "pending_start": None,
            "failures": ["2026-10-25T07:50:00+00:00"],
            "gave_up_pending_report": True,
            "gave_up_report": cast(watchdog.GaveUpReport, {"failed_count": 9}),
        },
    )
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        run_timesync=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 0
    payload = next(
        payload
        for payload in global_event_payloads(tmp_path / "backstop.sqlite")
        if payload.get("event") == WATCHDOG_GAVE_UP_EVENT
    )
    assert payload["failed_count"] == 1
    assert payload["heartbeat"] is None
    assert payload["log_tail"] == []


def test_run_serve_skips_gave_up_event_when_state_corrupt(tmp_path: Path) -> None:
    """状态文件损坏时无法证明停过手，不产生停手事件，核心照常启动。"""
    state_path = tmp_path / "run" / watchdog.STATE_FILENAME
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("not json", encoding="utf-8")
    options = ServeOptions(
        clock=VirtualClock(datetime(2026, 10, 25, 8, 0, tzinfo=UTC)),
        max_ticks=1,
        tick_interval_seconds=0.0,
        register_tasks=False,
        run_timesync=False,
        registry_get=lambda _key, name: autologon_registry("1", "backstop")[name],
    )
    assert run_serve(tmp_path, options) == 0
    assert WATCHDOG_GAVE_UP_EVENT not in global_events(tmp_path / "backstop.sqlite")
