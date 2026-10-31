"""dead-man 外部心跳：通过发心跳、失败报 fail 且去重、异常只落日志、未配置零网络。"""

from __future__ import annotations

import json
import urllib.error
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request

import pytest

from contracts import NotificationLevel
from contracts.types import DeliveryResult
from core.api.store import put_settings
from core.clock import VirtualClock
from core.hostops import deadman, watchdog
from storage import migrate, open_database
from tests.notify_support import StubChannel

NOW = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
HEARTBEAT_URL = "https://hc-ping.com/0f4d1a8e-9abc-4def-8123-456789abcdef"
# docs/08 脱敏口径：来源加其后前 6 位
REDACTED_URL = "https://hc-ping.com/0f4d1"
ENABLED_NOTIFY: dict[str, Any] = {
    "notify.enabled": True,
    "notify.channel": "serverchan",
    "notify.token": "SCT123",
}
PASS_REPORT = watchdog.CheckReport(
    passed=True, action=watchdog.ACTION_NONE, heartbeat_age_s=10.0, core_pid=101
)
FAIL_REPORT = watchdog.CheckReport(
    passed=False,
    action=watchdog.ACTION_START_REQUESTED,
    heartbeat_age_s=310.0,
    core_pid=101,
)


class RecordingTransport:
    """记录请求并按剧本抛错的假传输；不打真实网络。"""

    def __init__(self, error: Exception | None = None) -> None:
        self.requests: list[tuple[str, float]] = []
        self.error = error

    def __call__(self, request: Request, timeout_seconds: float) -> object:
        self.requests.append((request.full_url, timeout_seconds))
        if self.error is not None:
            raise self.error
        return None


@pytest.fixture
def data_root(tmp_path: Path) -> Path:
    (tmp_path / "run").mkdir()
    (tmp_path / "logs").mkdir()
    return tmp_path


def write_settings(data_root: Path, values: Mapping[str, Any]) -> None:
    connection = open_database(data_root / "backstop.sqlite")
    try:
        migrate(connection)
        put_settings(connection, values, at=NOW)
    finally:
        connection.close()


def run_deadman(
    data_root: Path,
    report: watchdog.CheckReport,
    transport: RecordingTransport,
    channel: StubChannel | None = None,
) -> None:
    inputs = deadman.DeadmanInputs(
        clock=VirtualClock(NOW),
        transport=transport,
        channel_factory=(lambda _config: channel) if channel is not None else None,
    )
    deadman.on_check_report(report, data_root=data_root, inputs=inputs)


def deadman_lines(data_root: Path) -> list[dict[str, object]]:
    path = data_root / "logs" / watchdog.LOG_FILENAME
    if not path.is_file():
        return []
    return [
        dict(json.loads(line))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_pass_round_pings_configured_url_once(data_root: Path) -> None:
    # 恰好一次、GET、超时上限 5 秒（docs/08 外部报警）
    write_settings(data_root, {"notify.heartbeat_url": HEARTBEAT_URL})
    transport = RecordingTransport()
    run_deadman(data_root, PASS_REPORT, transport)
    assert transport.requests == [(HEARTBEAT_URL, 5.0)]
    [line] = deadman_lines(data_root)
    assert line["event"] == "deadman.heartbeat"
    assert line["url"] == REDACTED_URL
    assert line["result"] == "ok"


def test_without_url_makes_no_requests_and_no_lines(data_root: Path) -> None:
    # 未启用（缺键或空串）：零网络请求、零日志，行为与没有该功能完全一致
    write_settings(data_root, {})
    transport = RecordingTransport()
    run_deadman(data_root, PASS_REPORT, transport)
    run_deadman(data_root, FAIL_REPORT, transport)
    assert transport.requests == []
    assert deadman_lines(data_root) == []
    write_settings(data_root, {"notify.heartbeat_url": ""})
    run_deadman(data_root, PASS_REPORT, transport)
    run_deadman(data_root, FAIL_REPORT, transport)
    assert transport.requests == []
    assert deadman_lines(data_root) == []


def test_fail_round_sends_fail_notification_and_skips_ping(data_root: Path) -> None:
    write_settings(data_root, {"notify.heartbeat_url": HEARTBEAT_URL, **ENABLED_NOTIFY})
    transport = RecordingTransport()
    channel = StubChannel()
    run_deadman(data_root, FAIL_REPORT, transport, channel=channel)
    assert transport.requests == []
    assert len(channel.sent) == 1
    message = channel.sent[0]
    assert message.level == NotificationLevel.ERROR
    assert message.title == "看门狗：核心失联"
    assert HEARTBEAT_URL not in message.body
    [line] = deadman_lines(data_root)
    assert line["event"] == "deadman.check_failed"
    assert line["result"] == "notified"


def test_fail_round_notifies_once_until_check_passes(data_root: Path) -> None:
    # 同一次失败事件只发一条：连续失败去重，检查恢复通过后清标记，再失败重新通知
    write_settings(data_root, {"notify.heartbeat_url": HEARTBEAT_URL, **ENABLED_NOTIFY})
    transport = RecordingTransport()
    channel = StubChannel()
    run_deadman(data_root, FAIL_REPORT, transport, channel=channel)
    run_deadman(data_root, FAIL_REPORT, transport, channel=channel)
    assert len(channel.sent) == 1
    run_deadman(data_root, PASS_REPORT, transport, channel=channel)
    run_deadman(data_root, FAIL_REPORT, transport, channel=channel)
    assert len(channel.sent) == 2
    state = watchdog.read_state(data_root / "run" / watchdog.STATE_FILENAME)
    assert state["deadman_fail_notified"] is True


def test_fail_notification_retries_when_channel_fails(data_root: Path) -> None:
    # 投递失败不置去重标记：下一轮失败重试
    write_settings(data_root, {"notify.heartbeat_url": HEARTBEAT_URL, **ENABLED_NOTIFY})
    transport = RecordingTransport()
    channel = StubChannel()
    channel.results.append(DeliveryResult(ok=False, error_code="E_NOTIFY", detail="HTTP 500"))
    run_deadman(data_root, FAIL_REPORT, transport, channel=channel)
    [line] = deadman_lines(data_root)
    assert line["result"] == "notify_failed"
    state = watchdog.read_state(data_root / "run" / watchdog.STATE_FILENAME)
    assert state.get("deadman_fail_notified", False) is False
    run_deadman(data_root, FAIL_REPORT, transport, channel=channel)
    assert len(channel.sent) == 2


def test_notify_disabled_records_failure_without_network(data_root: Path) -> None:
    # 通知未启用：fail 发不出去，落日志说明原因；心跳开关与通知开关相互独立
    write_settings(data_root, {"notify.heartbeat_url": HEARTBEAT_URL})
    transport = RecordingTransport()
    channel = StubChannel()
    run_deadman(data_root, FAIL_REPORT, transport, channel=channel)
    assert channel.sent == []
    [line] = deadman_lines(data_root)
    assert line["result"] == "notify_failed"
    assert "notify.enabled" in str(line["detail"])


@pytest.mark.parametrize(
    "error",
    [urllib.error.URLError("连接被拒"), TimeoutError()],
    ids=["refused", "timeout"],
)
def test_ping_failure_recorded_and_masked(data_root: Path, error: Exception) -> None:
    # 心跳请求异常：不外抛（主循环不受影响），结果落日志且 URL 已脱敏
    write_settings(data_root, {"notify.heartbeat_url": HEARTBEAT_URL})
    transport = RecordingTransport(error=error)
    run_deadman(data_root, PASS_REPORT, transport)
    [line] = deadman_lines(data_root)
    assert line["event"] == "deadman.heartbeat"
    assert line["result"] == "failed"
    assert line["url"] == REDACTED_URL
    assert HEARTBEAT_URL not in json.dumps(line, ensure_ascii=False)


def test_missing_database_stays_dormant(data_root: Path) -> None:
    # 配置随库而生：缺库即未配置，静默不启用（docs/08 未启用零请求零日志）
    transport = RecordingTransport()
    run_deadman(data_root, PASS_REPORT, transport)
    assert transport.requests == []
    assert deadman_lines(data_root) == []


def test_corrupt_database_records_error_and_skips_network(data_root: Path) -> None:
    # 内部故障（库损坏）也不外抛：on_check_report 是看门狗主循环的安全边界
    (data_root / "backstop.sqlite").write_text("not a database", encoding="utf-8")
    transport = RecordingTransport()
    run_deadman(data_root, PASS_REPORT, transport)
    assert transport.requests == []
    [line] = deadman_lines(data_root)
    assert line["event"] == "deadman.error"


def test_non_string_heartbeat_url_records_error(data_root: Path) -> None:
    write_settings(data_root, {"notify.heartbeat_url": 5})
    transport = RecordingTransport()
    run_deadman(data_root, PASS_REPORT, transport)
    assert transport.requests == []
    [line] = deadman_lines(data_root)
    assert line["event"] == "deadman.error"
    assert "notify.heartbeat_url" in str(line["detail"])


def test_ping_independent_of_broken_channel_config(data_root: Path) -> None:
    # 渠道配置非法只影响 fail 通知：心跳直读原始键，照发
    write_settings(
        data_root,
        {"notify.heartbeat_url": HEARTBEAT_URL, "notify.enabled": True, "notify.channel": "ntfy"},
    )
    transport = RecordingTransport()
    run_deadman(data_root, PASS_REPORT, transport)
    assert transport.requests == [(HEARTBEAT_URL, 5.0)]


def test_cli_inputs_wire_the_deadman_hook(tmp_path: Path) -> None:
    """CLI 装配的注入件带 dead-man 挂载点；缺库时静默休眠，不抛异常。"""
    from core.__main__ import _watchdog_check_inputs

    inputs = _watchdog_check_inputs(tmp_path)
    assert inputs.deadman is not None
    inputs.deadman(PASS_REPORT)
    assert deadman_lines(tmp_path) == []
