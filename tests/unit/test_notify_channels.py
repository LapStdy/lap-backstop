"""三渠道 HTTP 客户端：本地假服务覆盖成功、4xx、5xx、超时、坏应答与请求形态。"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
import urllib.parse
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from contracts import Notification, NotificationLevel, Notifier, RunContext
from core.clock import SystemClock
from core.logging import NullLogger
from core.notify.channels import DingTalkChannel, NtfyChannel, ServerChanChannel
from core.worker.executor import CancelFlag

TITLE = "标题"
BODY = "正文行一\n正文行二"


def _message(level: NotificationLevel = NotificationLevel.INFO) -> Notification:
    return Notification(level=level, title=TITLE, body=BODY)


def _ctx(tmp_path: Path) -> RunContext:
    """渠道不消费上下文，这里给最小合法实例。"""
    return RunContext(
        run_id="notify:test",
        attempt=0,
        template_id="",
        plugin_id="",
        environment_id="",
        config={},
        work_dir=tmp_path,
        evidence_dir=tmp_path,
        deadline_at=datetime.now(UTC) + timedelta(seconds=30),
        cancel=CancelFlag(),
        clock=SystemClock(),
        secrets={},
        log=NullLogger(),
        emit=lambda event, fields: None,
        record_evidence=lambda kind, path, summary: "",
    )


class _FakeServer:
    """可脚本化的本地假渠道：按剧本应答并记录收到的请求。"""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.status = 200
        self.body = b"{}"
        self.delay_seconds = 0.0
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(self))
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def base_url(self) -> str:
        host = str(self._server.server_address[0])
        port = int(str(self._server.server_address[1]))
        return f"http://{host}:{port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def _make_handler(owner: _FakeServer) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            owner.requests.append(
                {"path": self.path, "headers": dict(self.headers.items()), "body": raw}
            )
            if owner.delay_seconds > 0:
                time.sleep(owner.delay_seconds)
            self.send_response(owner.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(owner.body)))
            self.end_headers()
            self.wfile.write(owner.body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    return Handler


@pytest.fixture
def fake_server() -> Iterator[_FakeServer]:
    server = _FakeServer()
    try:
        yield server
    finally:
        server.close()


def test_serverchan_success_posts_form(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.body = json.dumps({"code": 0}).encode("utf-8")
    channel = ServerChanChannel(token="SCT123", endpoint=fake_server.base_url)
    result = channel.send(_ctx(tmp_path), _message())
    assert result.ok
    assert result.error_code is None
    request = fake_server.requests[0]
    assert request["path"] == "/SCT123.send"
    form = urllib.parse.parse_qs(request["body"].decode("utf-8"))
    assert form["title"] == [TITLE]
    assert form["desp"] == [BODY]


def test_serverchan_business_code_failure(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.body = json.dumps({"code": 40001, "message": "bad key"}).encode("utf-8")
    channel = ServerChanChannel(token="SCT123", endpoint=fake_server.base_url)
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert result.error_code == "E_NOTIFY"
    assert "40001" in result.detail


@pytest.mark.parametrize("status", [403, 500])
def test_serverchan_http_status_failure(
    tmp_path: Path, fake_server: _FakeServer, status: int
) -> None:
    fake_server.status = status
    channel = ServerChanChannel(token="SCT123", endpoint=fake_server.base_url)
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert result.error_code == "E_NOTIFY"
    assert f"HTTP {status}" in result.detail


def test_serverchan_timeout(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.delay_seconds = 2.0
    channel = ServerChanChannel(token="SCT123", endpoint=fake_server.base_url, timeout_seconds=0.3)
    started = time.monotonic()
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert result.error_code == "E_NOTIFY"
    assert "超时" in result.detail
    assert time.monotonic() - started < 1.5


def test_serverchan_bad_json(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.body = b"<html>gateway error</html>"
    channel = ServerChanChannel(token="SCT123", endpoint=fake_server.base_url)
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert "JSON" in result.detail


def test_dingtalk_success_with_sign(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.body = json.dumps({"errcode": 0, "errmsg": "ok"}).encode("utf-8")
    channel = DingTalkChannel(
        webhook=f"{fake_server.base_url}/robot/send?access_token=abc", secret="SEC999"
    )
    result = channel.send(_ctx(tmp_path), _message())
    assert result.ok
    request = fake_server.requests[0]
    parsed = urllib.parse.urlsplit(request["path"])
    query = urllib.parse.parse_qs(parsed.query)
    timestamp = query["timestamp"][0]
    expected = base64.b64encode(
        hmac.new(b"SEC999", f"{timestamp}\nSEC999".encode(), hashlib.sha256).digest()
    )
    assert query["sign"][0] == expected.decode("ascii")
    payload = json.loads(request["body"].decode("utf-8"))
    assert payload["msgtype"] == "markdown"
    assert payload["markdown"]["title"] == TITLE
    assert TITLE in payload["markdown"]["text"]
    assert BODY in payload["markdown"]["text"]


def test_dingtalk_success_without_sign(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.body = json.dumps({"errcode": 0}).encode("utf-8")
    channel = DingTalkChannel(webhook=f"{fake_server.base_url}/robot/send")
    result = channel.send(_ctx(tmp_path), _message())
    assert result.ok
    assert "timestamp=" not in fake_server.requests[0]["path"]


def test_dingtalk_errcode_failure(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.body = json.dumps({"errcode": 310000, "errmsg": "sign not match"}).encode("utf-8")
    channel = DingTalkChannel(webhook=f"{fake_server.base_url}/robot/send")
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert result.error_code == "E_NOTIFY"
    assert "310000" in result.detail


def test_dingtalk_http_5xx(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.status = 502
    channel = DingTalkChannel(webhook=f"{fake_server.base_url}/robot/send")
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert result.error_code == "E_NOTIFY"


def test_ntfy_success_publishes_json(tmp_path: Path, fake_server: _FakeServer) -> None:
    channel = NtfyChannel(topic_url=f"{fake_server.base_url}/my-topic", token="tk_1")
    result = channel.send(_ctx(tmp_path), _message(NotificationLevel.WARN))
    assert result.ok
    request = fake_server.requests[0]
    assert request["path"] == "/"
    assert request["headers"].get("Authorization") == "Bearer tk_1"
    payload = json.loads(request["body"].decode("utf-8"))
    assert payload["topic"] == "my-topic"
    assert payload["title"] == TITLE
    assert payload["message"] == BODY
    assert payload["priority"] == 4
    assert payload["tags"] == ["warning"]


def test_ntfy_error_level_maps_priority(tmp_path: Path, fake_server: _FakeServer) -> None:
    channel = NtfyChannel(topic_url=f"{fake_server.base_url}/my-topic")
    result = channel.send(_ctx(tmp_path), _message(NotificationLevel.ERROR))
    assert result.ok
    payload = json.loads(fake_server.requests[0]["body"].decode("utf-8"))
    assert payload["priority"] == 5
    assert payload["tags"] == ["rotating_light"]
    assert "Authorization" not in fake_server.requests[0]["headers"]


def test_ntfy_http_4xx(tmp_path: Path, fake_server: _FakeServer) -> None:
    fake_server.status = 403
    channel = NtfyChannel(topic_url=f"{fake_server.base_url}/my-topic")
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert result.error_code == "E_NOTIFY"


def test_ntfy_rejects_multi_segment_topic(tmp_path: Path, fake_server: _FakeServer) -> None:
    channel = NtfyChannel(topic_url=f"{fake_server.base_url}/a/b")
    result = channel.send(_ctx(tmp_path), _message())
    assert not result.ok
    assert not fake_server.requests


@pytest.mark.parametrize(
    "channel",
    [
        ServerChanChannel(token="SCT123"),
        DingTalkChannel(webhook="https://oapi.dingtalk.com/robot/send"),
        NtfyChannel(topic_url="https://ntfy.sh/my-topic"),
    ],
)
def test_channels_satisfy_notifier_protocol(channel: object) -> None:
    assert isinstance(channel, Notifier)
