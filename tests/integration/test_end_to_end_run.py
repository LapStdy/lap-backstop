"""端到端闭环冒烟：serve 装配 fake 插件，到点调度触发一次运行，结果经真 HTTP 可查。

链路是生产路径本身：serve 启动 → 装配插件 → 调度扫描到点建运行 → tick 跑完
prepare/execute/verify/cleanup 四阶段 → 运行 succeeded。运行结束前进程仍在跑，
测试在事件流上等 ``run.finished``，再用 HTTP 取回运行详情、verdict 与证据文件。
"""

from __future__ import annotations

import hashlib
import json
import shutil
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from core.api.store import TOKEN_KEY
from core.clock import VirtualClock
from core.hostops import serve as serve_module
from storage import open_database
from tests.integration.test_api_events import parse_frame
from tests.integration.test_serve_api import configure, serve_options

NOW = datetime(2026, 10, 30, 3, 0, tzinfo=UTC)
TOKEN = "e2e-token"
TEMPLATE_ID = "template-1"
SCHEDULE_ID = "schedule-1"
ENV_ID = "env-1"
REPO_ROOT = Path(__file__).resolve().parents[2]
STREAM_TIMEOUT = 10.0
FRAME_TIMEOUT = 30.0
PROBE_TIMEOUT = 60.0
Frame = tuple[str | None, str, dict[str, Any]]


def _seed(data_root: Path) -> None:
    """前置数据：token 与随机端口设置、一个 fake 模板、一条一小时前就该跑的调度。"""
    configure(data_root, {TOKEN_KEY: TOKEN, "api.host": "127.0.0.1", "api.port": 0})
    connection = open_database(data_root / "backstop.sqlite")
    try:
        connection.execute(
            "INSERT INTO job_templates (id, name, plugin_id, environment_id, config, "
            "config_schema_version, verification_policy, recovery_policy, enabled, "
            "created_at, updated_at) VALUES (?, '日常', 'fake', ?, '{}', '1', ?, '{}', 1, ?, ?)",
            (
                TEMPLATE_ID,
                ENV_ID,
                json.dumps(
                    {
                        "id": "fake-policy",
                        "verifier_plugin_id": "fake",
                        "required_evidence": ["log"],
                        "params": {},
                    }
                ),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
        connection.execute(
            "INSERT INTO schedules (id, name, kind, expr, at, timezone, catch_up, "
            "window_minutes, coalesce, priority, deadline_minutes, preheat_minutes, "
            "keep_warm_minutes, job_template_id, enabled, next_run_at, created_at, updated_at) "
            "VALUES (?, '每小时', 'interval', '60', NULL, 'UTC', 'run_once', 120, 1, 0, 180, 0, "
            "0, ?, 1, ?, ?, ?)",
            (
                SCHEDULE_ID,
                TEMPLATE_ID,
                (NOW - timedelta(hours=1)).isoformat(),
                NOW.isoformat(),
                NOW.isoformat(),
            ),
        )
    finally:
        connection.close()


def _request(url: str) -> tuple[int, bytes, str]:
    """带 Bearer token 的 GET，返回状态码、正文与 Content-Type；HTTP 错误也取回正文。"""
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read(), response.headers.get("Content-Type", "")
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers.get("Content-Type", "")


def _get_json(url: str) -> dict[str, Any]:
    """读一个 JSON 端点；非 200 直接断言失败并带响应正文。"""
    status, body, _ = _request(url)
    assert status == 200, f"{url} 返回 {status}：{body[:200]!r}"
    payload = json.loads(body)
    assert isinstance(payload, dict)
    return payload


class _EventStream:
    """测试用 SSE 客户端：裸套接字逐帧收，等待时长由调用方给。"""

    def __init__(self, host: str, port: int) -> None:
        self._socket = socket.create_connection((host, port), timeout=STREAM_TIMEOUT)
        lines = [
            "GET /api/v1/events HTTP/1.0",
            f"Host: {host}:{port}",
            f"Authorization: Bearer {TOKEN}",
        ]
        self._socket.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("utf-8"))
        self._buffer = b""
        self.status_line = self._read_line()
        while self._read_line():
            pass

    def next_frame(self, timeout: float) -> Frame:
        """读下一完整帧；超时或连接先关都断言失败。"""
        deadline = time.monotonic() + timeout
        while b"\n\n" not in self._buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"事件流 {timeout:g} 秒内没有完整帧")
            self._socket.settimeout(remaining)
            chunk = self._socket.recv(4096)
            if not chunk:
                raise AssertionError("事件流在完整帧之前关闭")
            self._buffer += chunk
        raw, self._buffer = self._buffer.split(b"\n\n", 1)
        return parse_frame(raw)

    def close(self) -> None:
        """关闭连接；服务端按客户端断开收尾。"""
        self._socket.close()

    def _read_line(self) -> str:
        """读一行响应头；连接先关按断言失败处理。"""
        while b"\r\n" not in self._buffer:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise AssertionError("事件流响应头未读完就关闭")
            self._buffer += chunk
        line, self._buffer = self._buffer.split(b"\r\n", 1)
        return line.decode("utf-8", errors="replace")


def _probe(
    host: str,
    port: int,
    seen: dict[str, Any],
    connected: threading.Event,
    stop: threading.Event,
) -> None:
    """事件流上等运行结束，再用 HTTP 取回运行详情与证据文件，最后放行停机。

    首帧（心跳）到手才置 connected：此时事件流的游标已定在水位之后，tick 里写的
    运行事件才会被这条流交付。收集失败记进 seen，由主线程断言时抛出。
    """
    base = f"http://{host}:{port}"
    frames: list[Frame] = []
    try:
        stream = _EventStream(host, port)
        try:
            seen["status_line"] = stream.status_line
            seen["first_frame"] = stream.next_frame(STREAM_TIMEOUT)
            connected.set()
            while True:
                frame = stream.next_frame(FRAME_TIMEOUT)
                frames.append(frame)
                if frame[1] == "run.finished":
                    break
            seen["frames"] = frames
        finally:
            stream.close()
        run_id = str(frames[-1][2]["run_id"])
        seen["run_detail"] = _get_json(f"{base}/api/v1/runs/{run_id}")
        evidence = seen["run_detail"].get("evidence") or []
        if evidence:
            first = evidence[0]
            seen["evidence_file"] = _request(f"{base}/api/v1/runs/{run_id}/evidence/{first['id']}")
    except Exception as error:
        seen["failure"] = error
    finally:
        connected.set()
        stop.set()


def test_due_schedule_runs_to_completion_and_results_are_queryable_over_http(
    tmp_path: Path,
) -> None:
    plugins_root = tmp_path / "plugins"
    shutil.copytree(REPO_ROOT / "plugins" / "fake", plugins_root / "fake")
    data_root = tmp_path / "data"
    _seed(data_root)
    stop = threading.Event()
    connected = threading.Event()
    seen: dict[str, Any] = {}
    probes: list[threading.Thread] = []

    def on_api_listening(host: str, port: int) -> None:
        probe = threading.Thread(
            target=_probe,
            args=(host, port, seen, connected, stop),
            name="e2e-probe",
            daemon=True,
        )
        probe.start()
        probes.append(probe)
        if not connected.wait(STREAM_TIMEOUT):
            stop.set()

    code = serve_module.run_serve(
        data_root,
        serve_options(
            clock=VirtualClock(NOW),
            plugins_root=plugins_root,
            max_ticks=None,
            tick_interval_seconds=0.05,
            stop=stop,
            on_api_listening=on_api_listening,
        ),
    )
    for probe in probes:
        probe.join(PROBE_TIMEOUT)
    assert not any(probe.is_alive() for probe in probes)
    assert code == 0
    if "failure" in seen:
        raise seen["failure"]

    assert seen["status_line"].startswith("HTTP/1.0 200")
    assert seen["first_frame"][1] == "heartbeat"
    frames = seen["frames"]
    assert frames[-1][1] == "run.finished"
    run_id = frames[-1][2]["run_id"]
    assert frames[-1][2]["status"] == "succeeded"
    event_types = [frame[1] for frame in frames]
    # 一条流同时交付两张事件表：同一批里全局事件先于运行事件，只断言出现而不排全序。
    assert {
        "run.created",
        "run.state.changed",
        "run.signal",
        "run.verdict",
        "env.state.changed",
    } <= set(event_types)
    # 事件 id 的 run 前缀证明帧来自运行事件表，而不是恰好同形的全局流。
    last_event_id = frames[-1][0]
    assert last_event_id is not None
    assert last_event_id.startswith(f"r:{run_id}:")
    signals = [payload for _event_id, name, payload in frames if name == "run.signal"]
    assert [signal["kind"] for signal in signals] == ["completed"]

    detail = seen["run_detail"]
    run = detail["run"]
    assert run["id"] == run_id
    assert run["state"] == "succeeded"
    assert run["schedule_id"] == SCHEDULE_ID
    assert run["template_id"] == TEMPLATE_ID
    phases = {phase["phase"]: phase["state"] for phase in detail["phases"]}
    assert phases == {"prepare": "ok", "execute": "ok", "verify": "ok", "cleanup": "ok"}

    assert len(detail["verdicts"]) == 1
    verdict = detail["verdicts"][0]
    assert verdict["status"] == "completed"
    assert run["verdict_id"] == verdict["id"]
    assert len(detail["evidence"]) == 1
    evidence = detail["evidence"][0]
    assert evidence["kind"] == "log"
    assert evidence["path"] == f"{run_id}/fake.log"
    assert verdict["evidence_ids"] == [evidence["id"]]

    status, body, content_type = seen["evidence_file"]
    assert status == 200
    assert content_type.startswith("text/plain")
    assert body.startswith(b"fake instant")
    assert body == (data_root / "evidence" / run_id / "fake.log").read_bytes()
    assert hashlib.sha256(body).hexdigest() == evidence["sha256"]
