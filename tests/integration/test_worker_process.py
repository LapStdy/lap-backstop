"""worker 子进程协议：启动、能力调用、回调与异常回传。"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from core.plugins import rpc

REPO_ROOT = Path(__file__).resolve().parents[2]
PLUGINS_ROOT = REPO_ROOT / "plugins"
FIXTURE_PLUGINS = Path(__file__).resolve().parents[1] / "fixtures" / "plugins"


class WorkerProcess:
    """测试用的协议客户端：按行收发 JSON，回调由测试决定返回值。"""

    def __init__(
        self,
        tmp_path: Path,
        plugin_id: str = "fake",
        plugins_root: Path = PLUGINS_ROOT,
    ) -> None:
        self.tmp_path = tmp_path
        self._process = subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-m",
                "core.plugins.worker",
                plugin_id,
                "--plugins-root",
                str(plugins_root),
            ],
            cwd=REPO_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        self.ready = self._read()
        if self.ready.get("kind") != rpc.READY:
            raise AssertionError(f"worker 启动失败：{self.ready}")

    def alive(self) -> bool:
        return self._process.poll() is None

    def kill(self) -> None:
        self._process.kill()
        self._process.wait(timeout=10)

    def call(
        self,
        capability: str,
        method: str,
        args: Mapping[str, Any],
        *,
        config: Mapping[str, Any] | None = None,
        on_callback: Callable[[dict[str, Any]], Any] | None = None,
        call_id: int = 1,
    ) -> dict[str, Any]:
        assert self._process.stdin is not None
        message = {
            "kind": rpc.CALL,
            "id": call_id,
            "capability": capability,
            "method": method,
            "args": dict(args),
            "context": self.context(config or {}),
        }
        self._process.stdin.write(json.dumps(message) + "\n")
        self._process.stdin.flush()
        while True:
            reply = self._read()
            kind = reply.get("kind")
            if kind == rpc.CALLBACK:
                value = None if on_callback is None else on_callback(reply)
                self._respond(reply, value)
            elif kind == rpc.RESULT:
                return reply
            else:
                raise AssertionError(f"意外消息：{reply}")

    def send(self, message: Mapping[str, Any]) -> None:
        assert self._process.stdin is not None
        self._process.stdin.write(json.dumps(dict(message)) + "\n")
        self._process.stdin.flush()

    def close(self) -> int:
        assert self._process.stdin is not None
        self._process.stdin.write(json.dumps({"kind": rpc.SHUTDOWN}) + "\n")
        self._process.stdin.flush()
        return self._process.wait(timeout=10)

    def context(self, config: Mapping[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        return {
            "run_id": "run-1",
            "attempt": 1,
            "template_id": "template-1",
            "plugin_id": "fake",
            "environment_id": "env-1",
            "config": dict(config),
            "work_dir": str(self.tmp_path / "work"),
            "evidence_dir": str(self.tmp_path / "evidence"),
            "deadline_at": (now + timedelta(minutes=30)).isoformat(),
            "secrets": {},
        }

    def _respond(self, callback: Mapping[str, Any], value: Any) -> None:
        assert self._process.stdin is not None
        message = {
            "kind": rpc.CALLBACK_RESULT,
            "callback_id": callback["callback_id"],
            "ok": True,
            "value": value,
        }
        self._process.stdin.write(json.dumps(message) + "\n")
        self._process.stdin.flush()

    def _read(self) -> dict[str, Any]:
        assert self._process.stdout is not None
        line = self._process.stdout.readline()
        if not line:
            stderr = "" if self._process.stderr is None else self._process.stderr.read()
            raise AssertionError(f"worker 提前退出：{stderr[-800:]}")
        return rpc.decode_line(line)


@pytest.fixture
def worker(tmp_path: Path) -> Iterator[WorkerProcess]:
    session = WorkerProcess(tmp_path)
    try:
        yield session
    finally:
        if session.alive():
            session.kill()


def test_ready_lists_declared_capabilities(worker: WorkerProcess) -> None:
    assert worker.ready["kind"] == rpc.READY
    assert worker.ready["protocol"] == rpc.PROTOCOL_VERSION
    assert worker.ready["plugin_id"] == "fake"
    assert set(worker.ready["capabilities"]) == {"environment", "script", "verifier"}


def test_environment_probe_runs_in_worker(worker: WorkerProcess) -> None:
    reply = worker.call(
        "environment",
        "probe",
        {"env": {"id": "env-1", "provider_id": "fake", "config": {}}},
        config={"scenario": "instant"},
    )
    assert reply["ok"] is True
    assert reply["value"]["state"] == "ready"
    assert reply["value"]["detail"] == "fake:instant"


def test_verifier_callback_records_evidence(worker: WorkerProcess) -> None:
    seen: list[dict[str, Any]] = []

    def callback(message: dict[str, Any]) -> str:
        seen.append(message)
        return "evidence-1"

    reply = worker.call(
        "verifier",
        "verify",
        {},
        config={"scenario": "instant"},
        on_callback=callback,
    )
    assert reply["ok"] is True
    assert [item["op"] for item in seen] == ["record_evidence"]
    assert seen[0]["args"]["kind"] == "log"
    assert reply["value"]["status"] == "completed"
    assert reply["value"]["evidence_ids"] == ["evidence-1"]
    assert reply["value"]["checks"][0]["evidence_ids"] == ["evidence-1"]


def test_plugin_exception_returns_error_result(worker: WorkerProcess) -> None:
    reply = worker.call(
        "script",
        "launch",
        {"script": {"id": "script-1", "plugin_id": "fake", "config": {}}},
        config={"scenario": "crash"},
    )
    assert reply["ok"] is False
    assert reply["error"]["type"] == "RuntimeError"


def test_unknown_method_returns_error_result(worker: WorkerProcess) -> None:
    reply = worker.call("environment", "explode", {})
    assert reply["ok"] is False
    assert "explode" in reply["error"]["message"]


def test_shutdown_exits_cleanly(worker: WorkerProcess) -> None:
    assert worker.close() == 0


def test_plugin_stdout_stays_out_of_protocol(tmp_path: Path) -> None:
    plugin_root = tmp_path / "plugins"
    package = plugin_root / "printy"
    package.mkdir(parents=True)
    (package / "plugin.py").write_text(
        "import subprocess\n"
        "import sys\n"
        "\n"
        "from contracts import EnvironmentProbe, EnvironmentState\n"
        "\n"
        "class Environment:\n"
        "    def probe(self, env, ctx):\n"
        "        print('插件打印不应该进协议')\n"
        "        subprocess.run([sys.executable, '-c', \"print('child pollution')\"], check=True)\n"
        "        return EnvironmentProbe(state=EnvironmentState.READY, detail='打印过了')\n"
        "\n"
        "class Plugin:\n"
        "    environment = Environment()\n"
        "\n"
        "PLUGIN = Plugin()\n",
        encoding="utf-8",
    )
    (package / "plugin.json").write_text(
        json.dumps(
            {
                "id": "printy",
                "name": "printy · 打印测试插件",
                "version": "1.0.0",
                "api_version": 1,
                "capabilities": ["environment"],
                "config_schema": "config.schema.json",
                "requires": {"contract": ">=1 <2", "platform": "windows"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    session = WorkerProcess(tmp_path, "printy", plugins_root=plugin_root)
    try:
        reply = session.call(
            "environment",
            "probe",
            {"env": {"id": "env-1", "provider_id": "printy", "config": {}}},
        )
        assert reply["ok"] is True
        assert reply["value"]["detail"] == "打印过了"
    finally:
        session.close()


def test_cancel_is_scoped_to_one_call(tmp_path: Path) -> None:
    """取消按调用 id 生效：一次调用的取消不能让同一 worker 的后续调用都被标记。"""
    session = WorkerProcess(tmp_path, "cancel_probe", plugins_root=FIXTURE_PLUGINS)
    args = {"env": {"id": "env-1", "provider_id": "cancel_probe", "config": {}}}
    try:
        first = session.call("environment", "probe", args, call_id=1)
        assert first["value"]["detail"] == "cancelled=False"
        session.send({"kind": rpc.CANCEL, "id": 1})
        second = session.call("environment", "probe", args, call_id=2)
        assert second["value"]["detail"] == "cancelled=False"
    finally:
        session.close()


def test_cli_worker_subcommand_serves_protocol(tmp_path: Path) -> None:
    """exe 回调路径：`lap-backstop worker <id>` 与 -m 模块入口同一条协议（ADR 0010）。"""
    process = subprocess.Popen(
        [sys.executable, "-m", "core", "worker", "fake", "--plugins-root", str(PLUGINS_ROOT)],
        cwd=REPO_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    try:
        assert process.stdin is not None and process.stdout is not None
        ready = rpc.decode_line(process.stdout.readline())
        assert ready["kind"] == rpc.READY
        assert ready["plugin_id"] == "fake"
        assert set(ready["capabilities"]) == {"environment", "script", "verifier"}
        process.stdin.write(json.dumps({"kind": rpc.SHUTDOWN}) + "\n")
        process.stdin.flush()
        assert process.wait(timeout=10) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
