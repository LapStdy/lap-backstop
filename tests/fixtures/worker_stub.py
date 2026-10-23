"""测试用的 worker 桩：最小 RPC 协议与可注入的故障行为。

内核按 ``python tests/fixtures/worker_stub.py <mode> [control]`` 拉起本进程，并在参数末尾
追加 plugin_id 与 ``--plugins-root``；这里只读前两个位置参数。``sequence`` 模式每收到一次
调用就取控制文件的一行作为本次行为、再把剩余行写回，所以「先崩后好」跨进程重启仍然成立。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PROTOCOL_VERSION = 1
CAPABILITIES = ("environment",)
OK_VALUE: dict[str, Any] = {"state": "ready", "detail": "stub", "observed_at": None}
CRASH_EXIT = 7
FAULT_EXIT = 2
CALLBACK_ID = 1


def main(argv: list[str] | None = None) -> int:
    """按模式运行桩进程，返回进程退出码。"""
    arguments = list(sys.argv[1:] if argv is None else argv)
    mode = arguments[0] if arguments else "ok"
    control = Path(arguments[1]) if mode == "sequence" and len(arguments) > 1 else None
    if mode == "crash":
        return CRASH_EXIT
    if mode == "fault":
        _send_line({"kind": "fault", "message": "stub 加载失败"})
        return FAULT_EXIT
    _send_line(
        {
            "kind": "ready",
            "protocol": PROTOCOL_VERSION,
            "plugin_id": "stub",
            "capabilities": list(CAPABILITIES),
        }
    )
    while True:
        line = _read_line()
        if line is None:
            return 0
        message = _decode(line)
        if message is None:
            continue
        kind = message.get("kind")
        if kind == "shutdown":
            return 0
        if kind != "call":
            continue
        code = _handle_call(mode, control, message)
        if code is not None:
            return code


def _handle_call(mode: str, control: Path | None, message: dict[str, Any]) -> int | None:
    """处理一条 call；返回 None 表示继续会话，整数表示进程退出码。"""
    call_id = message.get("id")
    token = _consume_token(control) if mode == "sequence" else mode
    if token == "silent":
        return None
    if token == "crash":
        return CRASH_EXIT
    if token == "fault":
        _send_line({"kind": "fault", "message": "stub 被要求以故障退出"})
        return FAULT_EXIT
    if token == "badjson":
        _write_raw("not a json line")
        return 0
    if token == "callback":
        _send_line(
            {
                "kind": "callback",
                "id": call_id,
                "callback_id": CALLBACK_ID,
                "op": "record_evidence",
                "args": {"kind": "log", "path": "stub.log", "summary": "stub"},
            }
        )
        value = _await_callback_result()
        _send_line({"kind": "result", "id": call_id, "ok": True, "value": {"evidence": value}})
        return None
    if token == "notify":
        _send_line(
            {
                "kind": "notify",
                "op": "log",
                "id": call_id,
                "level": "info",
                "fields": {"message": "stub notify"},
            }
        )
    elif token != "ok":
        _send_line({"kind": "fault", "message": f"stub 未知行为：{token}"})
        return FAULT_EXIT
    _send_line({"kind": "result", "id": call_id, "ok": True, "value": OK_VALUE})
    return None


def _await_callback_result() -> Any:
    """等内核写回 callback_result；内核在等调用结果时同步处理回调。"""
    while True:
        line = _read_line()
        if line is None:
            raise SystemExit(0)
        message = _decode(line)
        if message is None:
            continue
        if message.get("kind") == "callback_result" and message.get("callback_id") == CALLBACK_ID:
            return message.get("value")
        if message.get("kind") == "shutdown":
            raise SystemExit(0)


def _consume_token(control: Path | None) -> str:
    """取控制文件第一行作为本次行为并写回剩余行；缺文件说明用例配置有问题。"""
    if control is None or not control.is_file():
        return "missing-control"
    tokens = [
        line.strip() for line in control.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    if not tokens:
        return "ok"
    token = tokens.pop(0)
    control.write_text("".join(f"{item}\n" for item in tokens), encoding="utf-8")
    return token


def _send_line(message: dict[str, Any]) -> None:
    """发一条协议消息；协议独占 stdout，插件输出不得混进来。"""
    _write_raw(json.dumps(message, ensure_ascii=True, separators=(",", ":")))


def _write_raw(text: str) -> None:
    sys.stdout.buffer.write(text.encode("utf-8") + b"\n")
    sys.stdout.buffer.flush()


def _read_line() -> str | None:
    """读一行 UTF-8 文本；stdin 关闭返回 None。"""
    raw = sys.stdin.buffer.readline()
    if not raw:
        return None
    return raw.decode("utf-8", errors="replace")


def _decode(line: str) -> dict[str, Any] | None:
    """解析内核消息；空行与非 JSON 行按坏协议跳过。"""
    text = line.strip()
    if not text:
        return None
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


if __name__ == "__main__":
    raise SystemExit(main())
