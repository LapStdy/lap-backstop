"""worker 子进程入口：加载插件，按行 JSON 执行能力调用。

内核只经 stdin/stdout 与本进程交换消息（ADR 0010）。协议独占标准输出，插件打印被
重定向到 stderr；插件的 emit、record_evidence 与日志经回调交给内核写库。
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import queue
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Final

from contracts import CAPABILITIES, RunContext
from core.plugins import rpc, rpc_types
from core.plugins.errors import PluginError, WorkerPayloadError
from core.plugins.loader import load_plugin

# 内核在等待调用结果时同步处理回调；这个超时只防死锁，不是业务截止时间。
_CALLBACK_TIMEOUT_SECONDS: Final = 30.0


class _ProcessClock:
    """worker 内的时钟：系统 UTC 与单调秒；虚拟时钟不跨进程（docs/08）。"""

    def monotonic(self) -> float:
        return time.monotonic()

    def utc_now(self) -> datetime:
        return datetime.now(UTC)


class _CancelFlag:
    """按调用 id 记录的取消标记；插件只能查询。"""

    def __init__(self, call_id: int, cancelled: set[int], lock: threading.Lock) -> None:
        self._call_id = call_id
        self._cancelled = cancelled
        self._lock = lock

    def is_cancelled(self) -> bool:
        with self._lock:
            return self._call_id in self._cancelled


class _ProtocolWriter:
    """写协议消息：一行一条 UTF-8 JSON，加锁保证多线程不会交错。"""

    def __init__(self, stream: BinaryIO) -> None:
        self._stream = stream
        self._lock = threading.Lock()

    def send(self, message: Mapping[str, Any]) -> None:
        payload = rpc.encode_line(message).encode("utf-8") + b"\n"
        with self._lock:
            self._stream.write(payload)
            self._stream.flush()


class _RemoteLogger:
    """插件日志：转成 notify 消息交给内核写日志。"""

    def __init__(self, writer: _ProtocolWriter, call_id: int) -> None:
        self._writer = writer
        self._call_id = call_id

    def debug(self, fields: Mapping[str, Any]) -> None:
        self._send("debug", fields)

    def info(self, fields: Mapping[str, Any]) -> None:
        self._send("info", fields)

    def warn(self, fields: Mapping[str, Any]) -> None:
        self._send("warn", fields)

    def error(self, fields: Mapping[str, Any]) -> None:
        self._send("error", fields)

    def _send(self, level: str, fields: Mapping[str, Any]) -> None:
        safe: dict[str, Any] = json.loads(json.dumps(dict(fields), default=str, ensure_ascii=True))
        self._writer.send(
            {"kind": rpc.NOTIFY, "op": "log", "id": self._call_id, "level": level, "fields": safe}
        )


class _Reader(threading.Thread):
    """读内核消息：调用进队列，回调结果进队列，cancel 置位，shutdown 退出。"""

    def __init__(
        self,
        stream: BinaryIO,
        *,
        calls: queue.Queue[dict[str, Any] | None],
        callbacks: queue.Queue[dict[str, Any]],
        cancelled: set[int],
        lock: threading.Lock,
    ) -> None:
        super().__init__(name="worker-reader", daemon=True)
        self._stream = stream
        self._calls = calls
        self._callbacks = callbacks
        self._cancelled = cancelled
        self._lock = lock

    def run(self) -> None:
        try:
            for raw in self._stream:
                text = raw.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    message = rpc.decode_line(text)
                except WorkerPayloadError:
                    continue
                kind = message.get("kind")
                if kind == rpc.CALL:
                    self._calls.put(message)
                elif kind == rpc.CALLBACK_RESULT:
                    self._callbacks.put(message)
                elif kind == rpc.CANCEL:
                    cancelled_id = message.get("id")
                    if isinstance(cancelled_id, int):
                        with self._lock:
                            self._cancelled.add(cancelled_id)
                elif kind == rpc.SHUTDOWN:
                    return
        finally:
            self._calls.put(None)


class _Server:
    """一次 worker 会话：声明能力、响应调用、转发插件副作用。"""

    def __init__(
        self,
        plugin: object,
        writer: _ProtocolWriter,
        *,
        plugin_id: str,
        incoming: BinaryIO,
    ) -> None:
        self._plugin = plugin
        self._plugin_id = plugin_id
        self._writer = writer
        self._calls: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._callbacks: queue.Queue[dict[str, Any]] = queue.Queue()
        self._cancelled: set[int] = set()
        self._cancel_lock = threading.Lock()
        self._callback_ids = itertools.count(1)
        self._clock = _ProcessClock()
        self._reader = _Reader(
            incoming,
            calls=self._calls,
            callbacks=self._callbacks,
            cancelled=self._cancelled,
            lock=self._cancel_lock,
        )

    def run(self) -> int:
        """发送 ready 后逐条处理调用，直到内核关闭 stdin 或发 shutdown。"""
        self._writer.send(
            {
                "kind": rpc.READY,
                "protocol": rpc.PROTOCOL_VERSION,
                "plugin_id": self._plugin_id,
                "capabilities": [
                    name for name in sorted(CAPABILITIES) if self._capability(name) is not None
                ],
            }
        )
        self._reader.start()
        while True:
            message = self._calls.get()
            if message is None:
                return 0
            self._handle(message)

    def _handle(self, message: Mapping[str, Any]) -> None:
        call_id = message.get("id")
        if not isinstance(call_id, int) or isinstance(call_id, bool):
            return
        try:
            capability = _text(message, "capability")
            method = _text(message, "method")
            args = _object(message, "args")
            encoder = _encoder(capability, method)
            context = self._context(rpc.decode_context(message.get("context")), call_id)
            value = encoder(self._invoke(capability, method, args, context))
        except Exception as error:
            self._writer.send(
                {
                    "kind": rpc.RESULT,
                    "id": call_id,
                    "ok": False,
                    "error": {"type": type(error).__name__, "message": str(error)},
                }
            )
            return
        self._writer.send({"kind": rpc.RESULT, "id": call_id, "ok": True, "value": value})

    def _invoke(
        self,
        capability: str,
        method: str,
        args: Mapping[str, Any],
        context: RunContext,
    ) -> object:
        target = self._capability(capability)
        if target is None:
            raise WorkerPayloadError(f"插件 {self._plugin_id} 没有 {capability} 能力")
        signature = _CALLS.get((capability, method))
        if signature is None:
            raise WorkerPayloadError(f"不支持的能力调用：{capability}.{method}")
        leading, trailing = signature
        call = getattr(target, method, None)
        if call is None:
            raise WorkerPayloadError(f"{capability} 缺少方法 {method}")
        return call(*leading(args), context, *trailing(args))

    def _capability(self, name: str) -> object | None:
        value = getattr(self._plugin, name, None)
        if value is None:
            return None
        return value

    def _context(self, payload: rpc.ContextPayload, call_id: int) -> RunContext:
        return RunContext(
            run_id=payload.run_id,
            attempt=payload.attempt,
            template_id=payload.template_id,
            plugin_id=payload.plugin_id,
            environment_id=payload.environment_id,
            config=payload.config,
            work_dir=payload.work_dir,
            evidence_dir=payload.evidence_dir,
            deadline_at=payload.deadline_at,
            cancel=_CancelFlag(call_id, self._cancelled, self._cancel_lock),
            clock=self._clock,
            secrets=payload.secrets,
            log=_RemoteLogger(self._writer, call_id),
            emit=lambda event, fields: self._emit(call_id, event, fields),
            record_evidence=lambda kind, path, summary: self._record_evidence(
                call_id, kind, path, summary
            ),
        )

    def _emit(self, call_id: int, event: str, payload: Mapping[str, Any]) -> None:
        self._callback(call_id, "emit", {"event": event, "payload": dict(payload)})

    def _record_evidence(self, call_id: int, kind: str, path: Path, summary: str) -> str:
        value = self._callback(
            call_id,
            "record_evidence",
            {"kind": kind, "path": str(path), "summary": summary},
        )
        if not isinstance(value, str):
            raise WorkerPayloadError("内核返回的证据 id 不是字符串")
        return value

    def _callback(self, call_id: int, op: str, args: Mapping[str, Any]) -> object:
        callback_id = next(self._callback_ids)
        self._writer.send(
            {
                "kind": rpc.CALLBACK,
                "id": call_id,
                "callback_id": callback_id,
                "op": op,
                "args": dict(args),
            }
        )
        deadline = time.monotonic() + _CALLBACK_TIMEOUT_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WorkerPayloadError(f"内核未在限期内响应回调：{op}")
            try:
                message = self._callbacks.get(timeout=remaining)
            except queue.Empty:
                continue
            if message.get("callback_id") != callback_id:
                continue
            if message.get("ok") is True:
                return message.get("value")
            raise WorkerPayloadError(f"内核拒绝回调 {op}：{message.get('error')}")


_Leading = Callable[[Mapping[str, Any]], tuple[object, ...]]
_Signature = tuple[_Leading, _Leading]


def _decode_environment(args: Mapping[str, Any]) -> tuple[object, ...]:
    return (rpc_types.decode_environment_ref(_object(args, "env")),)


def _decode_script(args: Mapping[str, Any]) -> tuple[object, ...]:
    return (rpc_types.decode_script_ref(_object(args, "script")),)


def _decode_handle(args: Mapping[str, Any]) -> tuple[object, ...]:
    return (rpc_types.decode_script_handle(_object(args, "handle")),)


def _decode_mode(args: Mapping[str, Any]) -> tuple[object, ...]:
    return (rpc_types.decode_stop_mode(args.get("mode")),)


def _decode_action(args: Mapping[str, Any]) -> tuple[object, ...]:
    return (rpc_types.decode_recovery_action(args.get("action")),)


def _decode_keep_warm(args: Mapping[str, Any]) -> tuple[object, ...]:
    return (_boolean(args, "keep_warm"),)


def _decode_minutes(args: Mapping[str, Any]) -> tuple[object, ...]:
    return (_integer(args, "minutes"),)


def _nothing(_args: Mapping[str, Any]) -> tuple[object, ...]:
    return ()


_CALLS: Final[Mapping[tuple[str, str], _Signature]] = {
    ("environment", "probe"): (_decode_environment, _nothing),
    ("environment", "prepare"): (_decode_environment, _nothing),
    ("environment", "reset"): (_decode_environment, _nothing),
    ("environment", "cleanup"): (_decode_environment, _decode_keep_warm),
    ("environment", "warm"): (_decode_environment, _decode_minutes),
    ("script", "launch"): (_decode_script, _nothing),
    ("script", "observe"): (_decode_handle, _nothing),
    ("script", "stop"): (_decode_handle, _decode_mode),
    ("script", "recover"): (_decode_handle, _decode_action),
    ("verifier", "verify"): (_nothing, _nothing),
}

_RESULT_ENCODERS: Final[Mapping[tuple[str, str], Callable[[Any], dict[str, Any]]]] = {
    ("environment", "probe"): rpc_types.encode_environment_probe,
    ("environment", "prepare"): rpc_types.encode_phase_result,
    ("environment", "reset"): rpc_types.encode_phase_result,
    ("environment", "cleanup"): rpc_types.encode_phase_result,
    ("environment", "warm"): rpc_types.encode_phase_result,
    ("script", "launch"): rpc_types.encode_script_handle,
    ("script", "observe"): rpc_types.encode_script_observation,
    ("script", "stop"): rpc_types.encode_stop_result,
    ("script", "recover"): rpc_types.encode_recovery_result,
    ("verifier", "verify"): rpc_types.encode_verdict,
}


def _encoder(capability: str, method: str) -> Callable[[Any], dict[str, Any]]:
    encoder = _RESULT_ENCODERS.get((capability, method))
    if encoder is None:
        raise WorkerPayloadError(f"不支持的能力调用：{capability}.{method}")
    return encoder


def _object(data: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = data.get(key)
    if not isinstance(value, Mapping):
        raise WorkerPayloadError(f"{key} 应为 JSON 对象")
    return value


def _text(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise WorkerPayloadError(f"{key} 应为字符串")
    return value


def _boolean(data: Mapping[str, Any], key: str) -> bool:
    value = data.get(key)
    if not isinstance(value, bool):
        raise WorkerPayloadError(f"{key} 应为布尔值")
    return value


def _integer(data: Mapping[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise WorkerPayloadError(f"{key} 应为整数")
    return value


def _parse_arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="worker 子进程：加载插件并执行能力调用")
    parser.add_argument("plugin_id", help="插件 id，对应 plugins/<id>/")
    parser.add_argument("--plugins-root", default="plugins", help="插件根目录")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """进程入口：协议独占复制出的 stdin/stdout，fd 0/1 改道 devnull 与 stderr。

    插件拉起的子进程继承的是 devnull 与 stderr，既读不到协议，也写不进协议。
    """
    arguments = _parse_arguments(sys.argv[1:] if argv is None else argv)
    protocol = os.fdopen(os.dup(1), "wb", buffering=0)
    incoming = os.fdopen(os.dup(0), "rb", buffering=0)
    os.dup2(2, 1)
    os.dup2(os.open(os.devnull, os.O_RDONLY), 0)
    writer = _ProtocolWriter(protocol)
    sys.stdout = sys.stderr
    try:
        plugin = load_plugin(Path(arguments.plugins_root), str(arguments.plugin_id))
    except PluginError as error:
        writer.send({"kind": rpc.FAULT, "message": str(error)})
        return 2
    return _Server(
        plugin,
        writer,
        plugin_id=str(arguments.plugin_id),
        incoming=incoming,
    ).run()


if __name__ == "__main__":
    raise SystemExit(main())
