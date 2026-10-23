"""worker 子进程协议：行分隔 JSON 消息与 RunContext 载荷。

唯一真源是 ADR 0010。内核与 worker 只交换 JSON，不共享内存；协议常量、消息编解码
在这里，契约类型的编解码在 ``core/plugins/rpc_types.py``。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from contracts import RunContext
from core.plugins.errors import WorkerPayloadError

PROTOCOL_VERSION: Final = 1

CALL: Final = "call"
RESULT: Final = "result"
CALLBACK: Final = "callback"
CALLBACK_RESULT: Final = "callback_result"
NOTIFY: Final = "notify"
READY: Final = "ready"
FAULT: Final = "fault"
CANCEL: Final = "cancel"
SHUTDOWN: Final = "shutdown"

LOG_LEVELS: Final = frozenset({"debug", "info", "warn", "error"})


@dataclass(frozen=True, slots=True)
class ContextPayload:
    """随一次 call 下发的 RunContext 可序列化字段。"""

    run_id: str
    attempt: int
    template_id: str
    plugin_id: str
    environment_id: str
    config: Mapping[str, Any]
    work_dir: Path
    evidence_dir: Path
    deadline_at: datetime
    secrets: Mapping[str, str]


def encode_line(message: Mapping[str, Any]) -> str:
    """把一条消息编码成单行 JSON；消息里的换行会被转义。"""
    return json.dumps(dict(message), ensure_ascii=True, separators=(",", ":"))


def decode_line(line: str) -> dict[str, Any]:
    """解码一条消息；非法 JSON、非对象或非字符串键抛 ``WorkerPayloadError``。"""
    try:
        value = json.loads(line)
    except json.JSONDecodeError as error:
        raise WorkerPayloadError(f"协议行不是合法 JSON：{line[:120]!r}") from error
    if not isinstance(value, dict):
        raise WorkerPayloadError("协议消息必须是 JSON 对象")
    if not all(isinstance(key, str) for key in value):
        raise WorkerPayloadError("协议消息的键必须是字符串")
    return {key: item for key, item in value.items() if isinstance(key, str)}


def encode_context(ctx: RunContext) -> dict[str, Any]:
    """把 RunContext 里可跨进程的字段编码成 JSON。"""
    return {
        "run_id": ctx.run_id,
        "attempt": ctx.attempt,
        "template_id": ctx.template_id,
        "plugin_id": ctx.plugin_id,
        "environment_id": ctx.environment_id,
        "config": dict(ctx.config),
        "work_dir": str(ctx.work_dir),
        "evidence_dir": str(ctx.evidence_dir),
        "deadline_at": ctx.deadline_at.astimezone(UTC).isoformat(),
        "secrets": dict(ctx.secrets),
    }


def decode_context(value: object) -> ContextPayload:
    """解码 RunContext 载荷；缺字段或类型不符抛 ``WorkerPayloadError``。"""
    data = _mapping(value, "context")
    return ContextPayload(
        run_id=_text(data, "run_id", "context"),
        attempt=_integer(data, "attempt", "context"),
        template_id=_text(data, "template_id", "context"),
        plugin_id=_text(data, "plugin_id", "context"),
        environment_id=_text(data, "environment_id", "context"),
        config=_json_object(_required(data, "config", "context"), "context.config"),
        work_dir=Path(_text(data, "work_dir", "context")),
        evidence_dir=Path(_text(data, "evidence_dir", "context")),
        deadline_at=_required_time(data, "deadline_at", "context"),
        secrets=_text_map(_required(data, "secrets", "context"), "context.secrets"),
    )


def _mapping(value: object, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkerPayloadError(f"{where} 应为 JSON 对象")
    if not all(isinstance(key, str) for key in value):
        raise WorkerPayloadError(f"{where} 的键必须是字符串")
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _json_object(value: object, where: str) -> Mapping[str, Any]:
    return dict(_mapping(value, where))


def _text_map(value: object, where: str) -> Mapping[str, str]:
    data = _mapping(value, where)
    result: dict[str, str] = {}
    for key, item in data.items():
        if not isinstance(item, str):
            raise WorkerPayloadError(f"{where}.{key} 应为字符串")
        result[key] = item
    return result


def _required(data: Mapping[str, Any], key: str, where: str) -> object:
    if key not in data:
        raise WorkerPayloadError(f"{where} 缺少字段 {key}")
    return data[key]


def _text(data: Mapping[str, Any], key: str, where: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise WorkerPayloadError(f"{where}.{key} 应为字符串")
    return value


def _integer(data: Mapping[str, Any], key: str, where: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise WorkerPayloadError(f"{where}.{key} 应为整数")
    return value


def _required_time(data: Mapping[str, Any], key: str, where: str) -> datetime:
    value = _required(data, key, where)
    if not isinstance(value, str):
        raise WorkerPayloadError(f"{where}.{key} 应为 ISO 8601 字符串")
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise WorkerPayloadError(f"{where}.{key} 不是合法时刻：{value!r}") from error
    if moment.tzinfo is None:
        raise WorkerPayloadError(f"{where}.{key} 必须带时区")
    return moment.astimezone(UTC)
