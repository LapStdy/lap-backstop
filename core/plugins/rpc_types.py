"""契约类型的 JSON 编解码：内核与 worker 边界只认这里转换出的对象。

解码按类型提示逐字段校验：缺字段、类型不符、未知枚举、naive 时刻都抛
``WorkerPayloadError``；未知的额外字段忽略，给协议向前兼容留余地。
"""

from __future__ import annotations

import dataclasses
import types
from collections.abc import Mapping
from dataclasses import MISSING
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Union, cast, get_args, get_origin, get_type_hints

from contracts import (
    CheckResult,
    EnvironmentProbe,
    EnvironmentRef,
    PhaseResult,
    RecoveryAction,
    RecoveryResult,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    Signal,
    StopMode,
    StopResult,
    Verdict,
)
from core.plugins.errors import WorkerPayloadError

_MAPPING_ORIGINS = (Mapping, dict)


def encode_value(value: Any) -> Any:
    """把契约对象转成 JSON 可序列化的取值。"""
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        return _encode_time(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: encode_value(getattr(value, item.name)) for item in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): encode_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [encode_value(item) for item in value]
    raise WorkerPayloadError(f"无法编码为 JSON 的取值：{type(value).__name__}")


def decode_value(expected: Any, value: Any, where: str) -> Any:
    """按契约类型解码并校验；非法载荷抛 ``WorkerPayloadError``。"""
    if expected is Any:
        return value
    origin = get_origin(expected)
    if origin is Union or origin is types.UnionType:
        return _decode_union(get_args(expected), value, where)
    if origin is tuple:
        return _decode_tuple(get_args(expected), value, where)
    if origin in _MAPPING_ORIGINS:
        args = get_args(expected)
        item_type = args[1] if len(args) == 2 else Any
        data = _mapping(value, where)
        return {key: decode_value(item_type, item, f"{where}.{key}") for key, item in data.items()}
    if not isinstance(expected, type):
        raise WorkerPayloadError(f"{where} 的契约类型不可解码：{expected!r}")
    return _decode_scalar(expected, value, where)


def decode_record[RecordT](cls: type[RecordT], value: Any, where: str) -> RecordT:
    """按 dataclass 字段解码一个契约对象。"""
    data = _mapping(value, where)
    hints = get_type_hints(cls)
    kwargs: dict[str, Any] = {}
    for item in dataclasses.fields(cast(Any, cls)):
        if item.name not in data:
            if item.default is not MISSING or item.default_factory is not MISSING:
                continue
            raise WorkerPayloadError(f"{where} 缺少字段 {item.name}")
        kwargs[item.name] = decode_value(hints[item.name], data[item.name], f"{where}.{item.name}")
    return cls(**kwargs)


def encode_environment_ref(ref: EnvironmentRef) -> dict[str, Any]:
    """环境引用转 JSON。"""
    return _record(ref)


def decode_environment_ref(value: Any) -> EnvironmentRef:
    """JSON 转环境引用。"""
    return decode_record(EnvironmentRef, value, "environment_ref")


def encode_script_ref(ref: ScriptRef) -> dict[str, Any]:
    """脚本引用转 JSON。"""
    return _record(ref)


def decode_script_ref(value: Any) -> ScriptRef:
    """JSON 转脚本引用。"""
    return decode_record(ScriptRef, value, "script_ref")


def encode_environment_probe(probe: EnvironmentProbe) -> dict[str, Any]:
    """环境探测结果转 JSON。"""
    return _record(probe)


def decode_environment_probe(value: Any) -> EnvironmentProbe:
    """JSON 转环境探测结果。"""
    return decode_record(EnvironmentProbe, value, "environment_probe")


def encode_phase_result(result: PhaseResult) -> dict[str, Any]:
    """阶段结果转 JSON。"""
    return _record(result)


def decode_phase_result(value: Any) -> PhaseResult:
    """JSON 转阶段结果。"""
    return decode_record(PhaseResult, value, "phase_result")


def encode_script_handle(handle: ScriptHandle) -> dict[str, Any]:
    """脚本句柄转 JSON。"""
    return _record(handle)


def decode_script_handle(value: Any) -> ScriptHandle:
    """JSON 转脚本句柄。"""
    return decode_record(ScriptHandle, value, "script_handle")


def encode_signal(signal: Signal) -> dict[str, Any]:
    """脚本信号转 JSON。"""
    return _record(signal)


def decode_signal(value: Any) -> Signal:
    """JSON 转脚本信号。"""
    return decode_record(Signal, value, "signal")


def encode_script_observation(observation: ScriptObservation) -> dict[str, Any]:
    """脚本观察结果转 JSON。"""
    return _record(observation)


def decode_script_observation(value: Any) -> ScriptObservation:
    """JSON 转脚本观察结果。"""
    return decode_record(ScriptObservation, value, "script_observation")


def encode_stop_result(result: StopResult) -> dict[str, Any]:
    """停止结果转 JSON。"""
    return _record(result)


def decode_stop_result(value: Any) -> StopResult:
    """JSON 转停止结果。"""
    return decode_record(StopResult, value, "stop_result")


def encode_recovery_result(result: RecoveryResult) -> dict[str, Any]:
    """恢复结果转 JSON。"""
    return _record(result)


def decode_recovery_result(value: Any) -> RecoveryResult:
    """JSON 转恢复结果。"""
    return decode_record(RecoveryResult, value, "recovery_result")


def encode_verdict(verdict: Verdict) -> dict[str, Any]:
    """结论转 JSON。"""
    return _record(verdict)


def decode_verdict(value: Any) -> Verdict:
    """JSON 转结论。"""
    return decode_record(Verdict, value, "verdict")


def encode_check_result(check: CheckResult) -> dict[str, Any]:
    """检查项转 JSON。"""
    return _record(check)


def decode_check_result(value: Any) -> CheckResult:
    """JSON 转检查项。"""
    return decode_record(CheckResult, value, "check_result")


def encode_stop_mode(mode: StopMode) -> str:
    """停止方式转协议取值。"""
    return mode.value


def decode_stop_mode(value: Any) -> StopMode:
    """协议取值转停止方式。"""
    return decode_record_mode(StopMode, value, "stop_mode")


def encode_recovery_action(action: RecoveryAction) -> str:
    """恢复动作转协议取值。"""
    return action.value


def decode_recovery_action(value: Any) -> RecoveryAction:
    """协议取值转恢复动作。"""
    return decode_record_mode(RecoveryAction, value, "recovery_action")


def decode_record_mode[ModeT: StrEnum](enum_cls: type[ModeT], value: Any, where: str) -> ModeT:
    """把协议里的状态字符串还原成枚举。"""
    if not isinstance(value, str):
        raise WorkerPayloadError(f"{where} 应为状态字符串")
    try:
        return enum_cls(value)
    except ValueError as error:
        raise WorkerPayloadError(f"{where} 出现未知取值：{value!r}") from error


def _record(value: object) -> dict[str, Any]:
    encoded = encode_value(value)
    if not isinstance(encoded, dict):
        raise WorkerPayloadError(f"契约对象编码结果不是对象：{type(value).__name__}")
    return encoded


def _decode_union(options: tuple[Any, ...], value: Any, where: str) -> Any:
    if value is None and type(None) in options:
        return None
    for option in options:
        if option is type(None):
            continue
        return decode_value(option, value, where)
    raise WorkerPayloadError(f"{where} 没有可解码的联合类型")


def _decode_tuple(args: tuple[Any, ...], value: Any, where: str) -> tuple[Any, ...]:
    items = _items(value, where)
    if len(args) == 2 and args[1] is Ellipsis:
        return tuple(
            decode_value(args[0], item, f"{where}[{index}]") for index, item in enumerate(items)
        )
    if len(args) != len(items):
        raise WorkerPayloadError(f"{where} 的数组长度应为 {len(args)}")
    return tuple(
        decode_value(expected, item, f"{where}[{index}]")
        for index, (expected, item) in enumerate(zip(args, items, strict=True))
    )


def _decode_scalar(expected: Any, value: Any, where: str) -> Any:
    if issubclass(expected, StrEnum):
        return decode_record_mode(expected, value, where)
    if expected is bool:
        if not isinstance(value, bool):
            raise WorkerPayloadError(f"{where} 应为布尔值")
        return value
    if expected is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise WorkerPayloadError(f"{where} 应为整数")
        return value
    if expected is float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise WorkerPayloadError(f"{where} 应为数字")
        return float(value)
    if expected is str:
        if not isinstance(value, str):
            raise WorkerPayloadError(f"{where} 应为字符串")
        return value
    if expected is datetime:
        return _decode_time(value, where)
    if dataclasses.is_dataclass(expected) and isinstance(expected, type):
        return decode_record(expected, value, where)
    raise WorkerPayloadError(f"{where} 的契约类型不可解码：{expected.__name__}")


def _mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkerPayloadError(f"{where} 应为 JSON 对象")
    if not all(isinstance(key, str) for key in value):
        raise WorkerPayloadError(f"{where} 的键必须是字符串")
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _items(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        raise WorkerPayloadError(f"{where} 应为数组")
    return value


def _encode_time(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat()


def _decode_time(value: Any, where: str) -> datetime:
    if not isinstance(value, str):
        raise WorkerPayloadError(f"{where} 应为 ISO 8601 字符串")
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise WorkerPayloadError(f"{where} 不是合法时刻：{value!r}") from error
    if moment.tzinfo is None:
        raise WorkerPayloadError(f"{where} 必须带时区")
    return moment.astimezone(UTC)
