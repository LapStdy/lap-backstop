"""请求字段解析：类型收窄到统一错误；资源专属校验留在各自端点模块。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from contracts import ContractError, ErrorCode


def require_text(values: Mapping[str, Any], key: str) -> str:
    """非空字符串字段。"""
    value = values.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是非空字符串")
    return value


def require_enum(
    values: Mapping[str, Any],
    key: str,
    allowed: frozenset[str],
) -> str:
    """枚举字段；取值必须在允许集合内。"""
    value = values.get(key)
    # 先收窄成字符串再做成员判断：集合成员判断对 list/dict 这类不可哈希值会抛
    # TypeError，落到兜底 500，而入参非法应当按 E_BAD_REQUEST 回 400。
    if not isinstance(value, str) or value not in allowed:
        raise ContractError(
            ErrorCode.BAD_REQUEST,
            f"字段 {key} 取值必须是 {'、'.join(sorted(allowed))} 之一",
        )
    return value


def require_int(
    values: Mapping[str, Any],
    key: str,
    *,
    minimum: int | None = None,
) -> int:
    """整数字段；布尔不算整数，可带下限。"""
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是整数")
    if minimum is not None and value < minimum:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 不能小于 {minimum}")
    return value


def require_bool(values: Mapping[str, Any], key: str) -> bool:
    """布尔字段。"""
    value = values.get(key)
    if not isinstance(value, bool):
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是布尔值")
    return value


def require_object(values: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    """JSON 对象字段；payload 结构由插件 schema 决定，这里只收窄形态。"""
    value = values.get(key)
    if not isinstance(value, dict):
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是 JSON 对象")
    return value


def require_timestamp(values: Mapping[str, Any], key: str) -> str:
    """RFC3339 时间字段；必须带时区，返回规范化 ISO 文本。"""
    value = values.get(key)
    if not isinstance(value, str):
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须是 RFC3339 时间")
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as error:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 不是合法时间：{value}") from error
    if moment.tzinfo is None:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 必须带时区")
    return moment.isoformat()


def require_timezone(values: Mapping[str, Any], key: str) -> str:
    """IANA 时区字段。"""
    name = require_text(values, key)
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as error:
        raise ContractError(ErrorCode.BAD_REQUEST, f"字段 {key} 不是合法时区：{name}") from error
    return name


__all__ = [
    "require_bool",
    "require_enum",
    "require_int",
    "require_object",
    "require_text",
    "require_timestamp",
    "require_timezone",
]
