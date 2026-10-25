"""HTTP 线格式：请求/响应值对象、JSON 编解码、错误响应与内容 ETag。

docs/03 的 HTTP 约定在这里落成字节：JSON 字段 snake_case、错误体带错误码、
ETag 由资源内容决定而不是由时间精度决定。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from contracts import ContractError, ErrorCode, http_status


@dataclass(frozen=True, slots=True)
class ApiRequest:
    """一次进入应用的请求；query 与 headers 的键一律小写。"""

    method: str
    path: str
    query: Mapping[str, str]
    headers: Mapping[str, str]
    body: bytes


@dataclass(frozen=True, slots=True)
class ApiResponse:
    """应用的应答；payload 是可直接 JSON 化的 snake_case 结构。"""

    status: int
    payload: object
    etag: str | None = None


def decode_json(body: bytes) -> object:
    """解析请求体。

    Raises:
        ContractError: 请求体为空、不是 UTF-8 或不是合法 JSON，错误码 E_BAD_REQUEST。
    """
    if not body:
        raise ContractError(ErrorCode.BAD_REQUEST, "请求体不能为空")
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(ErrorCode.BAD_REQUEST, f"请求体不是合法 JSON：{error}") from error


def encode_json(payload: object) -> bytes:
    """应答编码；键排序保证同一资源输出字节稳定。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def error_response(code: ErrorCode, message: str) -> ApiResponse:
    """按 docs/03 的错误体与 HTTP 状态映射构造应答。"""
    return ApiResponse(
        status=http_status(code),
        payload={"error": {"code": code.value, "message": message}},
    )


def content_etag(payload: object) -> str:
    """资源内容的强 ETag；同一秒内的两次修改也能区分。"""
    digest = hashlib.sha256(encode_json(payload)).hexdigest()[:16]
    return f'"{digest}"'


def check_if_match(header: str | None, current: str) -> None:
    """If-Match 校验：``*`` 匹配任意存在；否则必须等于当前 ETag。

    Raises:
        ContractError: 头存在且不匹配当前版本，错误码 E_CONFLICT。
    """
    if header is None or header == "*" or header == current:
        return
    raise ContractError(ErrorCode.CONFLICT, "If-Match 与当前版本不符，先重读再改")


__all__ = [
    "ApiRequest",
    "ApiResponse",
    "check_if_match",
    "content_etag",
    "decode_json",
    "encode_json",
    "error_response",
]
