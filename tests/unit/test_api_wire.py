"""API 线格式：JSON 编解码、错误响应映射与内容 ETag。"""

from __future__ import annotations

import json

import pytest

from contracts import ContractError, ErrorCode
from core.api.wire import content_etag, decode_json, encode_json, error_response


def test_decode_json_accepts_object_and_array() -> None:
    assert decode_json(b'{"a": 1}') == {"a": 1}
    assert decode_json(b"[1, 2]") == [1, 2]


def test_decode_json_keeps_unicode_payload() -> None:
    assert decode_json('{"名字": "日常"}'.encode()) == {"名字": "日常"}


@pytest.mark.parametrize(
    "body",
    [b"", b"{", b"\xff\xfe"],
)
def test_decode_json_rejects_empty_or_malformed(body: bytes) -> None:
    with pytest.raises(ContractError) as raised:
        decode_json(body)
    assert raised.value.code is ErrorCode.BAD_REQUEST


def test_encode_json_is_sorted_and_reversible() -> None:
    raw = encode_json({"b": 1, "a": {"名字": "值"}})
    assert json.loads(raw.decode("utf-8")) == {"a": {"名字": "值"}, "b": 1}
    assert raw.index(b'"a"') < raw.index(b'"b"')


@pytest.mark.parametrize(
    ("code", "status"),
    [
        (ErrorCode.BAD_REQUEST, 400),
        (ErrorCode.CONFIG, 400),
        (ErrorCode.AUTH, 401),
        (ErrorCode.NOT_FOUND, 404),
        (ErrorCode.CONFLICT, 409),
        (ErrorCode.PLUGIN_VERSION, 409),
        (ErrorCode.BUSY, 429),
        (ErrorCode.DISK, 507),
        (ErrorCode.INTERNAL, 500),
    ],
)
def test_error_response_maps_contracts_status(code: ErrorCode, status: int) -> None:
    response = error_response(code, "说明")
    assert response.status == status
    assert response.payload == {"error": {"code": code.value, "message": "说明"}}


def test_content_etag_changes_only_with_content() -> None:
    first = content_etag({"updated_at": "t", "name": "日常"})
    same = content_etag({"name": "日常", "updated_at": "t"})
    other = content_etag({"updated_at": "t", "name": "改名"})
    assert first == same
    assert first != other
    assert first.startswith('"') and first.endswith('"')
