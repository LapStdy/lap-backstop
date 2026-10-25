"""HTTP API 层：docs/03 端点的独立服务、鉴权与 JSON 线格式。"""

from __future__ import annotations

from core.api.server import ApiOptions, ApiServer, start_api_server
from core.api.wire import (
    ApiRequest,
    ApiResponse,
    content_etag,
    decode_json,
    encode_json,
    error_response,
)

__all__ = [
    "ApiOptions",
    "ApiRequest",
    "ApiResponse",
    "ApiServer",
    "content_etag",
    "decode_json",
    "encode_json",
    "error_response",
    "start_api_server",
]
