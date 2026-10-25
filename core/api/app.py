"""API 应用：Bearer 鉴权、路由分发与错误映射；每请求一条独立数据库连接。

线程模型：HTTP 服务每个请求一个线程，SQLite 连接按请求独立打开（WAL），
不与 serve 循环共用连接对象，写冲突由 busy_timeout 吸收。
"""

from __future__ import annotations

import hmac
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from contracts import Clock, ContractError, ErrorCode, Logger
from core.api import store
from core.api.ctx import Handler, RequestCtx
from core.api.schedules_api import create_schedule, get_schedule, list_schedules
from core.api.system_api import get_settings, health, put_settings
from core.api.wire import ApiRequest, ApiResponse, decode_json, error_response
from core.clock import SystemClock
from core.logging import NullLogger
from storage import open_database

API_PREFIX = "/api/v1"


@dataclass(frozen=True, slots=True)
class Route:
    """一条路由：方法、按 ``{name}`` 占位的路径段与处理器。"""

    method: str
    segments: tuple[str, ...]
    handler: Handler


ROUTES: tuple[Route, ...] = (
    Route("GET", ("health",), health),
    Route("GET", ("settings",), get_settings),
    Route("PUT", ("settings",), put_settings),
    Route("GET", ("schedules",), list_schedules),
    Route("POST", ("schedules",), create_schedule),
    Route("GET", ("schedules", "{id}"), get_schedule),
)


class ApiApp:
    """无传输依赖的请求处理核心；HTTP 服务与进程内测试共用。"""

    def __init__(
        self,
        database: Path,
        *,
        clock: Clock | None = None,
        log: Logger | None = None,
    ) -> None:
        self._database = database
        self._clock: Clock = SystemClock() if clock is None else clock
        self._log: Logger = NullLogger() if log is None else log
        self._routes = ROUTES

    def handle(self, request: ApiRequest) -> ApiResponse:
        """处理一次请求；任何失败都映射成错误响应并写一行请求日志。"""
        started = time.monotonic()
        response = self._safe_handle(request)
        self._log.info(
            {
                "event": "api.request",
                "method": request.method,
                "path": request.path,
                "status": response.status,
                "duration_ms": int((time.monotonic() - started) * 1000),
            }
        )
        return response

    def _safe_handle(self, request: ApiRequest) -> ApiResponse:
        try:
            with closing(open_database(self._database)) as connection:
                self._authorize(connection, request)
                return self._dispatch(connection, request)
        except ContractError as error:
            return error_response(error.code, error.message)
        except sqlite3.Error as error:
            self._log.error({"event": "api.storage_error", "error": str(error)})
            return error_response(ErrorCode.STORAGE, "数据库读写失败")
        except Exception as error:
            # 堆栈与原始错误只进日志；界面只看到人话与错误码（docs/11 日志一节）。
            self._log.error({"event": "api.internal_error", "error": repr(error)})
            return error_response(ErrorCode.INTERNAL, "未分类内部错误，详情见日志")

    def _authorize(self, connection: sqlite3.Connection, request: ApiRequest) -> None:
        """校验 Bearer token；未配置 token 时拒绝一切业务请求（401）。"""
        token = store.bearer_token(connection)
        scheme, _, value = request.headers.get("authorization", "").partition(" ")
        credentials_match = (
            token is not None
            and scheme.lower() == "bearer"
            and hmac.compare_digest(value.encode("utf-8"), token.encode("utf-8"))
        )
        if not credentials_match:
            raise ContractError(ErrorCode.AUTH, "鉴权失败：缺少或错误的 Bearer token")

    def _dispatch(self, connection: sqlite3.Connection, request: ApiRequest) -> ApiResponse:
        matched = _match(self._routes, request.method, request.path)
        if matched is None:
            raise ContractError(
                ErrorCode.NOT_FOUND,
                f"未知端点：{request.method} {request.path}",
            )
        route, params = matched
        body = decode_json(request.body) if request.body else None
        context = RequestCtx(
            connection=connection,
            clock=self._clock,
            params=params,
            query=request.query,
            headers=request.headers,
            body=body,
        )
        return route.handler(context)


def _match(
    routes: tuple[Route, ...],
    method: str,
    path: str,
) -> tuple[Route, dict[str, str]] | None:
    """按方法与路径找路由；路径必须落在 /api/v1 前缀下。"""
    segments = _split(path)
    if not segments:
        return None
    for route in routes:
        params = _match_route(route, method, segments)
        if params is not None:
            return route, params
    return None


def _split(path: str) -> tuple[str, ...]:
    """去掉版本前缀并切分段；前缀不符返回空元组，由调用方按 404 处理。"""
    if not path.startswith(API_PREFIX + "/"):
        return ()
    return tuple(part for part in path[len(API_PREFIX) :].split("/") if part)


def _match_route(
    route: Route,
    method: str,
    segments: tuple[str, ...],
) -> dict[str, str] | None:
    if route.method != method or len(route.segments) != len(segments):
        return None
    params: dict[str, str] = {}
    for pattern, actual in zip(route.segments, segments, strict=True):
        if pattern.startswith("{") and pattern.endswith("}"):
            params[pattern[1:-1]] = actual
        elif pattern != actual:
            return None
    return params


__all__ = ["API_PREFIX", "ROUTES", "ApiApp", "Route"]
