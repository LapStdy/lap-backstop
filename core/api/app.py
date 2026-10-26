"""API 应用：Bearer 鉴权、路由分发与错误映射；每请求一条独立数据库连接。

线程模型：HTTP 服务每个请求一个线程，SQLite 连接按请求独立打开（WAL），
不与 serve 循环共用连接对象，写冲突由 busy_timeout 吸收。
"""

from __future__ import annotations

import hmac
import sqlite3
import subprocess
import threading
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from contracts import Clock, ContractError, ErrorCode, Logger
from core.api import store
from core.api.ctx import Handler, RequestCtx
from core.api.events_api import StreamOptions, stream_frames, stream_run_frames
from core.api.export_api import get_export
from core.api.hostops_api import post_tasks_register
from core.api.notifications_api import post_notification_test
from core.api.runs_api import get_evidence_file, get_run, list_runs, require_run, stop_run
from core.api.schedules_api import (
    create_schedule,
    delete_schedule,
    get_schedule,
    list_schedules,
    put_schedule,
    run_now,
)
from core.api.system_api import (
    get_environment,
    get_plugin,
    get_settings,
    get_status,
    health,
    list_environments,
    list_leases,
    list_plugins,
    list_resources,
    put_settings,
)
from core.api.templates_api import (
    create_template,
    delete_template,
    get_template,
    list_templates,
    put_template,
)
from core.api.wire import ApiRequest, ApiResponse, decode_json, error_response
from core.clock import SystemClock
from core.hostops.tasks import Runner
from core.logging import NullLogger
from storage import open_database

API_PREFIX = "/api/v1"
# /events 是流式端点，不走 ROUTES 的请求-响应形态；能力清单仍要如实列出。
EVENTS_CAPABILITY = "events"


@dataclass(frozen=True, slots=True)
class Route:
    """一条路由：方法、带 ``{name}`` 参数捕获的路径段与处理器。"""

    method: str
    segments: tuple[str, ...]
    handler: Handler


ROUTES: tuple[Route, ...] = (
    Route("GET", ("health",), health),
    Route("GET", ("status",), get_status),
    Route("GET", ("settings",), get_settings),
    Route("PUT", ("settings",), put_settings),
    Route("POST", ("notifications", "test"), post_notification_test),
    Route("POST", ("host", "tasks", "register"), post_tasks_register),
    Route("GET", ("export",), get_export),
    Route("GET", ("schedules",), list_schedules),
    Route("POST", ("schedules",), create_schedule),
    Route("GET", ("schedules", "{id}"), get_schedule),
    Route("PUT", ("schedules", "{id}"), put_schedule),
    Route("DELETE", ("schedules", "{id}"), delete_schedule),
    Route("GET", ("job-templates",), list_templates),
    Route("POST", ("job-templates",), create_template),
    Route("GET", ("job-templates", "{id}"), get_template),
    Route("PUT", ("job-templates", "{id}"), put_template),
    Route("DELETE", ("job-templates", "{id}"), delete_template),
    Route("GET", ("runs",), list_runs),
    Route("GET", ("runs", "{id}"), get_run),
    Route("GET", ("runs", "{id}", "evidence", "{name}"), get_evidence_file),
    Route("POST", ("runs", "{id}", "stop"), stop_run),
    Route("POST", ("schedules", "{id}", "run"), run_now),
    Route("GET", ("environments",), list_environments),
    Route("GET", ("environments", "{id}"), get_environment),
    Route("GET", ("plugins",), list_plugins),
    Route("GET", ("plugins", "{id}"), get_plugin),
    Route("GET", ("resources",), list_resources),
    Route("GET", ("resources", "leases"), list_leases),
)


class ApiApp:
    """无传输依赖的请求处理核心；HTTP 服务与进程内测试共用。"""

    def __init__(
        self,
        database: Path,
        *,
        clock: Clock | None = None,
        log: Logger | None = None,
        task_runner: Runner = subprocess.run,
    ) -> None:
        self._database = database
        self._clock: Clock = SystemClock() if clock is None else clock
        self._log: Logger = NullLogger() if log is None else log
        self._task_runner: Runner = task_runner
        self._routes = ROUTES
        self._capabilities = tuple(
            sorted({route.segments[0] for route in ROUTES} | {EVENTS_CAPABILITY})
        )

    def handle(self, request: ApiRequest) -> ApiResponse:
        """处理一次请求；任何失败都映射成错误响应并写一行请求日志。"""
        started = self._clock.monotonic()
        response = self._safe_handle(request)
        self._log.info(
            {
                "event": "api.request",
                "method": request.method,
                "path": request.path,
                "status": response.status,
                "duration_ms": int((self._clock.monotonic() - started) * 1000),
            }
        )
        return response

    def authorize(self, request: ApiRequest) -> None:
        """只做鉴权不进路由；长连接端点开流前复用同一套校验。

        Raises:
            ContractError: 凭据缺失或不匹配，错误码 E_AUTH。
        """
        with closing(open_database(self._database)) as connection:
            self._authorize(connection, request)

    def event_frames(
        self,
        last_event_id: str | None,
        *,
        stop: threading.Event,
    ) -> Iterator[bytes]:
        """全局事件流的 SSE 帧迭代器；传输层鉴权后逐帧写出，游标由帧携带。"""
        return stream_frames(
            self._database,
            StreamOptions(clock=self._clock, stop=stop, last_event_id=last_event_id),
        )

    def run_event_frames(
        self,
        run_id: str,
        last_event_id: str | None,
        *,
        stop: threading.Event,
    ) -> Iterator[bytes]:
        """单运行事件流的 SSE 帧迭代器；未知运行按 404 口径在开流前拒绝。

        Raises:
            ContractError: 运行不存在，错误码 E_NOT_FOUND。
        """
        with closing(open_database(self._database)) as connection:
            require_run(connection, self._clock, run_id)
        return stream_run_frames(
            self._database,
            run_id,
            StreamOptions(clock=self._clock, stop=stop, last_event_id=last_event_id),
        )

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
            capabilities=self._capabilities,
            body=body,
            data_root=self._database.parent,
            task_runner=self._task_runner,
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


__all__ = ["API_PREFIX", "EVENTS_CAPABILITY", "ROUTES", "ApiApp", "Route"]
