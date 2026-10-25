"""独立 HTTP 服务：标准库实现，承载 ApiApp 并按请求线程分发。

本层只做传输：字节到 ApiRequest、ApiResponse 到字节；鉴权与路由在 ApiApp。
传入的 connection 只用于启动引导（取库文件路径、写 server_id），请求处理在
各自线程打开独立连接（WAL 支持多连接，busy_timeout 吸收写竞争）。
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from contracts import Clock, ContractError, ErrorCode, Logger
from core.api.app import ApiApp
from core.api.store import ensure_server_id
from core.api.wire import ApiRequest, ApiResponse, encode_json, error_response
from core.clock import SystemClock
from core.logging import NullLogger

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787
# 请求体上限：本地管理面足够宽裕，同时挡住失控客户端把内存吃满。
MAX_BODY_BYTES = 1_000_000
# 拒收时排空正文的量级：覆盖常见失控客户端，再大按断连处理。
MAX_DRAIN_BYTES = 4 * MAX_BODY_BYTES
# 空闲连接读超时：挂住的连接不长期占线程（docs/11 并发与超时）。
REQUEST_TIMEOUT_SECONDS = 30.0
# 服务循环的唤醒间隔：停机事件最多延迟这么久被看到。
POLL_INTERVAL_SECONDS = 0.25


@dataclass(frozen=True, slots=True)
class ApiOptions:
    """API 服务的注入项：时钟、日志、停机事件与循环旋钮；生产路径全用默认值。"""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    clock: Clock = field(default_factory=SystemClock)
    log: Logger = field(default_factory=NullLogger)
    stop: threading.Event = field(default_factory=threading.Event)
    max_requests: int | None = None


class _RequestCounter:
    """已处理请求数；HTTP 线程与停机判定线程各摸一次，需要加锁。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._served = 0

    def bump(self) -> int:
        """记一次已处理请求，返回累计数。"""
        with self._lock:
            self._served += 1
            return self._served

    @property
    def served(self) -> int:
        """当前累计的已处理请求数。"""
        with self._lock:
            return self._served


class ApiServer:
    """已启动的服务句柄：实际监听地址与停机入口。"""

    def __init__(self, http: ThreadingHTTPServer, thread: threading.Thread, options: ApiOptions):
        self._http = http
        self._thread = thread
        self._options = options

    @property
    def address(self) -> tuple[str, int]:
        """实际监听地址与端口；port=0 时取内核分配的端口。"""
        host, port = self._http.server_address[:2]
        return str(host), int(port)

    def shutdown(self) -> None:
        """停止接收新请求并等服务线程退出；可重复调用。"""
        self._options.stop.set()
        self._thread.join(timeout=10.0)


def start_api_server(
    connection: sqlite3.Connection,
    options: ApiOptions | None = None,
) -> ApiServer:
    """从既有连接引导并启动独立 API 服务，返回可停机的服务句柄。

    Raises:
        ValueError: 传入的是内存或临时数据库；API 需要能跨线程打开的文件库。
    """
    active = ApiOptions() if options is None else options
    database = _database_path(connection)
    ensure_server_id(connection, at=active.clock.utc_now())
    app = ApiApp(database, clock=active.clock, log=active.log)
    counter = _RequestCounter()
    http = ThreadingHTTPServer((active.host, active.port), _handler(app, counter))
    http.timeout = POLL_INTERVAL_SECONDS
    thread = threading.Thread(
        target=_serve,
        args=(http, counter, active),
        daemon=True,
        name="api-server",
    )
    thread.start()
    return ApiServer(http, thread, active)


def _serve(http: ThreadingHTTPServer, counter: _RequestCounter, options: ApiOptions) -> None:
    """服务循环：停机事件或请求上限任一满足即退出并关监听。"""
    while not options.stop.is_set():
        try:
            http.handle_request()
        except OSError as error:
            # 监听套接字已坏：与其让线程带着异常静默死亡，留下日志再退出。
            options.log.error({"event": "api.accept_failed", "error": str(error)})
            break
        if options.max_requests is not None and counter.served >= options.max_requests:
            options.stop.set()
    http.server_close()


class _HandlerMethods(BaseHTTPRequestHandler):
    """请求处理方法；具体会话的 app 与计数器由 _handler 组装的子类携带。"""

    # HTTP/1.0 逐请求关连接；挂载 SSE 时再升级常驻连接。
    timeout = REQUEST_TIMEOUT_SECONDS
    app: ApiApp
    counter: _RequestCounter

    def do_GET(self) -> None:
        self._respond()

    def do_POST(self) -> None:
        self._respond()

    def do_PUT(self) -> None:
        self._respond()

    def do_DELETE(self) -> None:
        self._respond()

    def _respond(self) -> None:
        try:
            response = self.app.handle(self._read_request())
            self._write(response)
        except ContractError as error:
            self._write(error_response(error.code, error.message))
        except (BrokenPipeError, ConnectionResetError):
            return
        finally:
            self.counter.bump()
            self.close_connection = True

    def _read_request(self) -> ApiRequest:
        split = urlsplit(self.path)
        query = {key: values[-1] for key, values in parse_qs(split.query).items()}
        headers = {key.lower(): value for key, value in self.headers.items()}
        length = self._body_length(headers)
        if length > MAX_BODY_BYTES:
            # 先读走正文再拒绝：不读就关连接会触发 TCP RST，客户端收不到 400。
            self._drain_body(length)
            raise ContractError(
                ErrorCode.BAD_REQUEST,
                f"请求体超过 {MAX_BODY_BYTES} 字节上限",
            )
        body = self.rfile.read(length) if length > 0 else b""
        return ApiRequest(self.command, split.path, query, headers, body)

    def _body_length(self, headers: Mapping[str, str]) -> int:
        """解析 Content-Length；畸形值按坏请求拒绝而不是让线程崩掉。"""
        raw = headers.get("content-length", "0")
        try:
            return int(raw)
        except ValueError as error:
            raise ContractError(
                ErrorCode.BAD_REQUEST,
                f"Content-Length 不是整数：{raw}",
            ) from error

    def _drain_body(self, length: int) -> None:
        remaining = min(length, MAX_DRAIN_BYTES)
        while remaining > 0:
            chunk = self.rfile.read(min(remaining, 65536))
            if not chunk:
                break
            remaining -= len(chunk)

    def _write(self, response: ApiResponse) -> None:
        body = b"" if response.payload is None else encode_json(response.payload)
        self.send_response(response.status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if response.etag is not None:
            self.send_header("ETag", response.etag)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        # 请求日志由 ApiApp 写结构化行，不走 stderr 自由文本。
        return None


def _handler(app: ApiApp, counter: _RequestCounter) -> type[BaseHTTPRequestHandler]:
    """按会话绑定 app 与计数器的处理器类；每个服务实例独立，互不覆盖。"""
    return type("BoundHandler", (_HandlerMethods,), {"app": app, "counter": counter})


def _database_path(connection: sqlite3.Connection) -> Path:
    """取主库文件路径；内存库与临时库无法被请求线程重新打开，直接拒绝。"""
    row = connection.execute("PRAGMA database_list").fetchone()
    file_name = "" if row is None else str(row[2])
    if not file_name:
        raise ValueError("API 服务需要文件数据库，内存库无法跨线程共享")
    return Path(file_name)


__all__ = [
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "MAX_BODY_BYTES",
    "ApiOptions",
    "ApiServer",
    "start_api_server",
]
