"""serve 的 API 挂载段：token 生成、监听设置解析与端口绑定。

挂载发生在 serve 装配段的启动序末尾（docs/08）；坏配置与端口被占的退出码
由本段返回、装配段转成进程退出码。环境动作、运行叫停与停机排水一并经
ApiServices 注入；API 请求线程自开连接（docs/11 并发）。
"""

from __future__ import annotations

import errno
import sqlite3
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from contracts import Clock, EventType
from core.__main__ import EXIT_ERROR, EXIT_PORT_CONFLICT
from core.api.hostops_api import DrainRequester
from core.api.runs_api import RunStopper
from core.api.server import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    ApiOptions,
    ApiServer,
    start_api_server,
)
from core.api.store import all_settings, bearer_token, ensure_token
from core.environments.actions import EnvironmentActions
from core.hostops import tailscale
from core.hostops.serve_events import _record_events
from core.hostops.serve_loop import _active_runs
from core.hostops.serve_options import ServeOptions
from core.jobs.run_service import JobRunService
from storage import open_database

PORT_CONFLICT_EVENT = "core.port_conflict"
API_HOST_KEY = "api.host"
API_PORT_KEY = "api.port"


@dataclass(frozen=True, slots=True)
class ApiServices:
    """API 挂载的协作对象：环境动作、运行叫停与停机排水。"""

    environment_actions: EnvironmentActions
    run_stopper: RunStopper
    drain_requester: DrainRequester


def _drain_requester(stop: threading.Event, database: Path, clock: Clock) -> DrainRequester:
    """排水动作：置位停机事件并返回当时的在跑运行数。

    API 请求线程自开连接（docs/11 并发）：tick 线程的连接不跨线程使用。
    置位先于统计：在跑数是请求被接受那一刻的口径，之后的推进归停机收尾。
    """

    def request() -> int:
        stop.set()
        connection = open_database(database)
        try:
            return len(_active_runs(JobRunService(connection, clock)))
        finally:
            connection.close()

    return request


def _start_api(
    connection: sqlite3.Connection,
    options: ServeOptions,
    services: ApiServices,
) -> tuple[ApiServer | None, int | None]:
    """按 settings 挂载 API：无 token 先自生成，端口被占退 4，tailnet 绑定失败降级。"""
    _ensure_token_logged(connection, options)
    try:
        settings = all_settings(connection)
        host, port = _api_listen(settings)
        extra_hosts, down_detail = tailscale.remote_listen_plan(
            settings, options.tailscale_runner, options.log
        )
    except ValueError as error:
        options.log.error({"event": "serve.api_config_invalid", "error": str(error)})
        return None, EXIT_ERROR
    if down_detail is not None:
        _record_events(
            connection,
            [(tailscale.TAILSCALE_DOWN_EVENT, {"detail": down_detail})],
            at=options.clock.utc_now(),
            event_type=EventType.HOST_EVENT,
        )
        options.log.warn({"event": "serve.tailscale_down", "detail": down_detail})
    # api.host 直接配成 tailnet 地址时不再重复追加，避免同一地址绑两次。
    plan = tailscale.ListenPlan(host, port, tuple(extra for extra in extra_hosts if extra != host))
    server = _bind_api(connection, options, plan, services)
    if server is None:
        return None, EXIT_PORT_CONFLICT
    tailscale.record_bind_degradation(
        connection, failures=server.bind_failures, at=options.clock.utc_now()
    )
    bound_hosts = {bound_host for bound_host, _bound_port in server.addresses}
    options.log.info(
        {
            "event": "serve.api_listening",
            "host": plan.host,
            "port": plan.port,
            "extra_hosts": [extra for extra in plan.extra_hosts if extra in bound_hosts],
        }
    )
    if options.on_api_listening is not None:
        options.on_api_listening(*server.address)
    return server, None


def _ensure_token_logged(connection: sqlite3.Connection, options: ServeOptions) -> None:
    """无可用 token 时生成并落库；日志只记已生成，不记值（docs/11）。"""
    if bearer_token(connection) is not None:
        return
    ensure_token(connection, at=options.clock.utc_now())
    options.log.info({"event": "serve.token_generated", "detail": "已生成访问令牌"})


def _bind_api(
    connection: sqlite3.Connection,
    options: ServeOptions,
    plan: tailscale.ListenPlan,
    services: ApiServices,
) -> ApiServer | None:
    """启动 API 服务；主地址端口被占写 core.port_conflict 返回 None，其余 OSError 上抛。"""
    try:
        return start_api_server(
            connection,
            ApiOptions(
                host=plan.host,
                port=plan.port,
                extra_hosts=plan.extra_hosts,
                clock=options.clock,
                log=options.log,
                stop=options.stop,
                task_runner=options.task_runner,
                environment_actions=services.environment_actions,
                run_stopper=services.run_stopper,
                drain_requester=services.drain_requester,
            ),
        )
    except OSError as error:
        # docs/08 的退出码 4 只针对端口被占；地址不可达等其它绑定失败走通用失败路径。
        if error.errno != errno.EADDRINUSE:
            raise
        _record_events(
            connection,
            [
                (
                    PORT_CONFLICT_EVENT,
                    {"detail": f"{plan.host}:{plan.port} 已被其它进程监听：{error}"},
                )
            ],
            at=options.clock.utc_now(),
            event_type=EventType.CORE_EVENT,
        )
        options.log.error(
            {
                "event": "serve.port_conflict",
                "host": plan.host,
                "port": plan.port,
                "error": str(error),
            }
        )
        return None


def _api_listen(settings: Mapping[str, Any]) -> tuple[str, int]:
    """读监听地址设置；缺项用默认值，取值非法按坏配置拒绝启动并指出键名。

    Raises:
        ValueError: api.host 或 api.port 的类型或取值范围不合法。
    """
    host = settings.get(API_HOST_KEY, DEFAULT_HOST)
    port = settings.get(API_PORT_KEY, DEFAULT_PORT)
    if not isinstance(host, str) or not host:
        raise ValueError(f"{API_HOST_KEY} 必须是非空字符串")
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65535:
        raise ValueError(f"{API_PORT_KEY} 必须是 0 到 65535 的整数")
    return host, port
