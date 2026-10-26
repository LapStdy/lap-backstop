"""处理器上下文：端点函数可见的请求形态与处理器签名。

独立成模块是为了让端点模块只依赖类型而不回指应用，避免路由表与处理器互相导入。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from contracts import Clock, ContractError, ErrorCode
from core.api.wire import ApiResponse
from core.environments.actions import EnvironmentActions
from core.hostops.tasks import Runner


@dataclass(frozen=True, slots=True)
class RequestCtx:
    """处理器可见的请求上下文；body 是已解析的 JSON 值，无请求体时为 None。"""

    connection: sqlite3.Connection
    clock: Clock
    params: Mapping[str, str]
    query: Mapping[str, str]
    headers: Mapping[str, str]
    capabilities: tuple[str, ...]
    body: object
    # 数据根目录，按数据库文件位置派生；供投递等需要宿主路径的端点使用。
    data_root: Path
    # 计划任务注册执行件；生产默认 subprocess.run，测试注入假 runner。
    task_runner: Runner
    # 内核环境动作；进程内测试与 serve 装配都注入，缺省时动作端点按未接入应答。
    environment_actions: EnvironmentActions | None = None


Handler = Callable[[RequestCtx], ApiResponse]


def body_object(ctx: RequestCtx) -> dict[str, Any]:
    """取对象形态的请求体；空体或非对象按 E_BAD_REQUEST 拒绝。"""
    if not isinstance(ctx.body, dict):
        raise ContractError(ErrorCode.BAD_REQUEST, "请求体必须是 JSON 对象")
    return ctx.body


__all__ = ["Handler", "RequestCtx", "body_object"]
