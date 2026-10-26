"""环境动作注入协议：HTTP 请求线程调用内核环境动作的窄接口。

API 层不直接摸 Supervisor 与插件注册表，只依赖本协议；serve 装配时把
Supervisor 包成实现注入。动作在请求线程同步执行，时长由 provider 自身超时决定。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class EnvironmentActionResult:
    """一次环境动作的结果：插件成功与失败都算端点的正常应答。"""

    ok: bool
    error_code: str | None = None
    detail: str = ""


class EnvironmentActions(Protocol):
    """环境动作的注入面：按环境 id 手动预热与复位。

    Raises:
        EnvironmentNotFoundError: 环境不存在。
        EnvironmentStateError: 状态机拒绝该动作（如 prepare 时环境 busy）。
        PluginError: provider 缺失或加载失败。
    """

    def prepare(self, environment_id: str) -> EnvironmentActionResult:
        """把环境带向 ready；busy 时拒绝。"""
        ...

    def reset(self, environment_id: str) -> EnvironmentActionResult:
        """复位环境：任何状态先回 booting，成功后等探针确认 ready。"""
        ...


__all__ = ["EnvironmentActionResult", "EnvironmentActions"]
