"""环境领域错误：与错误码表对应，供 API 与恢复策略映射。"""

from __future__ import annotations


class EnvironmentError(ValueError):
    """环境领域的可预期错误。"""

    code: str = "E_INTERNAL"


class EnvironmentNotFoundError(EnvironmentError):
    """环境未注册。"""

    code = "E_NOT_FOUND"


class EnvironmentStateError(EnvironmentError):
    """环境状态机拒绝这次迁移。"""

    code = "E_CONFLICT"
