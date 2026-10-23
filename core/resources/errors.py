"""资源领域错误：与错误码表对应。"""

from __future__ import annotations


class ResourceError(ValueError):
    """资源领域的可预期错误。"""

    code: str = "E_INTERNAL"


class ResourceNotFoundError(ResourceError):
    """资源未注册。"""

    code = "E_NOT_FOUND"


class ResourceBusyError(ResourceError):
    """资源已被占满，调用方应排队稍后重试。"""

    code = "E_BUSY"
