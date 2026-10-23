"""作业领域错误：携带错误码，供 API 与恢复策略映射。"""

from __future__ import annotations


class JobRunError(ValueError):
    """作业领域的可预期错误。"""

    code: str = "E_INTERNAL"


class RunNotFoundError(JobRunError):
    """指定的 JobRun 不存在。"""

    code = "E_NOT_FOUND"


class IllegalRunTransitionError(JobRunError):
    """状态机拒绝这次迁移，对应错误码 E_CONFLICT。"""

    code = "E_CONFLICT"
