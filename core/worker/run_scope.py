"""运行范围与取消标记：一次执行共享的状态，与内核持有的取消信号。

阶段方法只拿这一份范围，不再传宽参数表；取消标记在阶段边界检查（ADR 0007）。
"""

from __future__ import annotations

from dataclasses import dataclass

from contracts import RunContext, ScriptAdapter, ScriptHandle, Verdict, Verifier
from core.environments.service import EnvironmentCall
from core.events.verdicts import VerdictError
from storage.repositories.job_runs import JobRunRecord
from storage.repositories.job_templates import JobTemplateRow


class CancelFlag:
    """内核持有的取消标记；worker 在阶段边界检查。"""

    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        """置位取消标记。"""
        self._cancelled = True

    def is_cancelled(self) -> bool:
        """是否已请求取消。"""
        return self._cancelled


@dataclass(slots=True)
class RunScope:
    """一次执行共享的状态：阶段方法只拿这一份，不再传宽参数表。"""

    run: JobRunRecord
    template: JobTemplateRow
    call: EnvironmentCall
    script: ScriptAdapter
    verifier: Verifier
    context: RunContext
    handle: ScriptHandle | None = None
    verdict: Verdict | None = None
    verify_failure: VerdictError | None = None
    has_called_cleanup: bool = False  # 本尝试是否已调用 provider 清理（docs/09）

    @property
    def run_id(self) -> str:
        """运行 id 的短别名；日志与事件用。"""
        return self.run.id
