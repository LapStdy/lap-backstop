"""tick 循环的心跳侧：心跳写入件与在途运行口径。

写一次心跳带哪些字段、哪些运行算在途在这里定死；多久写一次的节律随循环
本体在 serve 模块（docs/08 心跳与停机两节）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from contracts import RunState
from core.hostops import heartbeat
from core.jobs.run_service import JobRunService
from storage.repositories.job_runs import JobRunRecord

_ACTIVE_RUN_STATES = frozenset(
    {RunState.PREPARING, RunState.WAITING, RunState.EXECUTING, RunState.VERIFYING}
)


@dataclass(frozen=True, slots=True)
class HeartbeatWriter:
    """心跳写入件：进程身份与目标路径固定，每次写取当前时刻与在途运行。"""

    path: Path
    started_at: datetime
    pid: int
    version: str

    def write(self, *, last_tick_at: datetime, active_run: str | None) -> None:
        """原子写一次心跳。

        Raises:
            OSError: 目录不可写或替换失败；由调用方决定重试。
        """
        heartbeat.write_heartbeat(
            self.path,
            {
                "pid": self.pid,
                "started_at": self.started_at.isoformat(),
                "version": self.version,
                "last_tick": last_tick_at.isoformat(),
                "active_run": active_run,
            },
        )


def _active_runs(runs: JobRunService) -> list[JobRunRecord]:
    """在途运行清单：准备、等待、执行、验证都算。"""
    return [run for run in runs.non_terminal() if run.state in _ACTIVE_RUN_STATES]


def _active_run(runs: JobRunService) -> str | None:
    """当前在途运行的 id；没有则 None。"""
    active = _active_runs(runs)
    return active[0].id if active else None
