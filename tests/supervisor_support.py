"""supervisor 场景测试的同步助手：派发后等执行线程收尾再断言。"""

from __future__ import annotations

from core.supervisor import Supervisor

JOIN_TIMEOUT_SECONDS = 10.0


def tick_and_settle(supervisor: Supervisor) -> None:
    """推进一轮并等全部执行线程收尾：断言读到的状态含线程的落库结果。

    tick 只派发不等待执行；fake 插件毫秒级收尾，等待上限只兜底线程异常挂起的情形。
    """
    supervisor.tick()
    supervisor.join_executors(JOIN_TIMEOUT_SECONDS)
