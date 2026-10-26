"""宿主端点：重新注册计划任务（docs/03 POST /host/tasks/register）。

注册动作与 serve 启动路径共用同一实现与汇总口径；结果写成全局事件
``host.event``（event=host.tasks_registered），逐任务结果结构化返回。
"""

from __future__ import annotations

from contracts import EventType
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.hostops import tasks
from storage.repositories.events import EventLog


def post_tasks_register(ctx: RequestCtx) -> ApiResponse:
    """按定义文件重新注册 P0 计划任务清单，返回逐任务结果。

    注册失败（schtasks 非零退出、超时）不算端点失败：结果照常落事件，
    是否重试由调用方决定。事件 detail 与 serve 启动路径同一构造。

    Raises:
        sqlite3.Error: 定义文件写入或事件落库失败，由应用层按 500 应答。
    """
    now = ctx.clock.utc_now()
    registration = tasks.register_registered_tasks(
        ctx.data_root, runner=ctx.task_runner, registered_at=now
    )
    EventLog(ctx.connection).append_global(
        EventType.HOST_EVENT,
        {"event": tasks.TASKS_REGISTERED_EVENT, "detail": registration.detail},
        at=now,
    )
    return ApiResponse(
        status=200,
        payload={
            "ok": registration.ok,
            "detail": registration.detail,
            "tasks": [
                {"name": outcome.name, "ok": outcome.ok, "detail": outcome.detail}
                for outcome in registration.outcomes
            ],
        },
    )


__all__ = ["post_tasks_register"]
