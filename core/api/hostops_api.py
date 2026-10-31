"""宿主端点：重新注册计划任务与请求排水停机（docs/03 端点一览）。

注册动作与 serve 启动路径共用同一实现与汇总口径；结果写成全局事件
``host.event``（event=host.tasks_registered），逐任务结果结构化返回。
"""

from __future__ import annotations

from collections.abc import Callable

from contracts import ContractError, ErrorCode, EventType
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse
from core.hostops import tasks
from storage.repositories.events import EventLog

# 排水停机的注入面：serve 装配传入「置位停机事件并返回在跑运行数」的动作；
# 未接入时端点按 409 应答，与运行叫停同一口径。
DrainRequester = Callable[[], int]


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


def post_host_drain(
    ctx: RequestCtx, *, drain_requester: DrainRequester | None = None
) -> ApiResponse:
    """请求排水停机：置位停机事件，调度停止接新作业，在跑作业到阶段边界收尾。

    与 SIGINT 走同一停机路径（docs/08 升级与回滚）；停机是异步的，应答只代表
    请求被接受，进程随后按既有收尾退出。内核实现由 serve 注入；未接入时 409，
    避免把「什么都没做」伪装成已停机。

    Raises:
        ContractError: 排水未接入（409）。
    """
    if drain_requester is None:
        raise ContractError(ErrorCode.CONFLICT, "排水停机未接入内核")
    return ApiResponse(
        status=200,
        payload={"status": "draining", "active_runs": drain_requester()},
    )


__all__ = ["DrainRequester", "post_host_drain", "post_tasks_register"]
