"""插件宿主错误：插件未注册、缺少能力、worker 进程故障。"""

from __future__ import annotations

from contracts.errors import ErrorCode


class PluginError(ValueError):
    """插件领域的可预期错误。"""

    code: str = "E_PLUGIN"


class PluginContractError(PluginError):
    """插件加载的契约校验失败：结构缺失用 E_PLUGIN_MANIFEST，版本不覆盖用 E_PLUGIN_VERSION。

    错误码随消息走（``E_PLUGIN_*: 原因``）；worker 以 FAULT 上报后，内核日志与
    plugin.rejected 事件能按码区分两类失败。
    """

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(f"{code.value}: {message}")
        self.code = code.value


class PluginNotFoundError(PluginError):
    """插件没有注册。"""

    code = "E_NOT_FOUND"


class PluginCapabilityError(PluginError):
    """插件未声明该能力，或能力对象缺失。"""

    code = "E_PLUGIN"


class WorkerError(PluginError):
    """worker 子进程的故障：载荷非法、超时、崩溃、取消或已降级。"""

    code: str = "E_PLUGIN"


class WorkerPayloadError(WorkerError):
    """内核与 worker 之间的 JSON 载荷不符合契约。"""

    code = "E_PLUGIN"


class WorkerTimeoutError(WorkerError):
    """能力调用超过调用超时，worker 已被强杀。"""

    code = "E_DEADLINE"


class WorkerCallError(WorkerError):
    """插件能力方法抛出异常，worker 把类型与消息回传给内核。"""

    code = "E_PLUGIN"


class WorkerCrashError(WorkerError):
    """worker 进程在调用中退出或被信号终止。"""

    code = "E_PLUGIN_CRASH"


class WorkerCancelledError(WorkerError):
    """等待期间收到取消，worker 已按宽限期处置。"""

    code = "E_CONFLICT"


class PluginDegradedError(WorkerError):
    """连续多次重启失败，插件停止接收调用。"""

    code = "E_PLUGIN"
