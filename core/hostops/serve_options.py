"""serve 的注入项与停机信号注册。

ServeOptions 收拢 serve 全链路的注入点：时钟、日志、停机事件、循环旋钮与
各外部动作的执行器；生产路径全用默认值，测试按需替换。停机注册只置停机
事件，事件置位后 tick 循环在当前轮结束处退出（docs/08 停机与阶段边界）。
"""

from __future__ import annotations

import signal
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType

from contracts import Clock, Logger
from core.clock import SystemClock
from core.hostops import tailscale, tasks, timesync
from core.hostops.doctor import RegistryGet, registry_get
from core.logging import NullLogger

TICK_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class ServeOptions:
    """serve 的注入项：时钟、日志、停机事件与循环旋钮；生产路径全用默认值。"""

    clock: Clock = field(default_factory=SystemClock)
    log: Logger = field(default_factory=NullLogger)
    stop: threading.Event = field(default_factory=threading.Event)
    max_ticks: int | None = None
    tick_interval_seconds: float = TICK_INTERVAL_SECONDS
    register_tasks: bool = True
    # 校时真的改宿主时钟：进程内测试关掉这个开关，避免 pytest 动系统时间（docs/08）。
    run_timesync: bool = True
    # 插件根缺省与 data 同级（docs/08 路径布局）；测试指向只含 fake 的临时目录。
    plugins_root: Path | None = None
    task_runner: tasks.Runner = field(default_factory=lambda: subprocess.run)
    # 校时命令的执行器（docs/08 时钟与 NTP）；测试替换为假件，不碰真实 w32tm。
    timesync_runner: timesync.Runner = field(default_factory=lambda: subprocess.run)
    registry_get: RegistryGet = field(default_factory=lambda: registry_get)
    tailscale_runner: tailscale.Runner = field(default_factory=lambda: tailscale.default_runner)
    # 磁盘可用字节数的注入点：用例固定一个稳定值，门禁便不看宿主机余量；
    # None 时由 Supervisor 自算数据根所在卷的真实值，生产行为不变。
    disk_free: Callable[[], int | None] | None = None
    on_api_listening: Callable[[str, int], None] | None = None


def install_stop_handlers() -> threading.Event:
    """注册停机信号并返回停机事件；事件置位后 tick 循环在当前轮结束处退出。

    Windows 的 TerminateProcess 没有处理器可走，优雅停机靠可捕获的控制台信号
    （SIGINT、SIGBREAK）；SIGTERM 一并注册，同一份代码在 POSIX 上也成立。

    Returns:
        停机事件；测试可直接注入自己的事件而不装信号处理器。
    """
    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    for name in ("SIGBREAK", "SIGINT", "SIGTERM"):
        number = getattr(signal, name, None)
        if number is not None:
            signal.signal(number, request_stop)
    return stop
