"""投递重试：有限次数加退避，结果交回内核线程落事件。

重试循环是纯函数（等待与停机都注入），线程外壳只做队列进出；
这里不发生任何数据库写入，维持内核单写连接纪律（docs/11）。
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from contracts import Notification
from contracts.types import DeliveryResult

_QUEUE_POLL_SECONDS: Final[float] = 0.5

Wait = Callable[[float], None]
Send = Callable[[Notification], DeliveryResult]


def _stop_wait(stop: threading.Event) -> Wait:
    """默认等待：用停机事件等待，close 时可提前打断退避。"""

    def wait(seconds: float) -> None:
        stop.wait(seconds)

    return wait


@dataclass(frozen=True, slots=True)
class PendingDelivery:
    """一条待投递通知；run_id 为 None 表示无运行归属（看门狗、环境）。

    notification_id 是账目行的主键：入账与终态更新按它对上同一行。
    """

    notification_id: str
    notification: Notification
    run_id: str | None
    channel: str


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    """一次投递任务的终态：成功，或重试耗尽/停机放弃。"""

    notification_id: str
    run_id: str | None
    channel: str
    ok: bool
    error: str
    attempts: int


def _error_of(result: DeliveryResult) -> str:
    if result.error_code and result.detail:
        return f"{result.error_code}：{result.detail}"
    return result.error_code or result.detail or "投递失败"


@dataclass(frozen=True, slots=True)
class ChannelSnapshot:
    """提交时刻的渠道视图；渠道对象与重试参数一起取，避免新旧混用。"""

    send: Send | None
    attempts: int
    backoff_seconds: tuple[float, ...]


SnapshotProvider = Callable[[], ChannelSnapshot]


def deliver_one(
    item: PendingDelivery,
    *,
    snapshot: ChannelSnapshot,
    wait: Wait,
    stop: threading.Event,
) -> DeliveryOutcome:
    """按快照里的渠道与策略投递一条通知：失败退避后重试，耗尽或停机即止。

    Args:
        item: 待投递通知与归属。
        snapshot: 提交时刻的渠道视图；send 为 None 视为渠道不可用。
        wait: 等待函数；实现须可被 stop 提前打断。
        stop: 停机事件；置位后不再尝试与等待。

    Returns:
        终态结果；attempts 字段是实际尝试的次数，停机打断时小于配置值。
    """
    notification_id = item.notification_id
    send = snapshot.send
    if send is None:
        return DeliveryOutcome(notification_id, item.run_id, item.channel, False, "渠道不可用", 0)
    last_error = "停机放弃投递"
    tried = 0
    for attempt in range(1, snapshot.attempts + 1):
        if stop.is_set():
            break
        tried = attempt
        try:
            result = send(item.notification)
        except Exception as error:  # 渠道实现在外部，抛异常属于实现缺陷，不能让线程倒下
            last_error = f"渠道异常（{type(error).__name__}）：{error}"
        else:
            if result.ok:
                return DeliveryOutcome(
                    notification_id, item.run_id, item.channel, True, "", attempt
                )
            last_error = _error_of(result)
        if attempt == snapshot.attempts or stop.is_set():
            break
        wait(snapshot.backoff_seconds[min(attempt - 1, len(snapshot.backoff_seconds) - 1)])
    return DeliveryOutcome(notification_id, item.run_id, item.channel, False, last_error, tried)


class DeliveryWorker:
    """单工作线程：逐条投递队列里的通知，结果排队交回内核线程。"""

    def __init__(
        self,
        snapshot_provider: SnapshotProvider,
        *,
        stop: threading.Event,
        wait: Wait | None = None,
    ) -> None:
        """创建工作器；线程由 start 拉起。

        Args:
            snapshot_provider: 每次投递前取当前渠道；send=None 时该条落成
                「渠道不可用」的失败结果（通知已关闭或配置非法）。
            stop: 停机事件，由 close 置位。
            wait: 退避等待的注入点；默认用停机事件等待（可提前打断）。
        """
        self._provider = snapshot_provider
        self._stop = stop
        self._wait: Wait = wait if wait is not None else _stop_wait(stop)
        self._queue: queue.Queue[PendingDelivery] = queue.Queue()
        self._outcomes: queue.Queue[DeliveryOutcome] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._current: PendingDelivery | None = None

    def start(self) -> None:
        """拉起工作线程；重复调用无害（线程已在时不重建）。"""
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="notify-delivery", daemon=True)
        self._thread.start()

    def submit(self, item: PendingDelivery) -> bool:
        """入队一条通知；停机后拒绝，由调用方把已入账的项落成终态。

        Returns:
            True 表示已入队；False 表示工作线程已停机，条目必须由调用方收尾。
        """
        with self._lock:
            if self._stop.is_set():
                return False
            self._queue.put(item)
            return True

    def drain_outcomes(self) -> list[DeliveryOutcome]:
        """取走已完成的结果；由内核线程在轮询里调用。"""
        outcomes: list[DeliveryOutcome] = []
        while True:
            try:
                outcomes.append(self._outcomes.get_nowait())
            except queue.Empty:
                return outcomes

    def close(self, *, join_timeout: float) -> tuple[PendingDelivery, ...]:
        """停机并返回未投递的条目；在途 HTTP 调用最多等一个超时周期。

        队列里没取走的项与等待上限到期仍在投递的那条都要交回调用方，
        否则它们已入账的 pending 行会永远停在中间态。

        Returns:
            未产生投递结果的条目；等待上限内正常收尾时为空。
        """
        with self._lock:
            self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=join_timeout)
        abandoned = self._take_queued()
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._current is not None:
                abandoned.append(self._current)
        return tuple(abandoned)

    def _take_queued(self) -> list[PendingDelivery]:
        items: list[PendingDelivery] = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except queue.Empty:
                return items

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._queue.get(timeout=_QUEUE_POLL_SECONDS)
            except queue.Empty:
                continue
            with self._lock:
                self._current = item
            try:
                try:
                    outcome = deliver_one(
                        item, snapshot=self._provider(), wait=self._wait, stop=self._stop
                    )
                except Exception as error:  # 兜底：任何意外都要落终态，线程继续服务后面的通知
                    outcome = DeliveryOutcome(
                        item.notification_id,
                        item.run_id,
                        item.channel,
                        False,
                        f"投递异常（{type(error).__name__}）：{error}",
                        0,
                    )
                self._outcomes.put(outcome)
            finally:
                with self._lock:
                    self._current = None


__all__ = [
    "ChannelSnapshot",
    "DeliveryOutcome",
    "DeliveryWorker",
    "PendingDelivery",
    "deliver_one",
]
