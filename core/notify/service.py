"""通知服务：尾随核心事件流，把关键事件交给 NotificationFactory 拼通知并投递。

poll() 由 serve 每轮调用：刷新渠道配置、追读新事件、排队投递、
把投递结果落成 notify.sent / notify.failed（运行内）或 core.event（无运行归属）。
只读既有事件流，不改事件语义；投递结果用契约里现成的事件类型。
推送口径（docs/06、docs/07）：运行结束、停手、环境 failed 各推一次；
同一运行只推一条（恢复停手与运行结束合并，先到先推）。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from contracts import (
    Clock,
    ContractError,
    EnvironmentState,
    ErrorCode,
    EventType,
    Logger,
    Notification,
    Notifier,
    RunContext,
)
from contracts.types import DeliveryResult
from core.api.store import all_settings
from core.hostops import tailscale
from core.hostops.watchdog import WATCHDOG_GAVE_UP_EVENT
from core.logging import NullLogger
from core.notify.channels import build_channel
from core.notify.config import NotifyConfig, resolve_config
from core.notify.delivery import (
    ChannelSnapshot,
    DeliveryOutcome,
    DeliveryWorker,
    PendingDelivery,
)
from core.notify.notifications import (
    NotificationFactory,
    no_emit,
    reject_evidence,
    test_notification,
)
from core.worker.executor import CancelFlag
from storage.repositories.events import EventLog

_EVENTS_PER_POLL: Final[int] = 200
_NOTIFIED_RUNS_LIMIT: Final[int] = 1000


def _payload_of(raw: object) -> Mapping[str, Any]:
    """事件载荷 JSON 反序列化；坏行按空载荷跳过，不让单条坏数据卡住尾随。"""
    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class NotifyService:
    """事件尾随与投递编排；拥有投递工作线程，poll 与 close 由 serve 驱动。"""

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock,
        *,
        data_root: Path,
        log: Logger | None = None,
        settings_reader: Callable[[], Mapping[str, Any]] | None = None,
        channel_factory: Callable[[NotifyConfig], Notifier] | None = None,
        wait: Callable[[float], None] | None = None,
    ) -> None:
        """创建服务并拉起投递线程；事件游标从当前末尾起算，不重推历史。

        构造参数超过上限 4 属评审已说明的例外：clock/data_root/log 是内核
        注入的三件套，settings_reader/channel_factory/wait 是三条测试缝，
        全部按关键字调用，语义互不重叠。

        Args:
            connection: 内核单写连接；事件尾随与结果落库共用。
            clock: 事件落库的时间来源。
            data_root: 数据根目录，用于投递上下文路径。
            log: 结构化日志；不落渠道密钥。
            settings_reader: settings 快照读取器；默认整表读取。
            channel_factory: 渠道构造的注入点；测试用它替换真实 HTTP 渠道。
            wait: 退避等待的注入点；默认用停机事件等待（可提前打断）。
        """
        self._connection = connection
        self._clock = clock
        self._data_root = data_root
        self._log: Logger = NullLogger() if log is None else log
        self._settings_reader = (
            settings_reader if settings_reader is not None else (lambda: all_settings(connection))
        )
        self._build_channel: Callable[[NotifyConfig], Notifier] = (
            channel_factory if channel_factory is not None else build_channel
        )
        self._events = EventLog(connection)
        self._notifications = NotificationFactory(connection)
        self._stop = threading.Event()
        self._config: NotifyConfig | None = None
        self._channel: Notifier | None = None
        self._invalid_reported = False
        self._notified_runs: set[str] = set()
        self._worker = DeliveryWorker(self._snapshot, stop=self._stop, wait=wait)
        self._global_seq = _max_global_seq(connection)
        self._run_rowid = _max_run_rowid(connection)
        self._worker.start()

    def poll(self) -> None:
        """一轮通知推进：刷新配置、追新事件、落投递结果。数据库故障向上抛。"""
        self._refresh_config()
        if self._config is not None and self._config.enabled:
            self._tail_events()
        self._record_outcomes()

    def send_test(self) -> DeliveryResult:
        """同步发一条测试通知（单次尝试、不重试、不落事件），供连通性自检。

        Returns:
            渠道应答；配置未启用或非法时返回失败结果。
        """
        try:
            config = resolve_config(self._settings_reader())
        except ContractError as error:
            return DeliveryResult(
                ok=False, error_code=ErrorCode.CONFIG_INVALID.value, detail=error.message
            )
        if not config.enabled:
            return DeliveryResult(
                ok=False,
                error_code=ErrorCode.CONFIG_INVALID.value,
                detail="通知未启用：先打开 notify.enabled",
            )
        channel = self._build_channel(config)
        return channel.send(self._delivery_context(), test_notification())

    def close(self, *, join_timeout: float = 15.0) -> None:
        """停机：等工作线程收尾；在途 HTTP 调用最多等一个超时周期。"""
        self._worker.close(join_timeout=join_timeout)

    def _snapshot(self) -> ChannelSnapshot:
        """工作线程取当前渠道；配置关闭或非法时 send 为 None，队列中的项被放弃。"""
        config = self._config
        channel = self._channel
        if config is None or not config.enabled or channel is None:
            return ChannelSnapshot(send=None, attempts=1, backoff_seconds=())
        return ChannelSnapshot(
            send=self._bind_send(channel),
            attempts=config.attempts,
            backoff_seconds=config.backoff_seconds,
        )

    def _bind_send(self, channel: Notifier) -> Callable[[Notification], DeliveryResult]:
        """把协议的双参 send 绑成投递循环用的单参形式；上下文只有内核投递身份。"""
        ctx = self._delivery_context()

        def send(message: Notification) -> DeliveryResult:
            return channel.send(ctx, message)

        return send

    def _delivery_context(self) -> RunContext:
        """内核投递的最小上下文：无运行归属，不接受证据，只满足协议签名。"""
        return RunContext(
            run_id="notify:kernel",
            attempt=0,
            template_id="",
            plugin_id="core.notify",
            environment_id="",
            config={},
            work_dir=self._data_root / "work" / "notify-kernel",
            evidence_dir=self._data_root / "evidence" / "notify-kernel",
            deadline_at=self._clock.utc_now(),
            cancel=CancelFlag(),
            clock=self._clock,
            secrets={},
            log=self._log,
            emit=no_emit,
            record_evidence=reject_evidence,
        )

    def _refresh_config(self) -> None:
        """读 settings；渠道相关键变化才重建渠道，非法配置只报一次。"""
        try:
            config = resolve_config(self._settings_reader())
        except ContractError as error:
            self._config = None
            self._channel = None
            if not self._invalid_reported:
                self._invalid_reported = True
                self._log.error({"event": "notify.config_invalid", "error": error.message})
                self._emit_core_event("notify.config_invalid", {"error": error.message})
            return
        self._invalid_reported = False
        if self._config is not None and config.signature == self._config.signature:
            return
        self._config = config
        self._channel = self._build_channel(config) if config.enabled else None
        self._log.info({"event": "notify.config_loaded", "channel": config.channel or "off"})

    def _tail_events(self) -> None:
        """追读全局与运行两张事件表的新增行；游标只推进已读到的位置。"""
        for row in self._connection.execute(
            "SELECT seq, type, payload FROM events_global WHERE seq > ? ORDER BY seq LIMIT ?",
            (self._global_seq, _EVENTS_PER_POLL),
        ):
            self._global_seq = int(row["seq"])
            self._notify_global(str(row["type"]), _payload_of(row["payload"]))
        for row in self._connection.execute(
            "SELECT rowid AS marker, run_id, type, payload FROM run_events "
            "WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (self._run_rowid, _EVENTS_PER_POLL),
        ):
            self._run_rowid = int(row["marker"])
            self._notify_run(str(row["run_id"]), str(row["type"]), _payload_of(row["payload"]))

    def _notify_global(self, event_type: str, payload: Mapping[str, Any]) -> None:
        """全局事件映射：环境 failed、看门狗停手与 Tailscale 未运行；都无运行归属。"""
        if event_type == EventType.ENV_STATE_CHANGED.value and (
            payload.get("to_state") == EnvironmentState.FAILED.value
        ):
            self._submit(self._notifications.environment(payload), run_id=None)
        elif event_type == EventType.CORE_EVENT.value and (
            payload.get("event") == WATCHDOG_GAVE_UP_EVENT
        ):
            self._submit(self._notifications.watchdog(payload), run_id=None)
        elif event_type == EventType.HOST_EVENT.value and (
            payload.get("event") == tailscale.TAILSCALE_DOWN_EVENT
        ):
            self._submit(self._notifications.tailscale(payload), run_id=None)

    def _notify_run(self, run_id: str, event_type: str, payload: Mapping[str, Any]) -> None:
        """运行事件映射：运行结束与恢复停手；同一运行只推先到的一条。"""
        if run_id in self._notified_runs:
            return
        if event_type == EventType.RUN_FINISHED.value:
            notification = self._notifications.run_end(run_id, payload)
        elif event_type == EventType.RECOVERY_FINISHED.value:
            notification = self._notifications.recovery(run_id, payload)
        else:
            return
        if notification is not None:
            self._submit(notification, run_id=run_id)

    def _submit(self, notification: Notification, *, run_id: str | None) -> None:
        """排队投递；渠道未就绪（关闭、非法配置）时静默丢弃。"""
        config = self._config
        if config is None or not config.enabled or self._channel is None:
            return
        if run_id is not None:
            if len(self._notified_runs) >= _NOTIFIED_RUNS_LIMIT:
                self._notified_runs.clear()
            self._notified_runs.add(run_id)
        self._worker.submit(
            PendingDelivery(notification=notification, run_id=run_id, channel=config.channel)
        )

    def _record_outcomes(self) -> None:
        """把投递结果落库；单条失败只告警，不影响其余结果。"""
        now = self._clock.utc_now()
        for outcome in self._worker.drain_outcomes():
            try:
                self._record_outcome(outcome, at=now)
            except (ValueError, sqlite3.Error) as error:
                self._log.warn({"event": "notify.outcome_record_failed", "error": str(error)})

    def _record_outcome(self, outcome: DeliveryOutcome, *, at: datetime) -> None:
        """投递成功记 notify.sent，耗尽记 notify.failed；无运行归属走 core.event。"""
        if outcome.run_id is not None:
            payload: dict[str, object] = (
                {"run_id": outcome.run_id, "channel": outcome.channel, "result": "ok"}
                if outcome.ok
                else {
                    "run_id": outcome.run_id,
                    "channel": outcome.channel,
                    "error": outcome.error,
                    "attempts": outcome.attempts,
                }
            )
            self._events.append_run(
                outcome.run_id,
                EventType.NOTIFY_SENT if outcome.ok else EventType.NOTIFY_FAILED,
                payload,
                at=at,
            )
            return
        if outcome.ok:
            self._emit_core_event("notify.sent", {"channel": outcome.channel, "result": "ok"})
        else:
            self._emit_core_event(
                "notify.failed",
                {"channel": outcome.channel, "error": outcome.error, "attempts": outcome.attempts},
            )

    def _emit_core_event(self, event: str, fields: dict[str, object]) -> None:
        """无运行归属的通知簿记走 core.event，payload 形态与 supervisor 一致。"""
        self._events.append_global(
            EventType.CORE_EVENT,
            {"event": event, "payload": dict(fields)},
            at=self._clock.utc_now(),
        )


def _max_global_seq(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COALESCE(MAX(seq), 0) FROM events_global").fetchone()
    return int(row[0])


def _max_run_rowid(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COALESCE(MAX(rowid), 0) FROM run_events").fetchone()
    return int(row[0])


__all__ = ["NotifyService"]
