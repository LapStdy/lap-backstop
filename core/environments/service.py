"""环境服务：把插件探针与动作翻译成状态迁移与事件。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from contracts import (
    Clock,
    EnvironmentProbe,
    EnvironmentProvider,
    EnvironmentRef,
    EnvironmentState,
    EventType,
    PhaseResult,
    RunContext,
)
from core.environments.errors import EnvironmentNotFoundError, EnvironmentStateError
from core.environments.state_machine import require_transition
from storage.db.transaction import transaction
from storage.repositories.environments import EnvironmentRepository, EnvironmentRow
from storage.repositories.events import EventLog

# 跨连接写环境状态的输家证据：先提交者赢，输家丢弃本次写入（docs/11 并发与超时）。
ENV_ACTION_CONFLICT: Final[str] = "core.env_action_conflict"


@dataclass(frozen=True, slots=True)
class EnvironmentCall:
    """一次环境操作需要的插件、引用与运行上下文。"""

    environment_id: str
    provider: EnvironmentProvider
    ref: EnvironmentRef
    ctx: RunContext


class EnvironmentService:
    """环境生命周期：探针、准备、复位、清理、保温。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock) -> None:
        self._connection = connection
        self._clock = clock
        self._environments = EnvironmentRepository(connection)
        self._events = EventLog(connection)

    def register(self, environment_id: str, *, provider_id: str, name: str) -> EnvironmentRow:
        """注册环境；初始状态 unknown。"""
        return self._environments.ensure(
            environment_id,
            provider_id=provider_id,
            name=name,
        )

    def get(self, environment_id: str) -> EnvironmentRow:
        """取环境；不存在抛 ``EnvironmentNotFoundError``。"""
        row = self._environments.get(environment_id)
        if row is None:
            raise EnvironmentNotFoundError(f"环境不存在：{environment_id}")
        return row

    def list_all(self) -> tuple[EnvironmentRow, ...]:
        """全部环境，按 id 排序。"""
        return self._environments.list_all()

    def probe(self, call: EnvironmentCall) -> EnvironmentProbe | None:
        """调用插件探针并把结果落成状态迁移。

        探针调用失败或结果无法从当前状态迁入时按状态不明处置：环境置 unknown，
        等下一次探测再判定，不向调用方抛错；已在 unknown 时只刷新观测时间（docs/02）。
        探针期间前置状态被其它连接改写时，探针结果丢弃、以库内状态为准（先提交者赢），
        输家证据见 core.env_action_conflict。探针本身失败时返回 None。
        """
        row = self.get(call.environment_id)
        now = self._clock.utc_now()
        try:
            probe = call.provider.probe(call.ref, call.ctx)
        except Exception as error:
            self.mark_unknown(call.environment_id, detail=f"探针失败：{error}")
            return None
        at = probe.observed_at if probe.observed_at is not None else now
        try:
            self._apply(row, probe.state, at=at, detail={"detail": probe.detail})
        except EnvironmentStateError:
            current = self.get(call.environment_id)
            if current.state is row.state:
                self.mark_unknown(
                    call.environment_id,
                    detail=f"探针报告 {probe.state.value}，不允许从 {row.state.value} 迁入",
                )
        return probe

    def mark_unknown(self, environment_id: str, *, detail: str) -> EnvironmentRow:
        """状态不明（探针不可用等）：置 unknown，等下一次探测再判定。

        前置状态被其它连接先改写时丢弃本次处置（先提交者赢），输家证据见
        core.env_action_conflict，返回库内当前行。
        """
        row = self.get(environment_id)
        try:
            return self._apply(
                row,
                EnvironmentState.UNKNOWN,
                at=self._clock.utc_now(),
                detail={"detail": detail},
            )
        except EnvironmentStateError:
            return self.get(environment_id)

    def prepare(self, call: EnvironmentCall) -> PhaseResult:
        """准备环境：unknown/off/failed 先进入 booting，成功后 ready。

        Raises:
            EnvironmentStateError: 环境 busy、状态机拒绝，或插件调用期间前置
                状态被其它连接改写（先提交者赢，本次结果丢弃）。
        """
        row = self.get(call.environment_id)
        if row.state is EnvironmentState.READY:
            now = self._clock.utc_now()
            return PhaseResult(ok=True, detail="环境已就绪", started_at=now, ended_at=now)
        if row.state is EnvironmentState.BUSY:
            raise EnvironmentStateError("环境执行中，不能准备")
        if row.state is not EnvironmentState.BOOTING:
            row = self._apply(
                row,
                EnvironmentState.BOOTING,
                at=self._clock.utc_now(),
                detail={"action": "prepare"},
            )
        result = call.provider.prepare(call.ref, call.ctx)
        self._finish_action(row, result, success=EnvironmentState.READY, action="prepare")
        return result

    def reset(self, call: EnvironmentCall) -> PhaseResult:
        """复位环境：任何状态先进入 booting；复位成功后仍等探针确认 ready。

        Raises:
            EnvironmentStateError: 状态机拒绝，或插件调用期间前置状态被其它
                连接改写（先提交者赢，本次结果丢弃）。
        """
        row = self.get(call.environment_id)
        if row.state is not EnvironmentState.BOOTING:
            row = self._apply(
                row,
                EnvironmentState.BOOTING,
                at=self._clock.utc_now(),
                detail={"action": "reset"},
            )
        result = call.provider.reset(call.ref, call.ctx)
        if not result.ok:
            self._apply(
                row,
                EnvironmentState.FAILED,
                at=self._clock.utc_now(),
                detail={"action": "reset", "error_code": result.error_code or ""},
            )
        return result

    def cleanup(
        self,
        call: EnvironmentCall,
        *,
        keep_warm: bool,
        warm_minutes: int = 0,
    ) -> PhaseResult:
        """清理环境：按保温策略回到 ready 或 off。"""
        result = call.provider.cleanup(call.ref, call.ctx, keep_warm)
        now = self._clock.utc_now()
        if not result.ok:
            self._apply(
                self.get(call.environment_id),
                EnvironmentState.FAILED,
                at=now,
                detail={"action": "cleanup", "error_code": result.error_code or ""},
            )
            return result
        target = EnvironmentState.READY if keep_warm else EnvironmentState.OFF
        with transaction(self._connection):
            current = self.get(call.environment_id)
            if current.state is EnvironmentState.BUSY and target is EnvironmentState.OFF:
                self._apply(
                    current,
                    EnvironmentState.READY,
                    at=now,
                    detail={"action": "cleanup"},
                )
                current = self.get(call.environment_id)
            self._apply(current, target, at=now, detail={"action": "cleanup"})
            warm_until = now + timedelta(minutes=warm_minutes) if keep_warm else None
            self._environments.set_warm_until(call.environment_id, warm_until)
        return result

    def warm(self, call: EnvironmentCall, *, minutes: int) -> PhaseResult:
        """维持保温：只在已就绪的环境上生效，写 warm_until。"""
        row = self.get(call.environment_id)
        if row.state is not EnvironmentState.READY:
            raise EnvironmentStateError("保温要求环境已就绪")
        result = call.provider.warm(call.ref, call.ctx, minutes)
        if not result.ok:
            self._apply(
                self.get(call.environment_id),
                EnvironmentState.FAILED,
                at=self._clock.utc_now(),
                detail={"action": "warm", "error_code": result.error_code or ""},
            )
            return result
        self._environments.set_warm_until(
            call.environment_id,
            self._clock.utc_now() + timedelta(minutes=minutes),
        )
        return result

    def warm_expired(self, now: datetime | None = None) -> tuple[EnvironmentRow, ...]:
        """保温到期、等待关闭的环境。"""
        moment = self._clock.utc_now() if now is None else now
        return self._environments.warm_expired(moment)

    def mark_busy(self, call: EnvironmentCall) -> EnvironmentRow:
        """脚本开始执行：环境从 ready 进入 busy。"""
        row = self.get(call.environment_id)
        return self._apply(
            row,
            EnvironmentState.BUSY,
            at=self._clock.utc_now(),
            detail={"action": "execute"},
        )

    def _finish_action(
        self,
        row: EnvironmentRow,
        result: PhaseResult,
        *,
        success: EnvironmentState,
        action: str,
    ) -> None:
        """按发起插件调用前记录的前置 row 应用动作结果；前置失效由 _apply 仲裁丢弃。"""
        target = success if result.ok else EnvironmentState.FAILED
        if row.state is target:
            return
        self._apply(
            row,
            target,
            at=self._clock.utc_now(),
            detail={"action": action, "error_code": result.error_code or ""},
        )

    def _apply(
        self,
        row: EnvironmentRow,
        target: EnvironmentState,
        *,
        at: datetime,
        detail: dict[str, str],
    ) -> EnvironmentRow:
        """以前置 row.state 为仲裁基准写入目标状态；前置失效时丢弃本次写。

        读、判迁移、写状态与事件在同一 IMMEDIATE 事务内完成；事务内重读与前置
        不符说明另一连接先提交（docs/11 并发与超时），按先提交者赢丢弃：写一条
        core.env_action_conflict，不写状态与 env.state.changed，抛 EnvironmentStateError。
        冲突证据与判定同事务落盘，异常在事务提交后抛出。
        """
        conflict: EnvironmentStateError | None = None
        with transaction(self._connection):
            current = self._environments.get(row.id)
            if current is None:
                raise EnvironmentNotFoundError(f"环境不存在：{row.id}")
            if current.state is not row.state:
                self._record_conflict(row, current, at=at, action=detail.get("action", "probe"))
                conflict = EnvironmentStateError(
                    f"环境状态已从 {row.state.value} 变为 {current.state.value}，本次操作未应用"
                )
            else:
                self._write_state(current, target, at=at, detail=detail)
        if conflict is not None:
            raise conflict
        return self.get(row.id)

    def _record_conflict(
        self,
        expected: EnvironmentRow,
        actual: EnvironmentRow,
        *,
        at: datetime,
        action: str,
    ) -> None:
        """输家证据：前置状态已变，本次写入被丢弃，只记 core.env_action_conflict。"""
        self._events.append_global(
            EventType.CORE_EVENT,
            {
                "event": ENV_ACTION_CONFLICT,
                "payload": {
                    "environment_id": expected.id,
                    "action": action,
                    "expected_state": expected.state.value,
                    "actual_state": actual.state.value,
                },
            },
            at=at,
        )

    def _write_state(
        self,
        current: EnvironmentRow,
        target: EnvironmentState,
        *,
        at: datetime,
        detail: dict[str, str],
    ) -> None:
        """前置成立时的落盘：目标同状态只刷新观测时间，迁移则写状态与事件。"""
        if target is current.state:
            self._environments.touch(current.id, at=at, detail=detail)
            return
        require_transition(current.state, target)
        self._environments.set_state(current.id, target, at=at, detail=detail)
        self._events.append_global(
            EventType.ENV_STATE_CHANGED,
            {
                "environment_id": current.id,
                "from_state": current.state.value,
                "to_state": target.value,
                "probe": dict(detail),
            },
            at=at,
        )
