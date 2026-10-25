"""环境服务：把插件探针与动作翻译成状态迁移与事件。"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

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
        探针本身失败时返回 None。
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
            self._apply(
                row,
                EnvironmentState.UNKNOWN,
                at=at,
                detail={"detail": f"探针报告 {probe.state.value}，不允许从 {row.state.value} 迁入"},
            )
        return probe

    def mark_unknown(self, environment_id: str, *, detail: str) -> EnvironmentRow:
        """状态不明（探针不可用等）：置 unknown，等下一次探测再判定。"""
        row = self.get(environment_id)
        return self._apply(
            row,
            EnvironmentState.UNKNOWN,
            at=self._clock.utc_now(),
            detail={"detail": detail},
        )

    def prepare(self, call: EnvironmentCall) -> PhaseResult:
        """准备环境：unknown/off/failed 先进入 booting，成功后 ready。"""
        row = self.get(call.environment_id)
        if row.state is EnvironmentState.READY:
            now = self._clock.utc_now()
            return PhaseResult(ok=True, detail="环境已就绪", started_at=now, ended_at=now)
        if row.state is EnvironmentState.BUSY:
            raise EnvironmentStateError("环境执行中，不能准备")
        if row.state is not EnvironmentState.BOOTING:
            self._apply(
                row,
                EnvironmentState.BOOTING,
                at=self._clock.utc_now(),
                detail={"action": "prepare"},
            )
        result = call.provider.prepare(call.ref, call.ctx)
        self._finish_action(call, result, success=EnvironmentState.READY, action="prepare")
        return result

    def reset(self, call: EnvironmentCall) -> PhaseResult:
        """复位环境：任何状态先进入 booting；复位成功后仍等探针确认 ready。"""
        row = self.get(call.environment_id)
        if row.state is not EnvironmentState.BOOTING:
            self._apply(
                row,
                EnvironmentState.BOOTING,
                at=self._clock.utc_now(),
                detail={"action": "reset"},
            )
        result = call.provider.reset(call.ref, call.ctx)
        if not result.ok:
            self._apply(
                self.get(call.environment_id),
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
        call: EnvironmentCall,
        result: PhaseResult,
        *,
        success: EnvironmentState,
        action: str,
    ) -> None:
        target = success if result.ok else EnvironmentState.FAILED
        current = self.get(call.environment_id)
        if current.state is target:
            return
        self._apply(
            current,
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
        if target is row.state:
            with transaction(self._connection):
                self._environments.touch(row.id, at=at, detail=detail)
            return self.get(row.id)
        require_transition(row.state, target)
        with transaction(self._connection):
            self._environments.set_state(row.id, target, at=at, detail=detail)
            self._events.append_global(
                EventType.ENV_STATE_CHANGED,
                {
                    "environment_id": row.id,
                    "from_state": row.state.value,
                    "to_state": target.value,
                    "probe": dict(detail),
                },
                at=at,
            )
        return self.get(row.id)
