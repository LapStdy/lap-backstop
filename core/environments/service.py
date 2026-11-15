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
    ErrorCode,
    EventType,
    PhaseResult,
    RunContext,
)
from core.environments.errors import EnvironmentNotFoundError, EnvironmentStateError
from core.environments.state_machine import require_transition
from storage.db.transaction import transaction
from storage.repositories.environments import EnvironmentRepository, EnvironmentRow
from storage.repositories.events import EventLog
from storage.repositories.leases import LeaseRepository

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
        self._writer = _StateWriter(connection)

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
            self._writer.apply(row, probe.state, at=at, detail={"detail": probe.detail})
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
            return self._writer.apply(
                row,
                EnvironmentState.UNKNOWN,
                at=self._clock.utc_now(),
                detail={"detail": detail},
            )
        except EnvironmentStateError:
            return self.get(environment_id)

    def prepare(self, call: EnvironmentCall) -> PhaseResult:
        """准备环境：unknown/off/failed 先进入 booting，成功后 ready。

        已 ready 的环境（保温窗口内复用）先调一次 ``warm(0)`` 确认：库里的 ready
        只说明上次收尾时的现场，游戏可能已被关掉或不在前台；确认失败按 prepare
        失败处置，交恢复策略收口。

        Raises:
            EnvironmentStateError: 环境 busy、状态机拒绝，或插件调用期间前置
                状态被其它连接改写（先提交者赢，本次结果丢弃）。
        """
        row = self.get(call.environment_id)
        if row.state is EnvironmentState.READY:
            return self._confirm_reuse(call, row)
        if row.state is EnvironmentState.BUSY:
            raise EnvironmentStateError("环境执行中，不能准备")
        if row.state is not EnvironmentState.BOOTING:
            row = self._writer.apply(
                row,
                EnvironmentState.BOOTING,
                at=self._clock.utc_now(),
                detail={"action": "prepare"},
            )
        result = call.provider.prepare(call.ref, call.ctx)
        self._finish_action(row, result, success=EnvironmentState.READY, action="prepare")
        return result

    def _confirm_reuse(self, call: EnvironmentCall, row: EnvironmentRow) -> PhaseResult:
        """复用保温中的环境：``warm(0)`` 只确认不延长保温窗口，失败置 failed。"""
        result = call.provider.warm(call.ref, call.ctx, 0)
        now = self._clock.utc_now()
        if not result.ok:
            self._writer.apply(
                row,
                EnvironmentState.FAILED,
                at=now,
                detail={"action": "prepare", "error_code": result.error_code or ""},
            )
            return result
        return PhaseResult(
            ok=True,
            detail=f"环境已就绪（{result.detail}）",
            started_at=result.started_at if result.started_at is not None else now,
            ended_at=result.ended_at if result.ended_at is not None else now,
        )

    def reset(self, call: EnvironmentCall) -> PhaseResult:
        """复位环境：任何状态先进入 booting；复位成功后仍等探针确认 ready。

        Raises:
            EnvironmentStateError: 状态机拒绝，或插件调用期间前置状态被其它
                连接改写（先提交者赢，本次结果丢弃）。
        """
        row = self.get(call.environment_id)
        if row.state is not EnvironmentState.BOOTING:
            row = self._writer.apply(
                row,
                EnvironmentState.BOOTING,
                at=self._clock.utc_now(),
                detail={"action": "reset"},
            )
        result = call.provider.reset(call.ref, call.ctx)
        if not result.ok:
            self._writer.apply(
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
        """清理环境：按保温策略回到 ready 或 off。

        以发起插件调用前读到的环境行为仲裁基准（docs/11 并发与超时）：调用期间
        前置状态被其它连接先提交时丢弃本次状态与保温写入，只记一条
        core.env_action_conflict，按 ``E_ENV_NOT_READY`` 失败结果收口，不向调用方
        抛状态冲突异常。
        """
        row = self.get(call.environment_id)
        result = call.provider.cleanup(call.ref, call.ctx, keep_warm)
        now = self._clock.utc_now()
        if not result.ok:
            return self._apply_failure(row, result, action="cleanup")
        target = EnvironmentState.READY if keep_warm else EnvironmentState.OFF
        with transaction(self._connection):
            try:
                baseline = row
                if row.state is EnvironmentState.BUSY and target is EnvironmentState.OFF:
                    baseline = self._writer.apply(
                        row,
                        EnvironmentState.READY,
                        at=now,
                        detail={"action": "cleanup"},
                    )
                self._writer.apply(baseline, target, at=now, detail={"action": "cleanup"})
                warm_until = now + timedelta(minutes=warm_minutes) if keep_warm else None
                self._environments.set_warm_until(call.environment_id, warm_until)
            except EnvironmentStateError as error:
                current = self.get(call.environment_id)
                if current.state is row.state:
                    raise
                return PhaseResult(
                    ok=False,
                    error_code=ErrorCode.ENV_NOT_READY.value,
                    detail=str(error),
                )
        return result

    def warm(self, call: EnvironmentCall, *, minutes: int) -> PhaseResult:
        """维持保温：只在已就绪的环境上生效，写 warm_until。

        以发起插件调用前读到的环境行为仲裁基准（docs/11 并发与超时）：调用期间
        前置状态被其它连接先提交时丢弃本次写入（成功路径的 warm_until 与失败路径
        的 failed），只记一条 core.env_action_conflict，按 ``E_ENV_NOT_READY`` 失败
        结果收口，不向调用方抛状态冲突异常。
        """
        row = self.get(call.environment_id)
        if row.state is not EnvironmentState.READY:
            raise EnvironmentStateError("保温要求环境已就绪")
        result = call.provider.warm(call.ref, call.ctx, minutes)
        if not result.ok:
            return self._apply_failure(row, result, action="warm")
        now = self._clock.utc_now()
        try:
            self._writer.keep_warm_until(row, now + timedelta(minutes=minutes), at=now)
        except EnvironmentStateError as error:
            return PhaseResult(
                ok=False,
                error_code=ErrorCode.ENV_NOT_READY.value,
                detail=str(error),
            )
        return result

    def warm_expired(self, now: datetime | None = None) -> tuple[EnvironmentRow, ...]:
        """保温到期且没有在途运行的环境；持有活跃租约的等运行收尾后再关。"""
        moment = self._clock.utc_now() if now is None else now
        leases = LeaseRepository(self._connection)
        return tuple(
            row for row in self._environments.warm_expired(moment) if not leases.active_for(row.id)
        )

    def mark_busy(self, call: EnvironmentCall) -> EnvironmentRow:
        """脚本开始执行：环境从 ready 进入 busy。"""
        row = self.get(call.environment_id)
        return self._writer.apply(
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
        self._writer.apply(
            row,
            target,
            at=self._clock.utc_now(),
            detail={"action": action, "error_code": result.error_code or ""},
        )

    def _apply_failure(
        self,
        row: EnvironmentRow,
        result: PhaseResult,
        *,
        action: str,
    ) -> PhaseResult:
        """provider 失败结果按前置 row 仲裁落盘：前置成立写 failed，原样返回结果。

        前置在插件调用期间被其它连接先提交时丢弃 failed 写入，返回
        ``E_ENV_NOT_READY`` 失败结果（detail 带前置变化与 provider 原错误码），
        终态保持赢家状态；前置未变但状态机拒绝时原样上抛（docs/11 并发与超时）。
        """
        try:
            self._writer.apply(
                row,
                EnvironmentState.FAILED,
                at=self._clock.utc_now(),
                detail={"action": action, "error_code": result.error_code or ""},
            )
        except EnvironmentStateError as error:
            current = self.get(row.id)
            if current.state is row.state:
                raise
            return PhaseResult(
                ok=False,
                error_code=ErrorCode.ENV_NOT_READY.value,
                detail=f"{error}（provider 原错误码：{result.error_code or '无'}）",
            )
        return result


class _StateWriter:
    """环境状态写入与跨连接仲裁：读、判迁移、写状态与事件在同一事务内完成。

    事务内重读与前置不符说明另一连接先提交（docs/11 并发与超时），按先提交者赢
    丢弃本次写：只写一条 core.env_action_conflict，不写状态与 env.state.changed。
    冲突证据与判定同事务落盘，异常在事务提交后抛出。
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self._environments = EnvironmentRepository(connection)
        self._events = EventLog(connection)

    def apply(
        self,
        row: EnvironmentRow,
        target: EnvironmentState,
        *,
        at: datetime,
        detail: dict[str, str],
    ) -> EnvironmentRow:
        """以前置 row.state 为仲裁基准写入目标状态；前置失效时丢弃本次写。"""
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
        updated = self._environments.get(row.id)
        if updated is None:
            raise EnvironmentNotFoundError(f"环境不存在：{row.id}")
        return updated

    def keep_warm_until(self, row: EnvironmentRow, warm_until: datetime, *, at: datetime) -> None:
        """以前置 row.state 为仲裁基准写保温到期；前置失效丢弃写入并记冲突。

        与 ``apply`` 同一仲裁，只写 warm_until 不动状态：前置失效时丢弃写入，
        只记一条 core.env_action_conflict（action=warm），证据与判定同事务落盘，
        异常在事务提交后抛出。
        """
        conflict: EnvironmentStateError | None = None
        with transaction(self._connection):
            current = self._environments.get(row.id)
            if current is None:
                raise EnvironmentNotFoundError(f"环境不存在：{row.id}")
            if current.state is not row.state:
                self._record_conflict(row, current, at=at, action="warm")
                conflict = EnvironmentStateError(
                    f"环境状态已从 {row.state.value} 变为 {current.state.value}，本次保温未写入"
                )
            else:
                self._environments.set_warm_until(row.id, warm_until)
        if conflict is not None:
            raise conflict

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
