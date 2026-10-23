"""调度发生点与补跑计算。

纯函数：不读系统时间、不碰数据库；调用方注入 Clock 与持久化的 ``next_run_at``。
cron 按本地墙钟展开：夏令时不存在的时间点跳过，重复的时间点只取较早一次（docs/09）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from core.scheduler.cron import CronSpec, CronSyntaxError, parse_cron

_ONE_MINUTE = timedelta(minutes=1)
_ONE_DAY = timedelta(days=1)
_NEXT_HORIZON = timedelta(days=366 * 4)


class ScheduleKind(StrEnum):
    CRON = "cron"
    AT = "at"
    INTERVAL = "interval"


class CatchUp(StrEnum):
    RUN_ONCE = "run_once"
    SKIP = "skip"
    WINDOW = "window"


class ScheduleError(ValueError):
    """调度配置不合法，对应错误码 E_CONFIG。"""


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    """一条调度的完整语义；字段与 docs/09 的调度类型和公共字段一一对应。"""

    id: str
    kind: ScheduleKind
    timezone: str
    catch_up: CatchUp = CatchUp.RUN_ONCE
    window_minutes: int = 120
    coalesce: bool = True
    priority: int = 0
    deadline_minutes: int = 180
    preheat_minutes: int = 0
    keep_warm_minutes: int = 0
    expr: str | None = None
    at: datetime | None = None
    every_minutes: int | None = None
    created_at: datetime | None = None
    next_run_at: datetime | None = None
    enabled: bool = True

    def __post_init__(self) -> None:
        self._validate_windows()
        self._validate_timezone()
        _require_aware(self.at, "at")
        _require_aware(self.created_at, "created_at")
        _require_aware(self.next_run_at, "next_run_at")
        self._validate_kind()

    def _validate_windows(self) -> None:
        if self.window_minutes < 0 or self.preheat_minutes < 0 or self.keep_warm_minutes < 0:
            raise ScheduleError("分钟数不能为负")
        if self.deadline_minutes <= 0:
            raise ScheduleError("deadline_minutes 必须大于 0")
        if self.window_minutes > self.deadline_minutes:
            raise ScheduleError("window_minutes 不能大于 deadline_minutes")

    def _validate_timezone(self) -> None:
        try:
            ZoneInfo(self.timezone)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ScheduleError(f"未知时区：{self.timezone!r}") from error

    def _validate_kind(self) -> None:
        if self.kind is ScheduleKind.CRON:
            if not self.expr:
                raise ScheduleError("kind=cron 需要 expr")
            try:
                parse_cron(self.expr)
            except CronSyntaxError as error:
                raise ScheduleError(str(error)) from error
            return
        if self.kind is ScheduleKind.AT:
            if self.at is None:
                raise ScheduleError("kind=at 需要 at")
            return
        if self.every_minutes is None or self.every_minutes <= 0:
            raise ScheduleError("kind=interval 需要正的 every_minutes")
        if self.created_at is None:
            raise ScheduleError("kind=interval 需要 created_at 作为对齐锚点")


@dataclass(frozen=True, slots=True)
class Occurrence:
    """一个发生点；被合并的其它发生点记在 ``merged_scheduled_ats``。"""

    scheduled_at: datetime
    merged_scheduled_ats: tuple[datetime, ...] = ()


def occurrences_between(
    spec: ScheduleSpec,
    start: datetime,
    end: datetime,
) -> tuple[datetime, ...]:
    """返回闭区间 ``[start, end]`` 内的发生点，UTC 升序。"""
    start_utc = _as_utc(start)
    end_utc = _as_utc(end)
    if start_utc > end_utc:
        return ()
    if spec.kind is ScheduleKind.AT:
        return _at_occurrences(spec, start_utc, end_utc)
    if spec.kind is ScheduleKind.INTERVAL:
        return _interval_occurrences(spec, start_utc, end_utc)
    if spec.expr is None:
        return ()
    timezone = ZoneInfo(spec.timezone)
    cron = parse_cron(spec.expr)
    first_day = start_utc.astimezone(timezone).date() - _ONE_DAY
    last_day = end_utc.astimezone(timezone).date() + _ONE_DAY
    found: dict[datetime, None] = {}
    day = first_day
    while day <= last_day:
        for moment in _day_occurrences(cron, day, timezone):
            if start_utc <= moment <= end_utc:
                found[moment] = None
        day += _ONE_DAY
    return tuple(sorted(found))


def next_occurrence(spec: ScheduleSpec, after: datetime) -> datetime | None:
    """返回严格晚于 ``after`` 的下一个发生点；没有则返回 None。"""
    pivot = _as_utc(after)
    if spec.kind is ScheduleKind.AT:
        moment = _as_utc(spec.at) if spec.at is not None else None
        return moment if moment is not None and moment > pivot else None
    if spec.kind is ScheduleKind.INTERVAL:
        return _interval_next(spec, pivot)
    if spec.expr is None:
        return None
    timezone = ZoneInfo(spec.timezone)
    cron = parse_cron(spec.expr)
    day = pivot.astimezone(timezone).date() - _ONE_DAY
    last_day = (pivot + _NEXT_HORIZON).astimezone(timezone).date() + _ONE_DAY
    while day <= last_day:
        for moment in _day_occurrences(cron, day, timezone):
            if moment > pivot:
                return moment
        day += _ONE_DAY
    return None


def plan_missed(spec: ScheduleSpec, now: datetime) -> tuple[Occurrence, ...]:
    """按 ``catch_up`` 策略计算停机期间应补的运行。

    起点是持久化的 ``next_run_at``：它是停机前已算出、尚未执行的发生点。
    """
    if not spec.enabled or spec.next_run_at is None:
        return ()
    now_utc = _as_utc(now)
    missed = occurrences_between(spec, spec.next_run_at, now_utc)
    if not missed:
        return ()
    if spec.catch_up is CatchUp.SKIP:
        return ()
    if spec.catch_up is CatchUp.RUN_ONCE:
        return (Occurrence(scheduled_at=missed[-1]),)
    cutoff = now_utc - timedelta(minutes=spec.window_minutes)
    in_window = tuple(moment for moment in missed if moment >= cutoff)
    if not in_window:
        return ()
    if not spec.coalesce:
        return tuple(Occurrence(scheduled_at=moment) for moment in in_window)
    return (
        Occurrence(
            scheduled_at=in_window[-1],
            merged_scheduled_ats=in_window[:-1],
        ),
    )


def scheduled_idempotency_key(schedule_id: str, scheduled_at: datetime) -> str:
    """计划运行的幂等键：``<schedule_id>:<UTC 整秒>``。

    Raises:
        ValueError: 时刻是 naive datetime。
    """
    normalized = _as_utc(scheduled_at).replace(microsecond=0)
    return f"{schedule_id}:{normalized.isoformat()}"


def manual_idempotency_key(request_key: str) -> str:
    """手动立即跑的幂等键：``manual:<Idempotency-Key>``。"""
    if not request_key:
        raise ValueError("Idempotency-Key 不能为空")
    return f"manual:{request_key}"


def _at_occurrences(
    spec: ScheduleSpec,
    start: datetime,
    end: datetime,
) -> tuple[datetime, ...]:
    if spec.at is None:
        return ()
    moment = _as_utc(spec.at)
    return (moment,) if start <= moment <= end else ()


def _interval_occurrences(
    spec: ScheduleSpec,
    start: datetime,
    end: datetime,
) -> tuple[datetime, ...]:
    every, anchor = _interval_parts(spec)
    if start <= anchor:
        candidate = anchor
    else:
        steps = (start - anchor) // every
        candidate = anchor + steps * every
        if candidate < start:
            candidate += every
    moments: list[datetime] = []
    while candidate <= end:
        moments.append(candidate)
        candidate += every
    return tuple(moments)


def _interval_next(spec: ScheduleSpec, after: datetime) -> datetime:
    every, anchor = _interval_parts(spec)
    if after < anchor:
        return anchor
    steps = (after - anchor) // every + 1
    return anchor + steps * every


def _interval_parts(spec: ScheduleSpec) -> tuple[timedelta, datetime]:
    if spec.every_minutes is None or spec.created_at is None:
        raise ScheduleError("kind=interval 需要 every_minutes 与 created_at")
    return timedelta(minutes=spec.every_minutes), spec.created_at


def _day_occurrences(cron: CronSpec, day: date, timezone: ZoneInfo) -> list[datetime]:
    moments: list[datetime] = []
    for hour in sorted(cron.hours):
        for minute in sorted(cron.minutes):
            if not cron.matches_calendar(day, hour, minute):
                continue
            moment = _local_to_utc(day, hour, minute, timezone)
            if moment is not None:
                moments.append(moment)
    return sorted(moments)


def _local_to_utc(
    day: date,
    hour: int,
    minute: int,
    timezone: ZoneInfo,
) -> datetime | None:
    """把本地墙钟时刻换成 UTC；本地时刻不存在时返回 None。

    夏令时回拨会让同一墙钟出现两次，这里只取较早的实例，保证一次触发只执行一遍。
    """
    naive = datetime(day.year, day.month, day.day, hour, minute)
    local = naive.replace(tzinfo=timezone, fold=0)
    moment = local.astimezone(UTC)
    round_trip = moment.astimezone(timezone)
    if (
        round_trip.year,
        round_trip.month,
        round_trip.day,
        round_trip.hour,
        round_trip.minute,
    ) != (naive.year, naive.month, naive.day, naive.hour, naive.minute):
        return None
    return moment


def _as_utc(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        raise ValueError("时刻必须带时区")
    return moment.astimezone(UTC)


def _require_aware(moment: datetime | None, name: str) -> None:
    if moment is not None and moment.tzinfo is None:
        raise ScheduleError(f"{name} 必须是带时区的时刻")


__all__ = [
    "CatchUp",
    "Occurrence",
    "ScheduleError",
    "ScheduleKind",
    "ScheduleSpec",
    "manual_idempotency_key",
    "next_occurrence",
    "occurrences_between",
    "plan_missed",
    "scheduled_idempotency_key",
]
