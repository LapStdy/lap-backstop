"""五字段 cron 的解析与匹配。

字段顺序为分、时、日、月、周；支持 ``*``、列表、区间与步长。
周字段 0 与 7 都表示周日，与常见 crontab 一致。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

FIELD_RANGES: tuple[tuple[int, int], ...] = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))


class CronSyntaxError(ValueError):
    """cron 表达式不合法。"""


@dataclass(frozen=True, slots=True)
class CronSpec:
    """解析后的 cron：每个字段是取值集合，另记日与周是否被限定。"""

    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    day_of_month_restricted: bool
    day_of_week_restricted: bool

    def matches_calendar(self, day: date, hour: int, minute: int) -> bool:
        """判断某个本地日期与时刻是否命中。

        日与周同时被限定时按 Vixie cron 的语义取「或」，否则两者都要满足。
        """
        if minute not in self.minutes or hour not in self.hours or day.month not in self.months:
            return False
        day_of_week = (day.weekday() + 1) % 7
        day_of_month_match = day.day in self.days_of_month
        day_of_week_match = day_of_week in self.days_of_week
        if self.day_of_month_restricted and self.day_of_week_restricted:
            return day_of_month_match or day_of_week_match
        return day_of_month_match and day_of_week_match


def parse_cron(expression: str) -> CronSpec:
    """解析五字段 cron 表达式。

    Raises:
        CronSyntaxError: 字段数不对、取值越界或出现非法字符。
    """
    fields = expression.split()
    if len(fields) != 5:
        raise CronSyntaxError(f"cron 需要 5 个字段，实际 {len(fields)} 个：{expression!r}")
    parsed = [
        _parse_field(text, low, high)
        for text, (low, high) in zip(fields, FIELD_RANGES, strict=True)
    ]
    minutes, hours, days_of_month, months, days_of_week = parsed
    return CronSpec(
        minutes=minutes[0],
        hours=hours[0],
        days_of_month=days_of_month[0],
        months=months[0],
        days_of_week=frozenset(0 if value == 7 else value for value in days_of_week[0]),
        day_of_month_restricted=days_of_month[1],
        day_of_week_restricted=days_of_week[1],
    )


def _parse_field(text: str, low: int, high: int) -> tuple[frozenset[int], bool]:
    item = text.strip()
    if not item:
        raise CronSyntaxError("cron 字段为空")
    values: set[int] = set()
    for part in item.split(","):
        values.update(_parse_item(part, low, high))
    return frozenset(values), item != "*"


def _parse_item(item: str, low: int, high: int) -> set[int]:
    base, separator, step_text = item.partition("/")
    step = _parse_step(step_text) if separator else 1
    if base == "*":
        start, end = low, high
    elif "-" in base:
        start_text, _, end_text = base.partition("-")
        start = _parse_number(start_text, low, high)
        end = _parse_number(end_text, low, high)
    else:
        start = _parse_number(base, low, high)
        end = high if separator else start
    if start > end:
        raise CronSyntaxError(f"cron 区间起点大于终点：{item!r}")
    return set(range(start, end + 1, step))


def _parse_step(text: str) -> int:
    if not text.isdigit() or int(text) == 0:
        raise CronSyntaxError(f"cron 步长必须是正整数：{text!r}")
    return int(text)


def _parse_number(text: str, low: int, high: int) -> int:
    if not text.isdigit():
        raise CronSyntaxError(f"cron 只接受数字：{text!r}")
    value = int(text)
    if not low <= value <= high:
        raise CronSyntaxError(f"cron 取值越界：{value} 不在 {low}..{high}")
    return value
