"""五字段 cron 的解析、取值与日/周规则。"""

from __future__ import annotations

from datetime import date

import pytest

from core.scheduler.cron import CronSyntaxError, parse_cron


def test_parse_cron_expands_lists_ranges_and_steps() -> None:
    spec = parse_cron("0 3,15 1-5 */2 1")
    assert spec.minutes == frozenset({0})
    assert spec.hours == frozenset({3, 15})
    assert spec.days_of_month == frozenset({1, 2, 3, 4, 5})
    assert spec.months == frozenset({1, 3, 5, 7, 9, 11})
    assert spec.days_of_week == frozenset({1})


def test_parse_cron_maps_sunday_alias() -> None:
    assert parse_cron("0 0 * * 7").days_of_week == frozenset({0})


def test_parse_cron_marks_restricted_fields() -> None:
    spec = parse_cron("0 0 * * *")
    assert not spec.day_of_month_restricted
    assert not spec.day_of_week_restricted
    restricted = parse_cron("0 0 1 * 1")
    assert restricted.day_of_month_restricted
    assert restricted.day_of_week_restricted


def test_day_and_week_restricted_are_ored() -> None:
    spec = parse_cron("0 0 1 * 1")
    assert spec.matches_calendar(date(2026, 10, 1), 0, 0)  # 每月 1 日
    assert spec.matches_calendar(date(2026, 10, 5), 0, 0)  # 周一
    assert not spec.matches_calendar(date(2026, 10, 6), 0, 0)  # 周二且非 1 日


def test_day_and_week_unrestricted_are_anded() -> None:
    spec = parse_cron("0 0 * * *")
    assert spec.matches_calendar(date(2026, 10, 6), 0, 0)


def test_month_restriction_applies() -> None:
    spec = parse_cron("0 0 * 2 *")
    assert spec.matches_calendar(date(2026, 2, 3), 0, 0)
    assert not spec.matches_calendar(date(2026, 3, 3), 0, 0)


@pytest.mark.parametrize(
    "expression",
    ["0 0 * *", "0 24 * * *", "60 0 * * *", "0 0 0 * *", "0 0 * 13 *", "0 0 * * 8", "*/0 * * * *"],
)
def test_invalid_cron_is_rejected(expression: str) -> None:
    with pytest.raises(CronSyntaxError):
        parse_cron(expression)


def test_names_are_not_supported() -> None:
    with pytest.raises(CronSyntaxError):
        parse_cron("0 0 * JAN *")
