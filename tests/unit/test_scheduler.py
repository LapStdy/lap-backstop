"""调度发生点、补跑策略与幂等键。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from core.scheduler.occurrences import (
    CatchUp,
    ScheduleError,
    ScheduleKind,
    ScheduleSpec,
    manual_idempotency_key,
    next_occurrence,
    occurrences_between,
    plan_missed,
    scheduled_idempotency_key,
)


def make_cron(**overrides: object) -> ScheduleSpec:
    values: dict[str, object] = {
        "id": "arknights_daily",
        "kind": ScheduleKind.CRON,
        "timezone": "Asia/Shanghai",
        "expr": "0 3,15 * * *",
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    values.update(overrides)
    return ScheduleSpec(**values)  # type: ignore[arg-type]


def make_interval(**overrides: object) -> ScheduleSpec:
    values: dict[str, object] = {
        "id": "every_hour",
        "kind": ScheduleKind.INTERVAL,
        "timezone": "UTC",
        "every_minutes": 60,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    values.update(overrides)
    return ScheduleSpec(**values)  # type: ignore[arg-type]


def test_cron_occurrences_follow_local_wall_clock() -> None:
    spec = make_cron()
    found = occurrences_between(
        spec,
        datetime(2026, 10, 22, 12, 0, tzinfo=UTC),
        datetime(2026, 10, 23, 12, 0, tzinfo=UTC),
    )
    assert found == (
        datetime(2026, 10, 22, 19, 0, tzinfo=UTC),  # 10-23 03:00 +08
        datetime(2026, 10, 23, 7, 0, tzinfo=UTC),  # 10-23 15:00 +08
    )


def test_cron_skips_nonexistent_local_time_on_spring_forward() -> None:
    spec = make_cron(timezone="America/New_York", expr="30 2 * * *")
    found = occurrences_between(
        spec,
        datetime(2026, 3, 8, 0, 0, tzinfo=UTC),
        datetime(2026, 3, 8, 12, 0, tzinfo=UTC),
    )
    assert found == ()


def test_cron_runs_once_for_repeated_local_time_on_fall_back() -> None:
    spec = make_cron(timezone="America/New_York", expr="30 1 * * *")
    found = occurrences_between(
        spec,
        datetime(2026, 11, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 11, 1, 23, 59, tzinfo=UTC),
    )
    assert found == (datetime(2026, 11, 1, 5, 30, tzinfo=UTC),)


def test_interval_occurrences_align_to_created_at() -> None:
    spec = make_interval()
    found = occurrences_between(
        spec,
        datetime(2026, 1, 1, 0, 30, tzinfo=UTC),
        datetime(2026, 1, 1, 2, 30, tzinfo=UTC),
    )
    assert found == (
        datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        datetime(2026, 1, 1, 2, 0, tzinfo=UTC),
    )


def test_at_occurrence_is_a_single_point() -> None:
    moment = datetime(2026, 12, 1, 0, 0, tzinfo=UTC)
    spec = ScheduleSpec(id="once", kind=ScheduleKind.AT, timezone="UTC", at=moment)
    found = occurrences_between(
        spec,
        datetime(2026, 11, 30, tzinfo=UTC),
        datetime(2026, 12, 2, tzinfo=UTC),
    )
    assert found == (moment,)
    assert next_occurrence(spec, datetime(2026, 11, 30, tzinfo=UTC)) == moment
    assert next_occurrence(spec, moment) is None


def test_next_occurrence_is_strictly_later() -> None:
    spec = make_interval()
    assert next_occurrence(spec, datetime(2026, 1, 1, tzinfo=UTC)) == datetime(
        2026, 1, 1, 1, 0, tzinfo=UTC
    )


def test_catch_up_run_once_keeps_only_latest_point() -> None:
    spec = make_interval(next_run_at=datetime(2026, 1, 1, tzinfo=UTC))
    runs = plan_missed(spec, datetime(2026, 1, 1, 3, tzinfo=UTC))
    assert [run.scheduled_at for run in runs] == [datetime(2026, 1, 1, 3, tzinfo=UTC)]
    assert runs[0].merged_scheduled_ats == ()


def test_catch_up_window_with_coalesce_merges_into_latest() -> None:
    spec = make_interval(
        catch_up=CatchUp.WINDOW,
        window_minutes=60,
        next_run_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    runs = plan_missed(spec, datetime(2026, 1, 1, 3, tzinfo=UTC))
    assert len(runs) == 1
    assert runs[0].scheduled_at == datetime(2026, 1, 1, 3, tzinfo=UTC)
    assert runs[0].merged_scheduled_ats == (datetime(2026, 1, 1, 2, tzinfo=UTC),)


def test_catch_up_window_without_coalesce_keeps_each_point() -> None:
    spec = make_interval(
        catch_up=CatchUp.WINDOW,
        window_minutes=60,
        coalesce=False,
        next_run_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    runs = plan_missed(spec, datetime(2026, 1, 1, 3, tzinfo=UTC))
    assert [run.scheduled_at for run in runs] == [
        datetime(2026, 1, 1, 2, tzinfo=UTC),
        datetime(2026, 1, 1, 3, tzinfo=UTC),
    ]


def test_catch_up_skip_produces_no_run() -> None:
    spec = make_interval(
        catch_up=CatchUp.SKIP,
        next_run_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert plan_missed(spec, datetime(2026, 1, 1, 3, tzinfo=UTC)) == ()


def test_catch_up_ignores_future_and_disabled_schedules() -> None:
    future = make_interval(next_run_at=datetime(2026, 1, 1, 4, tzinfo=UTC))
    assert plan_missed(future, datetime(2026, 1, 1, 3, tzinfo=UTC)) == ()
    disabled = make_interval(
        enabled=False,
        next_run_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert plan_missed(disabled, datetime(2026, 1, 1, 3, tzinfo=UTC)) == ()


def test_cron_catch_up_uses_persisted_next_run() -> None:
    spec = make_cron(next_run_at=datetime(2026, 10, 23, 7, 0, tzinfo=UTC))
    runs = plan_missed(spec, datetime(2026, 10, 23, 20, 0, tzinfo=UTC))
    assert [run.scheduled_at for run in runs] == [datetime(2026, 10, 23, 19, 0, tzinfo=UTC)]


def test_window_must_fit_inside_deadline() -> None:
    with pytest.raises(ScheduleError):
        make_cron(window_minutes=240, deadline_minutes=180)


def test_unknown_timezone_is_rejected() -> None:
    with pytest.raises(ScheduleError):
        make_cron(timezone="Mars/Olympus")


def test_bad_cron_expression_is_rejected() -> None:
    with pytest.raises(ScheduleError):
        make_cron(expr="0 3 * *")


def test_interval_requires_anchor() -> None:
    with pytest.raises(ScheduleError):
        ScheduleSpec(id="x", kind=ScheduleKind.INTERVAL, timezone="UTC", every_minutes=10)


def test_naive_at_is_rejected() -> None:
    with pytest.raises(ScheduleError):
        ScheduleSpec(
            id="x",
            kind=ScheduleKind.AT,
            timezone="UTC",
            at=datetime(2026, 1, 1),
        )


def test_scheduled_idempotency_key_truncates_to_whole_seconds() -> None:
    moment = datetime(2026, 1, 1, 0, 0, 0, 500000, tzinfo=UTC)
    assert scheduled_idempotency_key("daily", moment) == "daily:2026-01-01T00:00:00+00:00"
    with pytest.raises(ValueError):
        scheduled_idempotency_key("daily", datetime(2026, 1, 1))


def test_manual_idempotency_key_prefixes_request_key() -> None:
    assert manual_idempotency_key("abc-123") == "manual:abc-123"
    with pytest.raises(ValueError):
        manual_idempotency_key("")


def test_occurrence_times_stay_utc() -> None:
    spec = make_cron(next_run_at=datetime(2026, 10, 23, 7, 0, tzinfo=UTC))
    runs = plan_missed(spec, datetime(2026, 10, 23, 20, 0, tzinfo=UTC))
    assert all(run.scheduled_at.utcoffset() == timedelta(0) for run in runs)
