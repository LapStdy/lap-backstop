"""单测：运行到期判断的纯函数——retry_at 解析口径与 retry/scheduled 到期。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from contracts import RunState
from core.supervisor_recovery import retry_due, scheduled_due
from storage.repositories.job_runs import JobRunRecord

T0 = datetime(2026, 10, 27, 12, 0, tzinfo=UTC)


def run_record(
    *,
    detail: Mapping[str, object] | None = None,
    scheduled_at: datetime | None = T0,
) -> JobRunRecord:
    return JobRunRecord(
        id="run-1",
        template_id="template-1",
        schedule_id=None,
        scheduled_at=scheduled_at,
        created_at=T0,
        idempotency_key="key-1",
        state=RunState.RECOVERING,
        attempt=1,
        priority=0,
        deadline_at=None,
        started_at=None,
        finished_at=None,
        verdict_id=None,
        error_code=None,
        detail={} if detail is None else detail,
    )


def test_retry_due_without_retry_at_is_due_immediately() -> None:
    assert retry_due(run_record(detail={}), now=T0)


def test_retry_due_future_retry_at_not_due_until_passed() -> None:
    record = run_record(detail={"retry_at": (T0 + timedelta(minutes=5)).isoformat()})
    assert not retry_due(record, now=T0)
    assert retry_due(record, now=T0 + timedelta(minutes=5))


def test_retry_due_naive_retry_at_is_read_as_utc() -> None:
    naive = (T0 + timedelta(minutes=5)).replace(tzinfo=None)
    record = run_record(detail={"retry_at": naive.isoformat()})
    assert not retry_due(record, now=T0)
    assert retry_due(record, now=T0 + timedelta(minutes=5))


def test_retry_due_non_string_retry_at_is_due_immediately() -> None:
    assert retry_due(run_record(detail={"retry_at": 17}), now=T0)


def test_scheduled_due_without_scheduled_at_is_due_immediately() -> None:
    assert scheduled_due(run_record(scheduled_at=None), now=T0)


def test_scheduled_due_future_scheduled_at_not_due_until_passed() -> None:
    record = run_record(scheduled_at=T0 + timedelta(minutes=5))
    assert not scheduled_due(record, now=T0)
    assert scheduled_due(record, now=T0 + timedelta(minutes=5))
