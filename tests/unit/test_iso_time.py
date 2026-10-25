"""ISO 时间编解码：UTC 文本、时区归一、naive 拒绝与可空透传。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from storage.repositories.iso_time import format_at, parse_at, parse_optional_at

T0 = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)


def test_format_at_writes_utc_iso_text() -> None:
    assert format_at(T0) == "2026-10-25T08:00:00+00:00"


def test_format_at_converts_other_zones_to_utc() -> None:
    plus8 = timezone(timedelta(hours=8))
    local = datetime(2026, 10, 25, 16, 0, tzinfo=plus8)
    assert format_at(local) == "2026-10-25T08:00:00+00:00"


def test_format_at_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="带时区"):
        format_at(datetime(2026, 10, 25, 8, 0))


def test_parse_at_rejects_text_without_zone() -> None:
    with pytest.raises(ValueError, match="缺少时区"):
        parse_at("2026-10-25T08:00:00")


def test_parse_at_roundtrips_and_normalizes_zone() -> None:
    assert parse_at(format_at(T0)) == T0
    assert parse_at("2026-10-25T16:00:00+08:00") == T0


def test_parse_optional_at_passes_none_through() -> None:
    assert parse_optional_at(None) is None
    assert parse_optional_at("2026-10-25T08:00:00+00:00") == T0
