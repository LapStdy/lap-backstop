"""ISO 8601 时间戳编解码：数据库只存 UTC 文本，读出必须是带时区的 datetime。"""

from __future__ import annotations

from datetime import UTC, datetime


def format_at(moment: datetime) -> str:
    """把带时区时刻格式化成 UTC ISO 8601 文本。

    Raises:
        ValueError: 时刻是 naive datetime。
    """
    if moment.tzinfo is None:
        raise ValueError("时刻必须带时区")
    return moment.astimezone(UTC).isoformat()


def parse_at(value: str) -> datetime:
    """解析数据库里的 ISO 8601 文本。

    Raises:
        ValueError: 文本不是合法时间，或解析结果不带时区。
    """
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        raise ValueError(f"数据库时间缺少时区：{value!r}")
    return moment.astimezone(UTC)


def parse_optional_at(value: str | None) -> datetime | None:
    """可空时间戳：空值原样返回 None。"""
    return None if value is None else parse_at(value)
