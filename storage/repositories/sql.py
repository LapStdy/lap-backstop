"""仓储共用的 SQL 小工具：可空文本收窄与 IN 占位符生成。"""

from __future__ import annotations

from collections.abc import Sequence


def optional_str(value: object) -> str | None:
    """把可空列收窄成 str 或 None；读 SQLite 行时用。"""
    return None if value is None else str(value)


def placeholders(values: Sequence[object]) -> str:
    """生成 IN 子句的占位符；调用方按同一顺序展开 values。"""
    return ", ".join("?" for _ in values)


__all__ = ["optional_str", "placeholders"]
