"""仓储共用的 DB JSON 列编解码：排序键写入、对象形状读出。"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping


def dump_json(payload: Mapping[str, object]) -> str:
    """把 JSON 列写成排序键文本；键序稳定便于比对与测试。"""
    return json.dumps(dict(payload), ensure_ascii=False, sort_keys=True)


def load_json_object(value: object, what: str) -> Mapping[str, object]:
    """读 JSON 列并收窄成对象；不是对象按数据损坏抛 DatabaseError。"""
    payload = json.loads(str(value))
    if not isinstance(payload, dict):
        raise sqlite3.DatabaseError(f"{what} 不是 JSON 对象")
    return payload


__all__ = ["dump_json", "load_json_object"]
