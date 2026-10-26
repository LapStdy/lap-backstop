"""docs/03 端点表与 ROUTES 的对账：文档表格与请求-响应路由必须集合相等。

解析只认「| 方法 | 路径 | 说明 |」三列表格；SSE 流式端点不走 ROUTES，
不在对账范围。解析失败按 ValueError 报出，由测试变红，不静默跳过。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from core.api.app import ROUTES

DOCS_PATH = Path(__file__).resolve().parents[2] / "docs" / "03-数据模型与接口.md"

Route = tuple[str, str]

_METHOD = re.compile(r"[A-Z]+")
# 路径段：字面段或 {name} 参数捕获，与 Route.segments 的形态一致。
_SEGMENT = r"(?:[a-z0-9-]+|\{[a-z0-9-]+\})"
_PATH = re.compile(rf"/{_SEGMENT}(?:/{_SEGMENT})*")
_SEPARATOR = re.compile(r":?-{3,}:?")

FAKE_TABLE = """\
| 方法 | 路径 | 说明 |
|---|---|---|
| GET | /health | 存活、版本、server_id、能力 |
| GET | /schedules/{id} | 调度详情 |
| POST | /schedules/{id}/run | 立即跑一次，必须带 Idempotency-Key |
"""

FAKE_ROUTES: set[Route] = {
    ("GET", "health"),
    ("GET", "schedules/{id}"),
    ("POST", "schedules/{id}/run"),
}


def parse_route_table(markdown: str) -> set[Route]:
    """解析端点表，返回（方法、标准形路径）集合；路径去掉前导斜杠。

    表格按「| 方法 | 路径 | 说明 |」表头定位，说明列内容不参与对账；
    行顺序不约束。找不到表头、没有数据行或行不合文法都抛 ValueError。

    Raises:
        ValueError: 表头缺失、表格为空或某行的方法、路径不合文法。
    """
    rows: set[Route] = set()
    header_found = False
    for line in markdown.splitlines():
        cells = _cells(line)
        if not header_found:
            if len(cells) >= 2 and cells[0] == "方法" and cells[1] == "路径":
                header_found = True
            continue
        if not line.lstrip().startswith("|"):
            break
        if cells and all(_SEPARATOR.fullmatch(cell) for cell in cells):
            continue
        rows.add(_parse_row(cells, line))
    if not header_found:
        raise ValueError("端点表未找到：缺少「| 方法 | 路径 | 说明 |」表头")
    if not rows:
        raise ValueError("端点表没有数据行")
    return rows


def _cells(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|"):
        return []
    return [cell.strip() for cell in stripped.strip("|").split("|")]


def _parse_row(cells: list[str], raw: str) -> Route:
    """一行到（方法、标准形路径）；不合文法抛 ValueError 并带上原文。"""
    if len(cells) < 3:
        raise ValueError(f"端点表行需要 方法、路径、说明 三列：{raw}")
    method, path = cells[0], cells[1]
    if _METHOD.fullmatch(method) is None:
        raise ValueError(f"端点表行的方法必须是大写字母：{raw}")
    if _PATH.fullmatch(path) is None:
        raise ValueError(f"端点表行的路径不合文法：{raw}")
    return method, path[1:]


def route_diff(doc: set[Route], code: set[Route]) -> str:
    """两个路由集合的差异报告；相等返回空串，否则逐条列出差异行。"""
    only_in_docs = sorted(doc - code)
    only_in_code = sorted(code - doc)
    parts = []
    if only_in_docs:
        parts.append("端点表有而 ROUTES 没有：" + "、".join(_fmt(r) for r in only_in_docs))
    if only_in_code:
        parts.append("ROUTES 有而端点表没有：" + "、".join(_fmt(r) for r in only_in_code))
    return "；".join(parts)


def _fmt(route: Route) -> str:
    return f"{route[0]} /{route[1]}"


def implemented_routes() -> set[Route]:
    """ROUTES 派生的（方法、路径）集合；路径取段连接形，参数段保持 {name}。"""
    return {(route.method, "/".join(route.segments)) for route in ROUTES}


def docs_route_table() -> set[Route]:
    """docs/03 端点表的解析结果。"""
    return parse_route_table(DOCS_PATH.read_text(encoding="utf-8"))


def test_docs_endpoint_table_matches_implemented_routes() -> None:
    diff = route_diff(docs_route_table(), implemented_routes())
    assert diff == "", diff


def test_parse_reads_chinese_notes_and_braced_params() -> None:
    assert parse_route_table(FAKE_TABLE) == FAKE_ROUTES


def test_parse_unifies_leading_slash_and_ignores_row_order() -> None:
    reordered = """\
| 方法 | 路径 | 说明 |
|---|---|---|
| GET | /schedules/{id} | 调度详情 |
| GET | /health | 存活 |
"""
    assert parse_route_table(reordered) == FAKE_ROUTES - {("POST", "schedules/{id}/run")}


def test_route_diff_reports_row_missing_from_routes() -> None:
    doc = FAKE_ROUTES - {("POST", "schedules/{id}/run")}
    assert "schedules/{id}/run" in route_diff(doc, FAKE_ROUTES)


def test_route_diff_reports_row_missing_from_docs() -> None:
    doc = FAKE_ROUTES | {("POST", "advisor/suggest")}
    assert "advisor/suggest" in route_diff(doc, FAKE_ROUTES)


def test_route_diff_reports_wrong_method() -> None:
    doc = FAKE_ROUTES - {("GET", "health")} | {("POST", "health")}
    diff = route_diff(doc, FAKE_ROUTES)
    assert "POST /health" in diff


@pytest.mark.parametrize(
    "table",
    [
        "| GET | get | /health | 说明 |\n",
        "| GET | /health |\n",
        "| GET | health | 说明 |\n",
        "| GET | /health 存活 | 说明 |\n",
        "| 方法 | 路径 | 说明 |\n|---|---|---|\n",
        "| 路由 | 地址 | 说明 |\n|---|---|---|\n| GET | /health | 存活 |\n",
    ],
)
def test_parse_rejects_malformed_rows(table: str) -> None:
    with pytest.raises(ValueError):
        parse_route_table(table)
