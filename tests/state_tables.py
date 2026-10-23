"""把 docs/02 的两张迁移表解析成枚举集合。

文档是迁移表的唯一真源：测试拿这里的结果比对 core 里的状态机，每条边都由文档生成。
表格格式、状态名或行列数不符合预期时直接抛 ``DocumentTableError``，不静默降级。
迁移到自身不算迁移：「任意状态」边也不把当前状态算作迁出。
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from contracts import EnvironmentState, RunState

DOC_PATH: Final = Path(__file__).resolve().parents[1] / "docs" / "02-架构.md"

ANY_STATE_LABEL: Final = "任意状态"
NO_TARGET_LABEL: Final = "无"

_SEPARATOR: Final = re.compile(r":?-{3,}:?")


class DocumentTableError(ValueError):
    """docs/02 里的迁移表与预期的表格结构或状态名不符。"""


@dataclass(frozen=True, slots=True)
class TransitionTable[StateT: StrEnum]:
    """一张迁移表：显式边、任意状态边，以及由两者推出的合法边。"""

    edges: Mapping[StateT, frozenset[StateT]]
    any_state_targets: frozenset[StateT] = frozenset()

    @property
    def states(self) -> tuple[StateT, ...]:
        """表里声明的全部状态，按取值排序。"""
        return _ordered(self.edges)

    def targets(self, state: StateT) -> frozenset[StateT]:
        """当前状态的全部合法迁出目标，不含自身。"""
        if state not in self.edges:
            raise DocumentTableError(f"迁移表没有声明状态：{state.value}")
        return (self.edges[state] | self.any_state_targets) - {state}

    def legal_edges(self) -> tuple[tuple[StateT, StateT], ...]:
        """每条合法的 (起点, 终点)。"""
        return tuple(
            (source, target) for source in self.states for target in _ordered(self.targets(source))
        )

    def rejected_edges(self) -> tuple[tuple[StateT, StateT], ...]:
        """每条被表拒绝的 (起点, 终点)；自身到自身不算迁移，不计入。"""
        return tuple(
            (source, target)
            for source in self.states
            for target in self.states
            if target is not source and target not in self.targets(source)
        )


def environment_table(text: str | None = None) -> TransitionTable[EnvironmentState]:
    """解析「Environment」表：七个状态的显式迁出与两行任意状态迁出。"""
    rows = _table(
        _section(_read_document(text), "Environment"),
        ("状态", "含义", "允许迁出"),
    )
    edges: dict[EnvironmentState, frozenset[EnvironmentState]] = {}
    any_targets: set[EnvironmentState] = set()
    for label, meaning, allowed in rows:
        if label == ANY_STATE_LABEL:
            # 文档把任意状态的目标写在「含义」列，条件写在「允许迁出」列。
            any_targets.update(_enum_names(EnvironmentState, meaning, where="任意状态行"))
            continue
        state = _enum(EnvironmentState, label, where="Environment 表")
        if state in edges:
            raise DocumentTableError(f"Environment 表重复声明状态：{label}")
        edges[state] = frozenset(_enum_names(EnvironmentState, allowed, where=f"{label} 行"))
    return TransitionTable(edges=edges, any_state_targets=frozenset(any_targets))


def run_table(text: str | None = None) -> TransitionTable[RunState]:
    """解析「JobRun」状态表与「状态迁移」表，返回 JobRun 的完整迁移表。"""
    document = _read_document(text)
    states = _run_states(document)
    rows = _table(_section(document, "状态迁移"), ("当前状态", "允许迁出", "条件"))
    edges: dict[RunState, frozenset[RunState]] = {state: frozenset() for state in states}
    seen: set[RunState] = set()
    for sources, targets, _condition in rows:
        edges_targets = frozenset(_enum_names(RunState, targets, where="允许迁出列"))
        for source in _enum_names(RunState, sources, where="当前状态列"):
            if source not in edges:
                raise DocumentTableError(f"状态迁移表的起点未在状态表声明：{source.value}")
            edges[source] = edges[source] | edges_targets
            seen.add(source)
    declared = frozenset(states)
    missing = declared - seen
    if missing:
        names = "、".join(sorted(state.value for state in missing))
        raise DocumentTableError(f"状态迁移表缺少这些状态的行：{names}")
    for source, row_targets in edges.items():
        unknown = row_targets - declared
        if unknown:
            names = "、".join(sorted(state.value for state in unknown))
            raise DocumentTableError(f"{source.value} 行的目标未在状态表声明：{names}")
    return TransitionTable(edges=edges)


def shortest_paths[StateT: StrEnum](
    table: TransitionTable[StateT],
    start: StateT,
) -> dict[StateT, tuple[StateT, ...]]:
    """从起始状态出发，沿文档迁移表到每个状态的最短迁移序列。"""
    if start not in table.edges:
        raise DocumentTableError(f"迁移表没有声明状态：{start.value}")
    paths: dict[StateT, tuple[StateT, ...]] = {start: ()}
    frontier = [start]
    while frontier:
        following: list[StateT] = []
        for state in frontier:
            for target in _ordered(table.targets(state)):
                if target in paths:
                    continue
                paths[target] = (*paths[state], target)
                following.append(target)
        frontier = following
    return paths


def _read_document(text: str | None) -> str:
    return DOC_PATH.read_text(encoding="utf-8") if text is None else text


def _section(text: str, title: str) -> tuple[str, ...]:
    lines = text.splitlines()
    heading = f"### {title}"
    starts = [index for index, line in enumerate(lines) if line.strip() == heading]
    if not starts:
        raise DocumentTableError(f"docs/02 里找不到小节：{title}")
    body: list[str] = []
    for line in lines[starts[0] + 1 :]:
        if line.startswith("## ") or line.startswith("### "):
            break
        body.append(line)
    return tuple(body)


def _cells(line: str) -> tuple[str, ...]:
    stripped = line.strip()
    if not stripped.startswith("|"):
        raise DocumentTableError(f"不是表格行：{line}")
    return tuple(cell.strip() for cell in stripped.strip("|").split("|"))


def _table(lines: Sequence[str], header: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    for index, line in enumerate(lines):
        if not line.strip().startswith("|"):
            continue
        if _cells(line) != header:
            continue
        rows: list[tuple[str, ...]] = []
        for row_line in lines[index + 1 :]:
            if not row_line.strip().startswith("|"):
                break
            row = _cells(row_line)
            if all(_SEPARATOR.fullmatch(cell) for cell in row):
                continue
            if len(row) != len(header):
                raise DocumentTableError(f"列数与表头不符：{row_line}")
            rows.append(row)
        if not rows:
            raise DocumentTableError(f"表头 {header} 下没有数据行")
        return tuple(rows)
    raise DocumentTableError(f"找不到表头为 {header} 的表格")


def _run_states(document: str) -> tuple[RunState, ...]:
    rows = _table(_section(document, "JobRun"), ("状态", "含义"))
    states = tuple(_enum(RunState, label, where="JobRun 状态表") for label, _meaning in rows)
    repeated = {state for state in states if states.count(state) > 1}
    if repeated:
        names = "、".join(sorted(state.value for state in repeated))
        raise DocumentTableError(f"JobRun 状态表重复声明状态：{names}")
    return states


def _names(cell: str) -> tuple[str, ...]:
    if cell == NO_TARGET_LABEL:
        return ()
    names: list[str] = []
    for chunk in cell.split("、"):
        for name in chunk.split("/"):
            cleaned = name.strip()
            if cleaned:
                names.append(cleaned)
    return tuple(names)


def _enum_names[StateT: StrEnum](
    enum: type[StateT], cell: str, *, where: str
) -> tuple[StateT, ...]:
    return tuple(_enum(enum, name, where=where) for name in _names(cell))


def _enum[StateT: StrEnum](enum: type[StateT], name: str, *, where: str) -> StateT:
    try:
        return enum(name)
    except ValueError as error:
        raise DocumentTableError(f"{where} 出现未知状态名：{name}") from error


def _ordered[StateT: StrEnum](states: Iterable[StateT]) -> tuple[StateT, ...]:
    return tuple(sorted(states, key=lambda state: state.value))
