"""迁移表解析器：表格怎么读、哪里会报错。"""

from __future__ import annotations

import pytest
from tests.state_tables import (
    DocumentTableError,
    environment_table,
    run_table,
)

from contracts import EnvironmentState, RunState

SAMPLE = """\
# 02 · 架构

### Environment

| 状态 | 含义 | 允许迁出 |
|---|---|---|
| unknown | 尚未探测 | off、booting、failed |
| off | 未启动 | booting |
| 任意状态 | unknown | 休眠恢复或状态不明 |
| 任意状态 | booting | reset |

### JobRun

| 状态 | 含义 |
|---|---|
| scheduled | 已创建，未入队 |
| queued | 等待资源或工作进程 |
| preparing | 准备环境 |
| cancelled | 已取消 |
| succeeded | 完成 |
| failed | 失败 |

### 状态迁移

| 当前状态 | 允许迁出 | 条件 |
|---|---|---|
| scheduled | queued、cancelled | |
| queued | preparing、failed | |
| preparing | succeeded | |
| succeeded / failed / cancelled | 无 | 终态，不再迁出 |
"""


def test_environment_table_reads_explicit_rows() -> None:
    table = environment_table(SAMPLE)
    assert table.edges[EnvironmentState.UNKNOWN] == frozenset(
        {EnvironmentState.OFF, EnvironmentState.BOOTING, EnvironmentState.FAILED}
    )
    assert table.edges[EnvironmentState.OFF] == frozenset({EnvironmentState.BOOTING})


def test_environment_table_reads_any_state_targets_from_meaning_column() -> None:
    table = environment_table(SAMPLE)
    assert table.any_state_targets == frozenset(
        {EnvironmentState.UNKNOWN, EnvironmentState.BOOTING}
    )
    assert table.targets(EnvironmentState.OFF) == frozenset(
        {EnvironmentState.BOOTING, EnvironmentState.UNKNOWN}
    )


def test_self_is_never_a_target() -> None:
    table = environment_table(SAMPLE)
    for state in table.states:
        assert state not in table.targets(state)


def test_run_table_expands_slash_rows_and_dash_targets() -> None:
    table = run_table(SAMPLE)
    assert table.edges[RunState.SCHEDULED] == frozenset({RunState.QUEUED, RunState.CANCELLED})
    assert table.edges[RunState.SUCCEEDED] == frozenset()
    assert table.edges[RunState.FAILED] == frozenset()
    assert table.edges[RunState.CANCELLED] == frozenset()


def test_editing_a_row_changes_the_generated_table() -> None:
    trimmed = SAMPLE.replace("| queued | preparing、failed | |", "| queued | preparing | |")
    table = run_table(trimmed)
    assert table.edges[RunState.QUEUED] == frozenset({RunState.PREPARING})
    assert (RunState.QUEUED, RunState.FAILED) in table.rejected_edges()


def test_unknown_state_name_is_reported() -> None:
    broken = SAMPLE.replace("| off | 未启动 | booting |", "| stopped | 未启动 | booting |")
    with pytest.raises(DocumentTableError, match="stopped"):
        environment_table(broken)


def test_state_missing_from_transition_table_is_reported() -> None:
    broken = SAMPLE.replace("| preparing | succeeded | |\n", "")
    with pytest.raises(DocumentTableError, match="preparing"):
        run_table(broken)


def test_missing_table_is_reported() -> None:
    with pytest.raises(DocumentTableError, match="状态迁移"):
        run_table(
            "# 02 · 架构\n\n### JobRun\n\n| 状态 | 含义 |\n|---|---|\n| scheduled | 已创建 |\n"
        )


def test_column_count_mismatch_is_reported() -> None:
    broken = SAMPLE.replace("| off | 未启动 | booting |", "| off | 未启动 |")
    with pytest.raises(DocumentTableError, match="列数"):
        environment_table(broken)
