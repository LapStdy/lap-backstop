"""check_contracts 检查器用例：注册表解析、判形、豁免与入口行为。

判据是各文档的真源声明：事件与错误码清单只在 docs/03 的「SSE 事件」「错误码」
维护，能力签名与契约类型字段只在 docs/05 与 contracts/ 代码维护，docs/02 与
docs/09 只引用。用例分三层：注册表与 contracts 代码逐集相等（解析器不脱离
代码真源）；合成文档逐形态断言（枚举算违规、句子点名单个名目算引用）；
真实三份文档经 main 应零违规（当前文档只引用，检查器不得误报）。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

from contracts.errors import ErrorCode
from contracts.events import EventType

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "check_contracts.py"


def _load_checker() -> Any:
    spec = importlib.util.spec_from_file_location("check_contracts_under_test", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclass 装饰器按 __module__ 回查 sys.modules，先注册再执行。
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


cc = _load_checker()


@pytest.fixture()
def reg() -> Any:
    return cc.Registry(
        events=frozenset({"run.created", "run.state.changed", "heartbeat"}),
        error_codes=frozenset({"E_CONFIG", "E_DEADLINE"}),
        capabilities=frozenset({"ScriptAdapter"}),
        methods=frozenset({"launch", "verify"}),
        type_names=frozenset({"ScriptAdapter", "RunContext", "Verdict"}),
        fields={
            "RunContext": frozenset({"run_id", "attempt", "template_id"}),
            "Verdict": frozenset({"status", "policy_id"}),
        },
    )


def _rule_lines(doc: str, reg: Any, host: bool = False) -> list[tuple[str, int]]:
    return [(hit.rule, hit.line) for hit in cc.check_text(doc, "docs/02-示例.md", reg, host=host)]


# ---------------------------------------------------------------- 注册表


def test_registry_matches_contract_code() -> None:
    loaded = cc.load_registry(_REPO_ROOT / "contracts")
    assert loaded.events == {event.value for event in EventType}
    assert loaded.error_codes == {code.value for code in ErrorCode}
    assert loaded.capabilities == {
        "EnvironmentProvider",
        "ScriptAdapter",
        "Verifier",
        "ConfigProjection",
        "Notifier",
        "Advisor",
    }
    assert loaded.methods >= {"probe", "prepare", "reset", "cleanup", "warm"}
    assert loaded.methods >= {"launch", "observe", "stop", "recover", "verify"}
    assert loaded.methods >= {"diff", "apply", "rollback", "send", "advise"}
    assert loaded.fields["RunContext"] == frozenset(
        {
            "run_id",
            "attempt",
            "template_id",
            "plugin_id",
            "environment_id",
            "config",
            "work_dir",
            "evidence_dir",
            "deadline_at",
            "cancel",
            "clock",
            "secrets",
            "log",
            "emit",
            "record_evidence",
        }
    )
    assert {"Verdict", "Evidence", "VerificationPolicy", "RunContext"} <= loaded.type_names


def test_registry_rejects_dir_without_enums(tmp_path: Path) -> None:
    (tmp_path / "events.py").write_text("X = 1\n", encoding="utf-8")
    with pytest.raises(cc.ContractScanError):
        cc.load_registry(tmp_path)


# ---------------------------------------------------------------- 事件与错误码清单


def test_event_table_rows_are_flagged(reg: Any) -> None:
    doc = "# 附表\n\n| 事件 | 载荷 |\n|---|---|\n| run.created | run_id |\n| heartbeat | ts |\n"
    assert _rule_lines(doc, reg) == [("event", 5), ("event", 6)]


def test_error_cell_with_multiple_codes_is_flagged(reg: Any) -> None:
    doc = "| 400 | E_CONFIG、E_DEADLINE |\n"
    assert _rule_lines(doc, reg) == [("error", 1), ("error", 1)]


def test_list_items_of_registry_names_are_flagged(reg: Any) -> None:
    doc = "- E_CONFIG\n- E_DEADLINE\n- run.created：运行创建\n"
    assert _rule_lines(doc, reg) == [("error", 1), ("error", 2), ("event", 3)]


def test_prose_mentions_are_references_not_definitions(reg: Any) -> None:
    doc = (
        "每次迁移写 `run.state.changed`，超时记 `E_DEADLINE`，清单见 03。\n"
        "调用 ScriptAdapter.launch 与 Verdict.status 属于行为叙述。\n"
    )
    assert _rule_lines(doc, reg) == []


def test_registry_tables_inside_fence_are_not_flagged(reg: Any) -> None:
    doc = "~~~text\n| run.created | a |\n| E_CONFIG | b |\n~~~\n"
    assert _rule_lines(doc, reg) == []


# ---------------------------------------------------------------- 能力签名


def test_signature_block_is_flagged(reg: Any) -> None:
    doc = "```python\nclass ScriptAdapter(Protocol):\n    def launch(self, script, ctx): ...\n```\n"
    assert _rule_lines(doc, reg) == [("signature", 2), ("signature", 3)]


def test_contract_class_definition_is_flagged(reg: Any) -> None:
    doc = "~~~python\nclass RunContext:\n    run_id: str\n~~~\n"
    assert _rule_lines(doc, reg) == [("signature", 2)]


def test_unknown_protocol_class_is_flagged(reg: Any) -> None:
    assert _rule_lines("class Foo(Protocol):\n", reg) == [("signature", 1)]


def test_capability_method_definition_is_flagged(reg: Any) -> None:
    assert _rule_lines("async def launch(ctx):\n", reg) == [("signature", 1)]
    assert _rule_lines("def verify(self, ctx):\n", reg) == [("signature", 1)]


# ---------------------------------------------------------------- 契约字段


def test_type_table_copy_is_flagged(reg: Any) -> None:
    doc = "| 类型 | 字段 |\n|---|---|\n| Verdict | status、policy_id |\n"
    assert _rule_lines(doc, reg) == [("field", 3)]


def test_field_row_run_of_three_is_flagged_at_start(reg: Any) -> None:
    doc = "| 字段 | 类型 |\n|---|---|\n| run_id | str |\n| attempt | int |\n| template_id | str |\n"
    hits = cc.check_text(doc, "docs/02-示例.md", reg, host=False)
    assert [(hit.rule, hit.line) for hit in hits] == [("field", 3)]
    assert hits[0].text.startswith("连续 3 行以上枚举 RunContext 的字段（run_id 起）")


def test_field_row_run_of_two_is_not_flagged(reg: Any) -> None:
    doc = "| run_id | str |\n| attempt | int |\n"
    assert _rule_lines(doc, reg) == []


def test_field_label_list_run_is_flagged(reg: Any) -> None:
    doc = "- run_id：运行 id\n- attempt：尝试序号\n- template_id：模板\n"
    assert _rule_lines(doc, reg) == [("field", 1)]


# ---------------------------------------------------------------- 宿主豁免


def test_host_doc_registry_enumerations_pass(reg: Any) -> None:
    doc = (
        "| run.created | run_id |\n| E_CONFIG | x |\n- E_DEADLINE\n"
        "| Verdict | a |\n| run_id | b |\n"
    )
    assert _rule_lines(doc, reg, host=True) == []


def test_host_doc_signature_is_still_flagged(reg: Any) -> None:
    assert _rule_lines("def verify(self, ctx):\n", reg, host=True) == [("signature", 1)]


# ---------------------------------------------------------------- 入口


def test_main_on_repo_docs_reports_pass(capsys: pytest.CaptureFixture[str]) -> None:
    assert cc.main([]) == 0
    out = capsys.readouterr().out
    assert "契约检查：PASS" in out
    assert "违规 0" in out


def test_main_on_violating_doc_reports_file_and_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    doc = tmp_path / "02-示例.md"
    doc.write_text("| 事件 |\n|---|---|\n| run.created | a |\n", encoding="utf-8")
    assert cc.main([str(doc)]) == 1
    out = capsys.readouterr().out
    assert f"{doc.as_posix()}:3" in out
    assert "事件清单被复制" in out


def test_main_on_host_named_doc_passes_registry_tables(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    doc = tmp_path / "03-示例.md"
    doc.write_text("| run.created | run_id |\n| E_CONFIG | x |\n", encoding="utf-8")
    assert cc.main([str(doc)]) == 0
    assert "契约检查：PASS" in capsys.readouterr().out


def test_main_on_missing_doc_returns_two(tmp_path: Path) -> None:
    assert cc.main([str(tmp_path / "不存在.md")]) == 2
