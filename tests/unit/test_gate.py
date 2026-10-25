"""gate 命令清单用例：契约检查并入统一门禁。

判据是 docs/12 门禁一节的命令清单：七条命令按文档给出的顺序合并进
commands()，其中 check_contracts 检查 docs 对契约的重复定义，排在
check_artifacts 之前；命令里出现的仓库脚本必须真实存在，路径笔误会让
本地与 CI 跑出两套口径。
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "gate.py"


def _load_gate() -> Any:
    spec = importlib.util.spec_from_file_location("gate_under_test", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # importlib 加载的模块先注册进 sys.modules，避免机制按名回查时找不到。
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_gate()

CONTRACT_CHECK = [sys.executable, "scripts/check_contracts.py"]
ARTIFACT_CHECK = [sys.executable, "scripts/check_artifacts.py", "."]


def test_commands_run_seven_checks() -> None:
    assert len(gate.commands()) == 7


def test_contract_check_runs_before_artifact_check() -> None:
    commands = gate.commands()
    assert CONTRACT_CHECK in commands
    assert commands.index(CONTRACT_CHECK) < commands.index(ARTIFACT_CHECK)


def test_commands_reference_existing_repo_scripts() -> None:
    for command in gate.commands():
        for part in command:
            if part.startswith("scripts/"):
                assert (_REPO_ROOT / part).is_file(), part
