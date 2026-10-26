"""MAA 配置工具的安全检查：MAA 在跑或查询不可用时不允许写配置。"""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "maa_config.py"


def _load_tool() -> Any:
    spec = importlib.util.spec_from_file_location("maa_config_under_test", _SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


maa_config = _load_tool()


def _completed(stdout: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], 0, stdout, "")


def test_running_maa_is_reported(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed('"MAA.exe","123"\n'))
    assert maa_config.maa_running(tmp_path) is True


def test_unavailable_tasklist_refuses_conservatively(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """tasklist 不可用时按「在跑」处置：写配置的前提不能被工具故障绕过。"""

    def missing(*args: object, **kwargs: object) -> object:
        raise FileNotFoundError("tasklist")

    monkeypatch.setattr(subprocess, "run", missing)
    assert maa_config.maa_running(tmp_path) is True


def test_tasklist_timeout_refuses_conservatively(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def slow(*args: object, **kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd="tasklist", timeout=15)

    monkeypatch.setattr(subprocess, "run", slow)
    assert maa_config.maa_running(tmp_path) is True


def test_absent_maa_allows_write(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _completed("信息: 没有运行的任务\n"))
    assert maa_config.maa_running(tmp_path) is False
