"""数据根解析：环境变量钉盘优先、两种运行形态的默认回退、非法值拒绝。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from core import __main__ as core_main

FROZEN_EXE = "D:\\lap-backstop\\app\\releases\\1.0.0\\lap-backstop.exe"


def test_env_var_pins_data_root_outside_install_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(core_main.DATA_ROOT_ENV_VAR, str(tmp_path / "pinned"))
    monkeypatch.delattr(sys, "frozen", raising=False)

    assert core_main.resolve_data_root() == tmp_path / "pinned"


def test_env_var_beats_frozen_install_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(core_main.DATA_ROOT_ENV_VAR, "D:\\backstop-data")
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", FROZEN_EXE)

    assert core_main.resolve_data_root() == Path("D:\\backstop-data")


def test_default_source_mode_uses_repo_data_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(core_main.DATA_ROOT_ENV_VAR, raising=False)
    monkeypatch.delattr(sys, "frozen", raising=False)

    repo_root = Path(core_main.__file__).resolve().parents[1]
    assert core_main.resolve_data_root() == repo_root / "data"


def test_default_frozen_mode_uses_install_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(core_main.DATA_ROOT_ENV_VAR, raising=False)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", FROZEN_EXE)

    assert core_main.resolve_data_root() == Path("D:\\lap-backstop\\data")


@pytest.mark.parametrize("value", ["data", ".\\data", ""])
def test_relative_env_var_is_rejected(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(core_main.DATA_ROOT_ENV_VAR, value)

    with pytest.raises(ValueError, match=core_main.DATA_ROOT_ENV_VAR):
        core_main.resolve_data_root()


def test_main_reports_bad_env_var_instead_of_traceback(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(core_main.DATA_ROOT_ENV_VAR, "relative-data")

    assert core_main.main(["doctor"]) == core_main.EXIT_ERROR
    captured = capsys.readouterr()
    assert "启动参数无效" in captured.err
    assert core_main.DATA_ROOT_ENV_VAR in captured.err
