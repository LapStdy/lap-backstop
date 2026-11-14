"""worker 默认命令选择用例：源码模块入口、冻结 exe 子命令与冻结分支注入。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from core.plugins.channel import WorkerChannel, _worker_launch
from core.plugins.worker_options import WorkerConfig


def test_worker_launch_source_uses_module_entry() -> None:
    """源码运行的默认命令是 -m 模块入口，工作目录是仓库根。"""
    command, cwd = _worker_launch(False)
    assert command == [sys.executable, "-u", "-m", "core.plugins.worker"]
    assert cwd == Path(__file__).resolve().parents[2]


def test_worker_launch_frozen_uses_exe_subcommand() -> None:
    """打包件的默认命令是 exe 自身的 worker 子命令，工作目录是 exe 所在目录。"""
    command, cwd = _worker_launch(True)
    assert command == [sys.executable, "worker"]
    assert cwd == Path(sys.executable).resolve().parent


def test_channel_picks_launch_branch_by_frozen_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """默认命令按 sys.frozen 取分支；注入假冻结环境后应走 exe 回调形态。"""
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    channel = WorkerChannel("fake", WorkerConfig(plugins_root=tmp_path))
    assert channel._command == [sys.executable, "worker"]
    assert channel._cwd == Path(sys.executable).resolve().parent
