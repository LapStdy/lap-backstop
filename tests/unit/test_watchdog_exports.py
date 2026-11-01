"""看门狗对外入口契约：__all__ 恰为既定名单、逐个可导入、serve 与入口引用路径未变。

core.hostops.watchdog 是看门狗家族的唯一对外入口名；serve 的停手补报与
命令行的 watchdog check 都经这个名字取用，拆分家族模块不得改变引用路径。
"""

from __future__ import annotations

import importlib
import inspect
from typing import Any

from core import __main__ as core_main
from core.hostops import serve, watchdog

# core.hostops.watchdog.__all__ 的既定名单：公开面只许原样，不得增减改名。
EXPECTED_EXPORTS = [
    "ACTION_GAVE_UP",
    "ACTION_NONE",
    "ACTION_START_REQUESTED",
    "CORE_TASK_NAME",
    "HEARTBEAT_FILENAME",
    "LOCK_FILENAME",
    "LOG_FILENAME",
    "STATE_FILENAME",
    "WATCHDOG_GAVE_UP_EVENT",
    "WATCHDOG_MUTEX_NAME",
    "CheckInputs",
    "CheckPolicy",
    "CheckReport",
    "GaveUpReport",
    "WatchdogState",
    "is_pid_alive",
    "read_state",
    "run_check",
    "take_gave_up_event_payload",
    "write_state",
]


def test_all_is_exactly_the_established_list() -> None:
    assert watchdog.__all__ == EXPECTED_EXPORTS


def test_star_import_exposes_exactly_the_established_list() -> None:
    namespace: dict[str, Any] = {}
    exec("from core.hostops.watchdog import *", namespace)
    namespace.pop("__builtins__", None)
    assert set(namespace) == set(EXPECTED_EXPORTS)


def test_each_export_is_importable_from_entry_module() -> None:
    module = importlib.import_module("core.hostops.watchdog")
    for name in EXPECTED_EXPORTS:
        getattr(module, name)


def test_serve_claims_gave_up_payload_via_watchdog_module() -> None:
    source = inspect.getsource(serve._host_setup_events)
    assert "watchdog.take_gave_up_event_payload" in source
    assert "watchdog.STATE_FILENAME" in source


def test_serve_reexports_gave_up_event_from_watchdog() -> None:
    assert serve.WATCHDOG_GAVE_UP_EVENT == watchdog.WATCHDOG_GAVE_UP_EVENT


def test_main_entry_imports_run_check_from_watchdog() -> None:
    source = inspect.getsource(core_main._run_watchdog_check)
    assert "from core.hostops.watchdog import run_check" in source
