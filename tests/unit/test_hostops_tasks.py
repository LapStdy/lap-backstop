"""计划任务：XML 字段逐项断言、生成确定性、注册命令与失败路径。"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.hostops import tasks
from tests.fixtures.subprocess_fakes import fake_runner

REGISTERED_AT = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
TASK_NS = "{http://schemas.microsoft.com/windows/2004/02/mit/task}"
CORE_ACTION = tasks.TaskAction("C:\\app\\current\\lap-backstop.exe", "serve")


def test_core_task_xml_matches_documented_fields() -> None:
    xml = tasks.core_task_xml(CORE_ACTION)
    assert "<LogonTrigger>" in xml
    assert "<Delay>PT30S</Delay>" in xml
    assert "<Command>C:\\app\\current\\lap-backstop.exe</Command>" in xml
    assert "<Arguments>serve</Arguments>" in xml
    # 实例策略不启动新实例；不限运行时长；不看电源条件
    assert "<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>" in xml
    assert "<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>" in xml
    assert "<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>" in xml
    assert "<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>" in xml
    # 失败后每分钟重试、最多 3 次
    assert "<RestartOnFailure>" in xml
    assert "<Interval>PT1M</Interval>" in xml
    assert "<Count>3</Count>" in xml


def test_watchdog_task_xml_repeats_every_five_minutes() -> None:
    xml = tasks.watchdog_task_xml(
        tasks.TaskAction("C:\\app\\current\\lap-backstop.exe", "watchdog check"),
        registered_at=REGISTERED_AT,
    )
    assert "<LogonTrigger />" in xml
    assert "<TimeTrigger>" in xml
    assert "<StartBoundary>2026-10-25T12:00:00</StartBoundary>" in xml
    assert "<Interval>PT5M</Interval>" in xml
    assert "<Arguments>watchdog check</Arguments>" in xml
    # 短命进程不配失败重启：下一次计划触发就是兜底
    assert "RestartOnFailure" not in xml


def test_backup_task_xml_runs_daily_four_and_wakes_computer() -> None:
    xml = tasks.backup_task_xml(
        tasks.TaskAction("C:\\app\\current\\lap-backstop.exe", "backup"),
        registered_at=REGISTERED_AT,
    )
    assert "<CalendarTrigger>" in xml
    assert "T04:00:00</StartBoundary>" in xml
    assert "<DaysInterval>1</DaysInterval>" in xml
    assert "<WakeToRun>true</WakeToRun>" in xml


def test_xml_generation_is_deterministic_and_escapes_input() -> None:
    action = tasks.TaskAction("C:\\tools\\a&b.exe", "serve & watch <core>")
    first = tasks.core_task_xml(action)
    second = tasks.core_task_xml(action)
    assert first == second
    assert "<Command>C:\\tools\\a&amp;b.exe</Command>" in first
    assert "<Arguments>serve &amp; watch &lt;core&gt;</Arguments>" in first


def test_write_definitions_roundtrip_parses_utf16(tmp_path: Path) -> None:
    actions = {
        tasks.TASK_CORE: CORE_ACTION,
        tasks.TASK_WATCHDOG: tasks.TaskAction("exe", "watchdog check"),
        tasks.TASK_BACKUP: tasks.TaskAction("exe", "backup"),
    }
    written = tasks.write_definitions(tmp_path, actions, registered_at=REGISTERED_AT)
    assert set(written) == set(actions)
    for name, path in written.items():
        root = ET.fromstring(path.read_text(encoding="utf-16"))
        uri = root.find(f"{TASK_NS}RegistrationInfo/{TASK_NS}URI")
        assert uri is not None
        assert uri.text == f"\\{name}"


def test_write_definitions_rejects_unknown_task(tmp_path: Path) -> None:
    with pytest.raises(KeyError):
        tasks.write_definitions(tmp_path, {"other": CORE_ACTION}, registered_at=REGISTERED_AT)


def test_register_task_passes_schtasks_arguments(tmp_path: Path) -> None:
    runner, calls = fake_runner()
    xml_path = tmp_path / "definition.xml"
    xml_path.write_text("<Task />", encoding="utf-16")
    assert tasks.register_task("lap-backstop-core", xml_path, runner=runner) is True
    assert calls == [
        ["schtasks", "/create", "/f", "/tn", "lap-backstop-core", "/xml", str(xml_path)]
    ]
    failing, _ = fake_runner(returncode=1)
    assert tasks.register_task("lap-backstop-core", xml_path, runner=failing) is False


def test_startup_actions_cover_frozen_and_source_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delattr(sys, "frozen", raising=False)
    dev = tasks.startup_actions(tmp_path)
    assert dev[tasks.TASK_CORE].command == sys.executable
    assert "--data-root" in dev[tasks.TASK_CORE].arguments
    assert "watchdog check" in dev[tasks.TASK_WATCHDOG].arguments
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", "C:\\app\\current\\lap-backstop.exe")
    frozen = tasks.startup_actions(tmp_path)
    assert frozen[tasks.TASK_CORE] == tasks.TaskAction(
        "C:\\app\\current\\lap-backstop.exe", "serve"
    )
    assert frozen[tasks.TASK_WATCHDOG].arguments == "watchdog check"


def test_register_registered_tasks_reports_outcome(tmp_path: Path) -> None:
    runner, calls = fake_runner()
    ok, detail = tasks.register_registered_tasks(
        tmp_path, runner=runner, registered_at=REGISTERED_AT
    )
    assert ok is True
    assert tasks.TASK_CORE in detail
    assert tasks.TASK_WATCHDOG in detail
    # P0 只注册两个动作型任务，且先写定义文件再注册
    assert len(calls) == 2
    tasks_dir = tmp_path / tasks.TASKS_DIRNAME
    assert (tasks_dir / f"{tasks.TASK_CORE}.xml").is_file()
    failing, _ = fake_runner(returncode=1)
    ok, detail = tasks.register_registered_tasks(
        tmp_path, runner=failing, registered_at=REGISTERED_AT
    )
    assert ok is False
    assert "注册失败" in detail
