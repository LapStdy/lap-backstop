"""启动校时：w32tm 假执行器、偏移解析与补跑暂停决策（docs/08 时钟与 NTP）。"""

from __future__ import annotations

import sqlite3
import subprocess
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from core.api.store import all_settings, put_settings
from core.hostops import timesync
from core.hostops.timesync import (
    CATCH_UP_PAUSED_KEY,
    SKEW_EVENT,
    TIMESYNC_EVENT,
    ClockCheck,
    apply_startup_check,
    catch_up_paused,
    parse_offset_seconds,
    startup_check,
)
from storage import migrate, open_database

T0 = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)

Runner = Callable[..., subprocess.CompletedProcess[bytes]]


def completed(
    returncode: int,
    stdout: bytes = b"",
    stderr: bytes = b"",
) -> subprocess.CompletedProcess[bytes]:
    """构造一个已结束的命令结果。"""
    return subprocess.CompletedProcess(["w32tm"], returncode, stdout, stderr)


def fake_runner(
    resync: object,
    stripchart: object,
) -> tuple[Runner, list[list[str]]]:
    """按命令分派的假执行器：记录调用，结果或异常按命令取用。"""
    calls: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(list(command))
        outcome = resync if command == list(timesync.SYNC_COMMAND) else stripchart
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, subprocess.CompletedProcess)
        return outcome

    return run, calls


def test_parse_offset_seconds_reads_last_sample_line() -> None:
    output = "16:02:35, +00.1234567s\n16:02:36, +31.5s\n"
    assert parse_offset_seconds(output) == 31.5


def test_parse_offset_seconds_accepts_negative_and_bare_seconds() -> None:
    assert parse_offset_seconds("16:02:35, -2s") == -2.0


def test_parse_offset_seconds_returns_none_without_offset_line() -> None:
    assert parse_offset_seconds("16:02:35, error 0x800705B4") is None


def test_startup_check_syncs_and_measures_offset() -> None:
    runner, calls = fake_runner(
        completed(0),
        completed(0, b"16:02:36, +29.0s\n"),
    )
    check = startup_check(runner)
    assert check.sync_ok is True
    assert check.sync_detail == "已执行 w32tm /resync"
    assert check.offset_seconds == 29.0
    assert calls == [list(timesync.SYNC_COMMAND), list(timesync.OFFSET_COMMAND)]


def test_startup_check_resync_failure_still_measures_offset() -> None:
    """校时失败也测偏移：stripchart 能测到 40 秒就照记，不因失败放弃结论。"""
    runner, _calls = fake_runner(
        completed(1, stderr=b"The following error occurred: Access is denied.\n"),
        completed(0, b"16:02:36, +40.0s\n"),
    )
    check = startup_check(runner)
    assert check.sync_ok is False
    assert "退出码 1" in check.sync_detail
    assert "Access is denied" in check.sync_detail
    assert check.offset_seconds == 40.0


def test_startup_check_missing_command_reports_failure_without_offset() -> None:
    runner, _calls = fake_runner(FileNotFoundError(), FileNotFoundError())
    check = startup_check(runner)
    assert check.sync_ok is False
    assert "找不到 w32tm" in check.sync_detail
    assert check.offset_seconds is None


def test_startup_check_timeout_reports_failure_without_offset() -> None:
    expired = subprocess.TimeoutExpired(cmd=["w32tm"], timeout=10.0)
    runner, _calls = fake_runner(expired, expired)
    check = startup_check(runner)
    assert check.sync_ok is False
    assert "超时" in check.sync_detail
    assert check.offset_seconds is None


@pytest.fixture
def database(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """带 settings 表的临时库。"""
    connection = open_database(tmp_path / "state.db")
    migrate(connection)
    yield connection
    connection.close()


def test_apply_startup_check_offset_under_threshold_keeps_catch_up_running(
    database: sqlite3.Connection,
) -> None:
    events = apply_startup_check(database, ClockCheck(True, "已执行 w32tm /resync", 29.0), at=T0)
    assert [name for name, _payload in events] == [TIMESYNC_EVENT]
    assert events[0][1]["ok"] is True
    assert CATCH_UP_PAUSED_KEY not in all_settings(database)


def test_apply_startup_check_offset_over_threshold_pauses_catch_up(
    database: sqlite3.Connection,
) -> None:
    events = apply_startup_check(database, ClockCheck(True, "已执行 w32tm /resync", 31.0), at=T0)
    assert [name for name, _payload in events] == [SKEW_EVENT, TIMESYNC_EVENT]
    assert events[0][1]["offset_seconds"] == 31.0
    assert "补跑已暂停" in str(events[0][1]["detail"])
    assert all_settings(database)[CATCH_UP_PAUSED_KEY] is True


def test_apply_startup_check_offset_over_threshold_keeps_pause_set(
    database: sqlite3.Connection,
) -> None:
    apply_startup_check(database, ClockCheck(True, "已执行 w32tm /resync", 31.0), at=T0)
    again = apply_startup_check(database, ClockCheck(True, "已执行 w32tm /resync", 45.0), at=T0)
    assert again[0][0] == SKEW_EVENT
    assert all_settings(database)[CATCH_UP_PAUSED_KEY] is True


def test_apply_startup_check_offset_back_in_range_resumes_catch_up(
    database: sqlite3.Connection,
) -> None:
    apply_startup_check(database, ClockCheck(True, "已执行 w32tm /resync", 31.0), at=T0)
    events = apply_startup_check(database, ClockCheck(True, "已执行 w32tm /resync", 29.0), at=T0)
    assert [name for name, _payload in events] == [TIMESYNC_EVENT]
    assert "补跑暂停已解除" in str(events[0][1]["detail"])
    assert all_settings(database)[CATCH_UP_PAUSED_KEY] is False


def test_apply_startup_check_offset_unknown_keeps_pause_state(
    database: sqlite3.Connection,
) -> None:
    """偏移不可测时不下结论：已暂停的保持暂停，未暂停的不启动暂停。"""
    apply_startup_check(database, ClockCheck(True, "已执行 w32tm /resync", 31.0), at=T0)
    events = apply_startup_check(database, ClockCheck(False, "校时失败：超时", None), at=T0)
    assert [name for name, _payload in events] == [TIMESYNC_EVENT]
    assert all_settings(database)[CATCH_UP_PAUSED_KEY] is True


def test_catch_up_paused_ignores_corrupt_value(database: sqlite3.Connection) -> None:
    put_settings(database, {CATCH_UP_PAUSED_KEY: "yes"}, at=T0)
    assert catch_up_paused(database) is False
