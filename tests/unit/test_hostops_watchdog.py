"""看门狗：判死三条件、误报豁免、跨次验收与停手、记录与状态文件。"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from core.clock import VirtualClock
from core.hostops import watchdog
from tests.fixtures.subprocess_fakes import fake_runner

BASE = datetime(2026, 10, 25, 8, 0, tzinfo=UTC)
POLICY = watchdog.CheckPolicy(
    stale_seconds=300.0,
    acceptance_seconds=120.0,
    give_up_window=timedelta(minutes=30),
    give_up_max_failures=3,
)


def write_heartbeat(run_dir: Path, *, pid: int, age_seconds: float) -> None:
    """写心跳并用定值 mtime 固定陈旧度；mtime 基准是注入时钟的起点，不读系统时间。"""
    path = run_dir / watchdog.HEARTBEAT_FILENAME
    payload = {
        "pid": pid,
        "started_at": (BASE - timedelta(hours=1)).isoformat(),
        "version": "0.1.0",
        "last_tick": BASE.isoformat(),
        "active_run": None,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    stamp = BASE.timestamp() - age_seconds
    os.utime(path, (stamp, stamp))


def write_lock(run_dir: Path, *, pid: int, started_at: str) -> None:
    path = run_dir / watchdog.LOCK_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"pid": pid, "started_at": started_at, "exe_path": "x", "boot_id": ""}),
        encoding="utf-8",
    )


def last_log_entry(data_root: Path) -> dict[str, object]:
    lines = (data_root / "logs" / watchdog.LOG_FILENAME).read_text(encoding="utf-8").splitlines()
    return dict(json.loads(lines[-1]))


def state_of(data_root: Path) -> watchdog.WatchdogState:
    return watchdog.read_state(data_root / "run" / watchdog.STATE_FILENAME)


def check(
    data_root: Path,
    *,
    now: datetime,
    pid_alive: bool,
    returncode: int = 0,
) -> list[list[str]]:
    runner, calls = fake_runner(returncode)
    code = watchdog.run_check(
        data_root,
        inputs=watchdog.CheckInputs(
            now=now,
            policy=POLICY,
            runner=runner,
            pid_alive=lambda _pid: pid_alive,
        ),
    )
    assert code == 0
    return calls


def test_alive_heartbeat_takes_no_action(tmp_path: Path) -> None:
    write_heartbeat(tmp_path / "run", pid=101, age_seconds=10)
    calls = check(tmp_path, now=BASE, pid_alive=True)
    assert calls == []
    assert state_of(tmp_path)["pending_start"] is None
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_NONE


def test_dead_by_missing_heartbeat_requests_start(tmp_path: Path) -> None:
    calls = check(tmp_path, now=BASE, pid_alive=True)
    assert calls == [["schtasks", "/run", "/tn", watchdog.CORE_TASK_NAME]]
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_START_REQUESTED
    assert state_of(tmp_path)["pending_start"] == BASE.isoformat()


def test_dead_by_stale_mtime_requests_start(tmp_path: Path) -> None:
    write_heartbeat(tmp_path / "run", pid=101, age_seconds=301)
    calls = check(tmp_path, now=BASE, pid_alive=True)
    assert len(calls) == 1
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_START_REQUESTED


def test_dead_by_dead_pid_requests_start(tmp_path: Path) -> None:
    write_heartbeat(tmp_path / "run", pid=101, age_seconds=10)
    calls = check(tmp_path, now=BASE, pid_alive=False)
    assert len(calls) == 1
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_START_REQUESTED


def test_start_request_failure_leaves_no_pending(tmp_path: Path) -> None:
    check(tmp_path, now=BASE, pid_alive=False, returncode=1)
    assert state_of(tmp_path)["pending_start"] is None
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_NONE


def test_lock_exemption_cancels_false_positive(tmp_path: Path) -> None:
    # mtime 超限误判死亡，但锁描述的进程仍活着：豁免，不拉起也不计失败
    run_dir = tmp_path / "run"
    write_heartbeat(run_dir, pid=101, age_seconds=400)
    write_lock(run_dir, pid=101, started_at=(BASE - timedelta(hours=1)).isoformat())
    calls = check(tmp_path, now=BASE, pid_alive=True)
    assert calls == []
    assert state_of(tmp_path)["failures"] == []
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_NONE


def test_lock_pid_reuse_breaks_exemption(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # pid 活着但创建时间对不上 started_at：pid 已被复用，豁免不成立
    monkeypatch.setattr(watchdog, "_process_creation_time", lambda _pid: BASE + timedelta(hours=2))
    run_dir = tmp_path / "run"
    write_heartbeat(run_dir, pid=101, age_seconds=400)
    write_lock(run_dir, pid=101, started_at=(BASE - timedelta(hours=1)).isoformat())
    calls = check(tmp_path, now=BASE, pid_alive=True)
    assert len(calls) == 1


def test_acceptance_failures_accumulate_then_give_up(tmp_path: Path) -> None:
    # 拉起后从未出现新心跳：每轮验收失败一次，窗口内第 3 次停手并留待上报标记
    run_dir = tmp_path / "run"
    write_heartbeat(run_dir, pid=101, age_seconds=600)
    clock = VirtualClock(BASE)
    for _ in range(4):
        clock.advance(timedelta(seconds=150))
        check(tmp_path, now=clock.utc_now(), pid_alive=False)
    state = state_of(tmp_path)
    assert state["gave_up_pending_report"] is True
    assert len(state["failures"]) == 3
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_GAVE_UP
    detail = watchdog.claim_gave_up_report(run_dir / watchdog.STATE_FILENAME)
    assert detail is not None
    assert "3 次验收失败" in detail
    # 补报是取走式：第二次没有可报的
    assert watchdog.claim_gave_up_report(run_dir / watchdog.STATE_FILENAME) is None


def test_failures_outside_window_are_dropped_and_watchdog_resumes(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    stale = [(BASE - timedelta(minutes=31)).isoformat()] * 3
    watchdog.write_state(run_dir / watchdog.STATE_FILENAME, {"failures": stale})
    write_heartbeat(run_dir, pid=101, age_seconds=400)
    calls = check(tmp_path, now=BASE, pid_alive=False)
    assert len(calls) == 1
    assert last_log_entry(tmp_path)["action"] == watchdog.ACTION_START_REQUESTED
    assert state_of(tmp_path)["pending_start"] == BASE.isoformat()


def test_corrupt_state_is_treated_as_fresh(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True)
    (run_dir / watchdog.STATE_FILENAME).write_text("not json", encoding="utf-8")
    check(tmp_path, now=BASE, pid_alive=False)
    assert state_of(tmp_path)["pending_start"] == BASE.isoformat()


def test_log_line_has_documented_fields(tmp_path: Path) -> None:
    write_heartbeat(tmp_path / "run", pid=77, age_seconds=10)
    check(tmp_path, now=BASE, pid_alive=True)
    entry = last_log_entry(tmp_path)
    assert set(entry) == {"at", "heartbeat_age_s", "core_pid", "action"}
    assert entry["core_pid"] == 77
    assert entry["at"] == BASE.isoformat()


def test_pid_alive_helper_uses_real_processes() -> None:
    assert watchdog.is_pid_alive(os.getpid()) is True
    assert watchdog.is_pid_alive(4_000_000) is False


def test_watchdog_mutex_second_acquire_returns_none() -> None:
    handle = watchdog._acquire_watchdog_mutex(watchdog.WATCHDOG_MUTEX_NAME)
    assert handle is not None
    try:
        assert watchdog._acquire_watchdog_mutex(watchdog.WATCHDOG_MUTEX_NAME) is None
    finally:
        watchdog._kernel32.CloseHandle(handle)
