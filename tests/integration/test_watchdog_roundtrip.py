"""看门狗子进程轮回：判活、停手与互斥量，经 python -m core watchdog check。

拉起场景（schtasks /run 成功路径）不进集成：真宿主上任务存在时会真拉起核心，
CI 上又不可控，该路径由单元测试的假 runner 覆盖。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from core.hostops.heartbeat import write_heartbeat
from tests.integration.test_serve_shell import REPO_ROOT, WAIT_SECONDS

DEAD_PID = 4_000_000


def child_env() -> dict[str, str]:
    return {**os.environ, "PYTHONUTF8": "1"}


def run_watchdog(data_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "core",
            "watchdog",
            "check",
            "--data-root",
            str(data_root),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=WAIT_SECONDS,
        env=child_env(),
    )


def write_files(
    run_dir: Path,
    *,
    heartbeat_pid: int,
    heartbeat_age: float,
    lock_pid: int | None,
    failures: list[str],
) -> None:
    """落齐心跳、锁与看门狗状态；mtime 与失败时刻都锚定真实墙钟。"""
    run_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    stamp = time.time() - heartbeat_age
    write_heartbeat(
        run_dir / "core.heartbeat",
        {
            "pid": heartbeat_pid,
            "started_at": now.isoformat(),
            "version": "0.1.0",
            "last_tick": now.isoformat(),
            "active_run": None,
        },
    )
    os.utime(run_dir / "core.heartbeat", (stamp, stamp))
    if lock_pid is not None:
        (run_dir / "core.lock").write_text(
            json.dumps(
                {
                    "pid": lock_pid,
                    "started_at": now.isoformat(),
                    "exe_path": "x",
                    "boot_id": "",
                }
            ),
            encoding="utf-8",
        )
    (run_dir / "watchdog.state.json").write_text(
        json.dumps({"pending_start": None, "failures": failures, "gave_up_pending_report": False}),
        encoding="utf-8",
    )


def last_log_entry(data_root: Path) -> dict[str, object]:
    lines = (data_root / "logs" / "watchdog.jsonl").read_text(encoding="utf-8").splitlines()
    return dict(json.loads(lines[-1]))


def test_alive_heartbeat_exits_zero_without_action(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    write_files(
        data_root / "run",
        heartbeat_pid=os.getpid(),
        heartbeat_age=5,
        lock_pid=os.getpid(),
        failures=[],
    )
    result = run_watchdog(data_root)
    assert result.returncode == 0
    entry = last_log_entry(data_root)
    assert entry["action"] == "none"
    assert entry["core_pid"] == os.getpid()


def test_three_window_failures_lead_to_give_up_without_external_start(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    now = datetime.now(UTC)
    failures = [(now - timedelta(minutes=m)).isoformat() for m in (3, 2, 1)]
    write_files(
        data_root / "run",
        heartbeat_pid=DEAD_PID,
        heartbeat_age=5,
        lock_pid=DEAD_PID,
        failures=failures,
    )
    result = run_watchdog(data_root)
    assert result.returncode == 0
    # 停手路径零外部调用：不会真的执行 schtasks /run
    assert last_log_entry(data_root)["action"] == "gave_up"
    state = json.loads((data_root / "run" / "watchdog.state.json").read_text(encoding="utf-8"))
    assert state["gave_up_pending_report"] is True


def test_second_watchdog_instance_exits_zero_without_checking(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    holder_code = (
        "import time\n"
        "from core.hostops import instance, watchdog\n"
        "handle = instance.acquire_named_mutex(watchdog.WATCHDOG_MUTEX_NAME)\n"
        "assert handle is not None\n"
        "print('held', flush=True)\n"
        "time.sleep(60)\n"
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_code],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        text=True,
        env=child_env(),
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "held"
        result = run_watchdog(data_root)
        assert result.returncode == 0
        # 第二个实例没有做检查，连日志目录都不该出现
        assert not (data_root / "logs").exists()
    finally:
        holder.terminate()
        holder.wait(timeout=WAIT_SECONDS)
