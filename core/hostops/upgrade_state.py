r"""升级的落盘状态：回滚凭据、结果事件与领域异常。

回滚凭据记在 data\run\upgrade.state.json（目标版本、前一版本、快照文件名）：
标记存在表示一次升级尚无结论，清除后重复回滚即空操作。结果事件
host.upgrade_finished 与 host.upgrade_failed 的载荷版本身份由 UpgradeIdentity
固定；事件通道故障只提示，不掩盖命令退出码。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from contracts import EventType
from storage import open_database
from storage.repositories.events import EventLog

UPGRADE_FINISHED_EVENT = "host.upgrade_finished"
UPGRADE_FAILED_EVENT = "host.upgrade_failed"
STATE_FILENAME = "upgrade.state.json"


class UpgradeError(RuntimeError):
    """升级或回滚的预期失败；消息是人话，可直接进事件 detail 与命令输出。"""


@dataclass(frozen=True, slots=True)
class UpgradeMarker:
    """一次尚无结论的升级的回滚凭据（data\\run\\upgrade.state.json）。"""

    target_version: str
    previous_version: str
    snapshot_name: str


@dataclass(frozen=True, slots=True)
class UpgradeIdentity:
    """升级结果事件携带的版本身份。"""

    target_version: str
    previous_version: str


def marker_path(data_root: Path) -> Path:
    """回滚凭据文件路径。"""
    return data_root / "run" / STATE_FILENAME


def read_marker(path: Path) -> UpgradeMarker | None:
    """读回滚凭据；缺失、损坏或字段缺失一律按无标记处理（人工删除即复位）。"""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    target = data.get("target_version")
    previous = data.get("previous_version")
    snapshot = data.get("snapshot_name")
    if not (isinstance(target, str) and isinstance(previous, str) and isinstance(snapshot, str)):
        return None
    return UpgradeMarker(target_version=target, previous_version=previous, snapshot_name=snapshot)


def write_marker(path: Path, marker: UpgradeMarker) -> None:
    """原子写回滚凭据：切换联接前落盘，任何时刻崩溃都能凭它回到前一版本。

    Raises:
        OSError: 目录不可写；由调用方按升级失败收场。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    payload = {
        "target_version": marker.target_version,
        "previous_version": marker.previous_version,
        "snapshot_name": marker.snapshot_name,
    }
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    os.replace(temporary, path)


def clear_marker(path: Path) -> None:
    """清除回滚凭据：升级已有结论（成功或已回滚），从此重复回滚即空操作。"""
    path.unlink(missing_ok=True)


def record_finished(
    database: Path, identity: UpgradeIdentity, *, detail: str, at: datetime
) -> None:
    """升级成功事件：detail 汇总快照与保留清理结果。"""
    _record_event(
        database,
        UPGRADE_FINISHED_EVENT,
        {"detail": detail, "ok": True, **_identity_payload(identity)},
        at=at,
    )


def record_failed(
    database: Path, identity: UpgradeIdentity, *, detail: str, is_healthy: bool, at: datetime
) -> None:
    """升级失败事件：ok 表示服务是否恢复到旧版本并就绪，供通知分支判断。"""
    _record_event(
        database,
        UPGRADE_FAILED_EVENT,
        {"detail": detail, "ok": is_healthy, **_identity_payload(identity)},
        at=at,
    )


def _identity_payload(identity: UpgradeIdentity) -> dict[str, str]:
    """事件载荷里的版本字段。"""
    return {
        "target_version": identity.target_version,
        "previous_version": identity.previous_version,
    }


def _record_event(database: Path, event: str, payload: dict[str, object], *, at: datetime) -> None:
    """把升级结果写进全局事件；事件通道故障只提示，不掩盖命令退出码。"""
    try:
        connection = open_database(database)
    except sqlite3.Error as error:
        print(f"升级结果事件未写入：{error}", file=sys.stderr)
        return
    try:
        EventLog(connection).append_global(EventType.HOST_EVENT, {"event": event, **payload}, at=at)
    except sqlite3.Error as error:
        print(f"升级结果事件未写入：{error}", file=sys.stderr)
    finally:
        connection.close()


__all__ = [
    "STATE_FILENAME",
    "UPGRADE_FAILED_EVENT",
    "UPGRADE_FINISHED_EVENT",
    "UpgradeError",
    "UpgradeIdentity",
    "UpgradeMarker",
    "clear_marker",
    "marker_path",
    "read_marker",
    "record_failed",
    "record_finished",
    "write_marker",
]
