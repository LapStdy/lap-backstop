r"""升级与回滚编排：drain → 升级前快照 → 切联接 → 启动 → 60 秒健康检查。

回滚凭据与结果事件见 upgrade_state；版本目录与联接见 releases；对核心进程的
排水、拉起与就绪等待见 core_control。本模块只定顺序与失败收尾：切换前的失败
只恢复服务，不写库不动联接；切换后的失败按标记回滚（联接、库、服务）并写
host.upgrade_failed。回滚对未就绪的新版本进程清场两次：库恢复前后各一次，
覆盖心跳晚到的新版本进程（docs/08 升级与回滚）。
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path

from core.hostops.core_control import (
    UpgradeInputs,
    api_endpoint,
    drain_core,
    launch_and_check,
    restart_and_check,
    running_core_pid,
    terminate_pid,
    unready_core_pid,
)
from core.hostops.releases import (
    UpgradeLayout,
    UpgradePlan,
    layout,
    plan_upgrade,
    prune_releases,
    same_path,
    switch_current,
)
from core.hostops.upgrade_state import (
    UpgradeError,
    UpgradeIdentity,
    UpgradeMarker,
    clear_marker,
    marker_path,
    read_marker,
    record_failed,
    record_finished,
    write_marker,
)
from storage import BackupError, create_pre_upgrade_snapshot, open_database, verify_integrity
from storage.db.backup import TIMESTAMP_FORMAT


@dataclass(frozen=True, slots=True)
class RollbackOutcome:
    """一次回滚或恢复的结论：逐步结果与旧版本是否就绪。"""

    detail: str
    is_healthy: bool


@dataclass(frozen=True, slots=True)
class _Abort:
    """切换前失败的症状：失败原因与核心是否已被排水停住。"""

    reason: str
    is_core_stopped: bool


def run_upgrade(data_root: Path, target: Path, *, inputs: UpgradeInputs | None = None) -> int:
    """升级入口：drain → 升级前快照 → 切联接 → 启动 → 60 秒健康检查。

    排水与快照之前的失败（目录不合法、排水被拒、排水超时）不动系统任何状态，
    核心照常在跑；快照之后到健康检查之间的失败一律自动回滚并写
    host.upgrade_failed。

    Returns:
        进程退出码：0 成功；1 失败（未开始、被中止或已自动回滚）。
    """
    active = UpgradeInputs() if inputs is None else inputs
    app = layout(data_root)
    try:
        plan = plan_upgrade(app, target)
    except UpgradeError as error:
        print(f"升级未开始：{error}", file=sys.stderr)
        return 1
    if same_path(plan.previous_dir, app.releases / plan.target_version):
        print(f"当前版本已是 {plan.target_version}，无需升级")
        return 0
    is_core_stopped = False
    try:
        core_pid = running_core_pid(app.data_root, inputs=active)
        if core_pid is not None:
            endpoint = api_endpoint(app.database)
            drain_core(core_pid, inputs=active, endpoint=endpoint)
            print("核心已排水退出")
            is_core_stopped = True
        snapshot_name = _snapshot_database(app, plan.target_version, inputs=active).name
    except UpgradeError as error:
        abort = _Abort(reason=str(error), is_core_stopped=is_core_stopped)
        return _abort_before_switch(app, plan, inputs=active, abort=abort)
    marker = UpgradeMarker(
        target_version=plan.target_version,
        previous_version=plan.previous_version,
        snapshot_name=snapshot_name,
    )
    try:
        write_marker(marker_path(app.data_root), marker)
        switch_current(app.app_dir, app.releases / plan.target_version)
        launch_and_check(app, inputs=active)
    except (UpgradeError, OSError) as error:
        return _settle_failed_upgrade(app, marker, inputs=active, reason=str(error))
    return _settle_success(app, plan, snapshot_name=snapshot_name, inputs=active)


def run_rollback(data_root: Path, *, inputs: UpgradeInputs | None = None) -> int:
    """手动回滚入口：按标记执行；无标记即空操作，不写事件、不动目录（幂等）。

    Returns:
        进程退出码：0 表示无待回滚升级或回滚后旧版本就绪；1 表示回滚已执行
            但旧版本未在健康检查时限内就绪，需要人工介入。
    """
    active = UpgradeInputs() if inputs is None else inputs
    app = layout(data_root)
    marker = read_marker(marker_path(app.data_root))
    if marker is None:
        print("没有待回滚的升级")
        return 0
    outcome = perform_rollback(app, marker, inputs=active, reason="手动回滚")
    clear_marker(marker_path(app.data_root))
    record_failed(
        app.database,
        _identity_of(marker),
        detail=outcome.detail,
        is_healthy=outcome.is_healthy,
        at=active.clock.utc_now(),
    )
    print(f"回滚完成：{outcome.detail}")
    return 0 if outcome.is_healthy else 1


def perform_rollback(
    app: UpgradeLayout, marker: UpgradeMarker, *, inputs: UpgradeInputs, reason: str
) -> RollbackOutcome:
    """按标记回滚：退场未就绪的新版本 → 联接切回 → 库回快照 → 拉起旧版本。

    新版本进程在库恢复前后各清场一次，覆盖心跳晚到的进程；就绪前的新版本没有
    在跑作业（API 未就绪则执行循环未启动），强杀不损失任何作业。联接重建与库
    顶替按幂等设计：重复执行同一标记，库与快照一致时不再顶替。

    Returns:
        RollbackOutcome: detail 供事件与命令输出；is_healthy 表示旧版本是否在
            健康检查时限内就绪，且清场后确认新版本进程确实退场。
    """
    parts = [f"回滚原因：{reason}"]
    _retire_unready_core(app, marker, inputs=inputs, parts=parts)
    parts.append(_switch_back(app, marker))
    parts.append(_restore_database(app, marker, inputs=inputs))
    _retire_unready_core(app, marker, inputs=inputs, parts=parts)
    is_healthy = restart_and_check(app, inputs=inputs, parts=parts)
    late = _unready_detail(app, marker, inputs=inputs)
    if late is not None:
        parts.append(late)
        is_healthy = False
    return RollbackOutcome(detail="；".join(parts), is_healthy=is_healthy)


def _abort_before_switch(
    app: UpgradeLayout, plan: UpgradePlan, *, inputs: UpgradeInputs, abort: _Abort
) -> int:
    """切换前的失败收尾：联接与库未动过，只把服务恢复到升级前并写失败事件。

    核心未被排水（本来没在跑或排水被拒）时服务一直在，is_healthy 即 True。
    """
    parts = [f"升级中止：{abort.reason}"]
    is_healthy = True
    if abort.is_core_stopped:
        is_healthy = restart_and_check(app, inputs=inputs, parts=parts)
    detail = "；".join(parts)
    record_failed(
        app.database,
        UpgradeIdentity(plan.target_version, plan.previous_version),
        detail=detail,
        is_healthy=is_healthy,
        at=inputs.clock.utc_now(),
    )
    print(f"升级失败：{detail}", file=sys.stderr)
    return 1


def _settle_failed_upgrade(
    app: UpgradeLayout, marker: UpgradeMarker, *, inputs: UpgradeInputs, reason: str
) -> int:
    """切换后的失败收尾：按标记回滚、清标记、写 host.upgrade_failed。"""
    outcome = perform_rollback(app, marker, inputs=inputs, reason=reason)
    clear_marker(marker_path(app.data_root))
    record_failed(
        app.database,
        _identity_of(marker),
        detail=outcome.detail,
        is_healthy=outcome.is_healthy,
        at=inputs.clock.utc_now(),
    )
    print(f"升级失败：{outcome.detail}", file=sys.stderr)
    return 1


def _settle_success(
    app: UpgradeLayout, plan: UpgradePlan, *, snapshot_name: str, inputs: UpgradeInputs
) -> int:
    """成功收尾：保留清理、清标记、写 host.upgrade_finished。

    保留清理失败不推翻升级结论：服务已在新版本上就绪，清理原因记进事件与
    输出，留给下一次升级重试。
    """
    removed: tuple[str, ...] = ()
    prune_detail = "未清理旧版本"
    try:
        removed = prune_releases(
            app.releases, protected=(app.releases / plan.target_version, plan.previous_dir)
        )
        prune_detail = f"清理旧版本 {len(removed)} 个"
    except UpgradeError as error:
        print(f"旧版本保留清理失败：{error}", file=sys.stderr)
        prune_detail = f"旧版本清理失败：{error}"
    clear_marker(marker_path(app.data_root))
    detail = f"已升级到 {plan.target_version}（快照 {snapshot_name}；{prune_detail}）"
    record_finished(
        app.database,
        UpgradeIdentity(plan.target_version, plan.previous_version),
        detail=detail,
        at=inputs.clock.utc_now(),
    )
    print(f"升级完成：current -> {plan.target_version}；{detail}")
    return 0


def _retire_unready_core(
    app: UpgradeLayout, marker: UpgradeMarker, *, inputs: UpgradeInputs, parts: list[str]
) -> None:
    """结束未就绪的新版本进程（若有），结果并进 parts。"""
    pid = unready_core_pid(app.data_root, marker.target_version, inputs=inputs)
    if pid is not None:
        parts.append(terminate_pid(pid, inputs=inputs))


def _unready_detail(
    app: UpgradeLayout, marker: UpgradeMarker, *, inputs: UpgradeInputs
) -> str | None:
    """就绪判定后的补杀：新版本进程仍在服务时结束它并给出 detail，否则 None。"""
    pid = unready_core_pid(app.data_root, marker.target_version, inputs=inputs)
    if pid is None:
        return None
    return terminate_pid(pid, inputs=inputs) + "；服务未恢复到旧版本"


def _switch_back(app: UpgradeLayout, marker: UpgradeMarker) -> str:
    """联接切回前一版本目录；目录被删时如实报告，不猜回滚目标。"""
    previous = app.releases / marker.previous_version
    if not previous.is_dir():
        return f"前一版本目录缺失，联接未切回：{previous}"
    switch_current(app.app_dir, previous)
    return f"current 已切回 {marker.previous_version}"


def _restore_database(app: UpgradeLayout, marker: UpgradeMarker, *, inputs: UpgradeInputs) -> str:
    """用升级前快照顶替现行库：现行库连同 WAL 改名保留现场，快照复制后核对。

    只核对完整性、不再迁移：快照生成于前一版本在跑时，schema 就是前一版本
    所期望的形态；用升级执行方的迁移前滚反而可能越过回滚目标的版本。库与
    快照字节一致时不再顶替，重复回滚不产生重复的现场文件。失败不抛，转成
    人话 detail 并入回滚结论。
    """
    snapshot = app.data_root / "backups" / marker.snapshot_name
    if not snapshot.is_file():
        return f"回滚快照缺失，库未恢复：{marker.snapshot_name}"
    if _same_file(app.database, snapshot):
        return f"库与快照一致，未重复恢复：{snapshot.name}"
    keepaside = app.database.with_name(
        f"{app.database.stem}.pre-rollback-{_stamp(inputs)}{app.database.suffix}"
    )
    try:
        if app.database.exists():
            os.replace(app.database, keepaside)
        _move_wal_sidecars(app.database, keepaside)
        shutil.copyfile(snapshot, app.database)
        _verify_restored(app.database)
    except (OSError, sqlite3.Error) as error:
        return f"库恢复失败：{error}（现行库保留为 {keepaside.name}）"
    return f"库已恢复自 {snapshot.name}（原库保留为 {keepaside.name}）"


def _same_file(left: Path, right: Path) -> bool:
    """两份文件字节一致（SHA-256）；任一不可读按不一致处理。"""
    try:
        return _sha256(left) == _sha256(right)
    except OSError:
        return False


def _sha256(path: Path) -> str:
    """计算文件 SHA-256 摘要。"""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _move_wal_sidecars(database: Path, keepaside: Path) -> None:
    """把 -wal 与 -shm 随主库一起挪走：残留的旧 WAL 配上新库文件会互相污染。"""
    for suffix in ("-wal", "-shm"):
        sidecar = database.with_name(database.name + suffix)
        if sidecar.exists():
            os.replace(sidecar, sidecar.with_name(keepaside.name + suffix))


def _verify_restored(database: Path) -> None:
    """恢复后的完整性核对；失败说明快照或目标盘不可信。

    Raises:
        sqlite3.Error: integrity_check 未通过，由调用方记入回滚 detail。
    """
    connection = open_database(database)
    try:
        verify_integrity(connection)
    finally:
        connection.close()


def _snapshot_database(app: UpgradeLayout, version: str, *, inputs: UpgradeInputs) -> Path:
    """升级前快照；回滚材料在这里生成，失败则升级不得继续。

    Raises:
        UpgradeError: 库不存在或快照生成失败（磁盘、权限、完整性）。
    """
    if not app.database.is_file():
        raise UpgradeError(f"库文件不存在，先启动一次 serve 完成初始化：{app.database}")
    connection = open_database(app.database)
    try:
        snapshot = create_pre_upgrade_snapshot(
            connection, app.data_root / "backups", version=version, clock=inputs.clock
        )
    except (BackupError, sqlite3.Error) as error:
        raise UpgradeError(f"升级前快照失败：{error}") from error
    finally:
        connection.close()
    return snapshot.path


def _stamp(inputs: UpgradeInputs) -> str:
    """文件名时间戳，与备份快照同一形状。"""
    return inputs.clock.utc_now().astimezone(UTC).strftime(TIMESTAMP_FORMAT)


def _identity_of(marker: UpgradeMarker) -> UpgradeIdentity:
    """从回滚凭据取事件版本身份。"""
    return UpgradeIdentity(marker.target_version, marker.previous_version)


__all__ = [
    "RollbackOutcome",
    "perform_rollback",
    "run_rollback",
    "run_upgrade",
]
