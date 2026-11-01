"""lap-backstop 命令行入口：子命令分发、数据根解析与退出码常量。

退出码集中定义：0 正常；1 一般失败；2 doctor 仅有 WARN；3 第二实例；
4 端口冲突与 5 磁盘水位属于核心运行期退出码，由对应机制产生（docs/08）。
"""

from __future__ import annotations

import argparse
import functools
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.hostops.watchdog import CheckInputs

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_WARN_ONLY = 2
EXIT_SECOND_INSTANCE = 3
EXIT_PORT_CONFLICT = 4
EXIT_DISK_LOW = 5

# 打包件位于 app\releases\<version>\lap-backstop.exe，数据根在其上三级的 data\。
_FROZEN_ROOT_DEPTH = 3


def resolve_data_root() -> Path:
    """数据根目录：源码运行取仓库内的 data\，打包运行取 exe 上三级的 data\。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parents[_FROZEN_ROOT_DEPTH] / "data"
    return Path(__file__).resolve().parents[1] / "data"


def resolve_executable_path() -> str:
    """当前解释器或 exe 的绝对路径，写进锁文件供排障。"""
    return str(Path(sys.executable).resolve())


def main(argv: list[str] | None = None) -> int:
    """命令行入口，返回进程退出码。"""
    tokens = sys.argv[1:] if argv is None else argv
    if tokens[:1] == ["worker"]:
        # 打包件 exe 的 worker 回调不经主解析器注册，对外帮助保持不可见（ADR 0010）。
        from core.plugins.worker import main as worker_main

        return worker_main(tokens[1:])
    arguments = _build_parser().parse_args(tokens)
    if arguments.command == "serve":
        from core.hostops.serve import ServeOptions, install_stop_handlers, run_serve
        from core.logging import DailyRotatingJsonLineLogger

        data_root = Path(arguments.data_root)
        options = ServeOptions(
            log=DailyRotatingJsonLineLogger(data_root / "logs"),
            stop=install_stop_handlers(),
            register_tasks=not arguments.no_register_tasks,
            run_timesync=not arguments.no_timesync,
        )
        code = run_serve(data_root, options)
        if code != EXIT_OK:
            print(
                f"serve 退出码 {code}，运行日志见 {data_root / 'logs'} 目录",
                file=sys.stderr,
            )
        return code
    if arguments.command == "watchdog":
        return _run_watchdog_check(Path(arguments.data_root))
    if arguments.command == "doctor":
        from core.hostops.doctor import run_doctor

        return run_doctor(Path(arguments.data_root))
    if arguments.command == "backup":
        return _run_backup(Path(arguments.data_root))
    if arguments.command == "evidence-mirror":
        return _run_evidence_mirror(Path(arguments.data_root))
    if arguments.command == "db":
        return _run_db_check(Path(arguments.data_root))
    if arguments.command == "upgrade":
        return _run_upgrade(Path(arguments.data_root), Path(arguments.target))
    if arguments.command == "rollback":
        return _run_rollback(Path(arguments.data_root))
    from core.clock import SystemClock
    from core.hostops.tasks import register_registered_tasks

    data_root = Path(arguments.data_root)
    registration = register_registered_tasks(data_root, registered_at=SystemClock().utc_now())
    print(registration.detail)
    return EXIT_OK if registration.ok else EXIT_ERROR


def _run_watchdog_check(data_root: Path) -> int:
    """看门狗单次检查；注入项见 _watchdog_check_inputs。"""
    from core.hostops.watchdog import run_check

    return run_check(data_root, inputs=_watchdog_check_inputs(data_root))


def _watchdog_check_inputs(data_root: Path) -> CheckInputs:
    """看门狗检查的注入项：数值取 [host] 默认值，dead-man 挂真实传输与渠道（docs/08）。"""
    from core.clock import SystemClock
    from core.hostops import deadman
    from core.hostops.watchdog import CheckInputs, CheckPolicy, is_pid_alive
    from storage.defaults import default

    return CheckInputs(
        now=datetime.now(UTC),
        policy=CheckPolicy(
            stale_seconds=float(default("host", "stale_seconds")),
            acceptance_seconds=float(default("host", "acceptance_seconds")),
            give_up_window=timedelta(minutes=int(default("host", "give_up_window_minutes"))),
            give_up_max_failures=int(default("host", "give_up_max_failures")),
        ),
        runner=subprocess.run,
        pid_alive=is_pid_alive,
        deadman=functools.partial(
            deadman.on_check_report,
            data_root=data_root,
            inputs=deadman.DeadmanInputs(clock=SystemClock()),
        ),
    )


def _run_backup(data_root: Path) -> int:
    """每日备份：在线快照、配置导出与修剪（docs/08「备份与恢复」）。"""
    from core.clock import SystemClock
    from core.hostops.doctor import DATABASE_FILENAME
    from storage import BackupError, create_snapshot, export_configs, open_database, prune_backups

    database_path = data_root / DATABASE_FILENAME
    if not database_path.is_file():
        print(f"库文件不存在，先启动一次 serve 完成初始化：{database_path}", file=sys.stderr)
        return EXIT_ERROR
    backups_dir = data_root / "backups"
    try:
        connection = open_database(database_path)
        try:
            clock = SystemClock()
            snapshot = create_snapshot(connection, backups_dir, clock=clock)
            exports = export_configs(connection, backups_dir, clock=clock)
        finally:
            connection.close()
        pruned = prune_backups(backups_dir)
    except (BackupError, sqlite3.Error, OSError) as error:
        _notify_failure(data_root, database_path, title="数据库备份失败", error=error)
        print(
            f"备份失败：{error}；检查 {backups_dir} 的写权限与磁盘剩余空间后重试",
            file=sys.stderr,
        )
        return EXIT_ERROR
    print(
        f"备份完成：{snapshot.path.name}（schema 版本 {snapshot.schema_version}，"
        f"{snapshot.size_bytes} 字节）；导出 {exports.settings_path.name} 与 "
        f"{exports.plugins_path.name}；修剪 {len(pruned.deleted)} 个文件"
    )
    return EXIT_OK


def _run_evidence_mirror(data_root: Path) -> int:
    """证据增量镜像：超阈值按 verdict 引用裁剪，失败写事件（docs/08「备份与恢复」）。"""
    from core.clock import SystemClock
    from core.hostops.doctor import DATABASE_FILENAME
    from core.hostops.evidence_mirror import (
        EvidenceMirrorError,
        mirror_evidence,
        mirror_threshold_bytes,
        plan_evidence,
        record_finished,
    )
    from storage import open_database

    database_path = data_root / DATABASE_FILENAME
    if not database_path.is_file():
        print(f"库文件不存在，先启动一次 serve 完成初始化：{database_path}", file=sys.stderr)
        return EXIT_ERROR
    try:
        connection = open_database(database_path)
        try:
            plan = plan_evidence(
                connection,
                data_root / "evidence",
                threshold_bytes=mirror_threshold_bytes(),
            )
            outcome = mirror_evidence(plan, data_root / "backups" / "evidence")
            record_finished(connection, outcome, at=SystemClock().utc_now())
        finally:
            connection.close()
    except (EvidenceMirrorError, OSError, sqlite3.Error) as error:
        _record_mirror_failure(database_path, error)
        print(f"证据镜像失败：{error}；重跑同一命令续做", file=sys.stderr)
        return EXIT_ERROR
    print(
        f"证据镜像完成：新增 {outcome.copied_new}、变更 {outcome.copied_changed}、"
        f"跳过一致 {outcome.skipped_unchanged}、按策略跳过 {outcome.skipped_by_policy}，"
        f"共 {outcome.bytes_copied} 字节"
    )
    return EXIT_OK


def _record_mirror_failure(database_path: Path, error: Exception) -> None:
    """镜像失败补记一条 host.event；事件写不进去只提示，不掩盖退出码。"""
    from core.clock import SystemClock
    from core.hostops.evidence_mirror import record_failure
    from storage import open_database

    try:
        connection = open_database(database_path)
        try:
            record_failure(connection, str(error), at=SystemClock().utc_now())
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as event_error:
        print(f"失败事件未写入：{event_error}", file=sys.stderr)


def _run_db_check(data_root: Path) -> int:
    """数据库检查：integrity_check 加迁移到当前版本（docs/08 恢复顺序第 4 步）。"""
    from core.hostops.doctor import DATABASE_FILENAME
    from storage import MigrationError, migrate, open_database, verify_integrity

    database_path = data_root / DATABASE_FILENAME
    if not database_path.is_file():
        print(
            f"库文件不存在，先把目标快照复制为 {database_path} 再检查（docs/08 恢复顺序）",
            file=sys.stderr,
        )
        return EXIT_ERROR
    try:
        connection = open_database(database_path)
        try:
            verify_integrity(connection)
            version = migrate(connection)
        finally:
            connection.close()
    except (MigrationError, sqlite3.Error, OSError) as error:
        _notify_failure(data_root, database_path, title="数据库检查未通过", error=error)
        print(f"数据库检查未通过：{error}", file=sys.stderr)
        return EXIT_ERROR
    print(f"数据库完整，schema 版本 {version}：{database_path}")
    return EXIT_OK


def _run_upgrade(data_root: Path, target: Path) -> int:
    """升级编排：外部动作全部走生产默认（真实 HTTP、schtasks 与进程存活检查）。"""
    from core.hostops.core_control import UpgradeInputs
    from core.hostops.upgrade import run_upgrade

    return run_upgrade(data_root, target, inputs=UpgradeInputs())


def _run_rollback(data_root: Path) -> int:
    r"""手动回滚：按 data\run\upgrade.state.json 的标记执行，无标记即空操作。"""
    from core.hostops.core_control import UpgradeInputs
    from core.hostops.upgrade import run_rollback

    return run_rollback(data_root, inputs=UpgradeInputs())


def _notify_failure(
    data_root: Path,
    database_path: Path,
    *,
    title: str,
    error: BaseException,
) -> None:
    """备份或库检查失败时发一条 ERROR 通知；通知失败只提示，不改退出码。

    CLI 没有常驻核心与连接池：配置从同一份库现读，投递复用一次性实现
    （core.notify.selftest），不建投递线程、不记账目。
    """
    from contracts import ContractError, Notification, NotificationLevel
    from core.api.store import all_settings
    from core.clock import SystemClock
    from core.notify.selftest import build_kernel_delivery_context, deliver_test_notification
    from storage import open_database

    try:
        connection = open_database(database_path)
        try:
            settings = all_settings(connection)
        finally:
            connection.close()
    except (OSError, sqlite3.Error, ValueError) as read_error:
        print(f"失败通知未发送：读不到通知配置（{read_error}）", file=sys.stderr)
        return
    context = build_kernel_delivery_context(SystemClock(), data_root=data_root)
    message = Notification(
        level=NotificationLevel.ERROR,
        title=title,
        body=f"库：{database_path}\n原因：{error}",
    )
    try:
        delivery = deliver_test_notification(settings, context, message=message)
    except ContractError as config_error:
        print(f"失败通知未发送：{config_error.message}", file=sys.stderr)
        return
    if not delivery.result.ok:
        print(f"失败通知发送失败：{delivery.result.detail}", file=sys.stderr)


def _build_parser() -> argparse.ArgumentParser:
    """子命令表：serve 常驻，watchdog 看门狗，doctor 自检，backup 备份，
    evidence-mirror 证据镜像，db check 库检查，tasks 计划任务，upgrade 与
    rollback 升级回滚；worker 由 main 拦截转发，不出现在帮助里。"""
    parser = argparse.ArgumentParser(
        prog="lap-backstop",
        description="lap-backstop 宿主操作：常驻核心与排障入口",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_serve_and_watchdog_parsers(subparsers)
    _add_maintenance_parsers(subparsers)
    _add_upgrade_parsers(subparsers)
    return parser


def _add_serve_and_watchdog_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """注册 serve 常驻与 watchdog 看门狗子命令。"""
    serve_parser = subparsers.add_parser("serve", help="常驻核心：调度循环与心跳")
    serve_parser.add_argument(
        "--data-root",
        default=str(resolve_data_root()),
        help="数据根目录，默认自动解析",
    )
    serve_parser.add_argument(
        "--no-register-tasks",
        action="store_true",
        help="跳过启动时的计划任务注册（测试与开发联调用）",
    )
    serve_parser.add_argument(
        "--no-timesync",
        action="store_true",
        help="跳过启动时的系统校时（测试与开发联调用）",
    )
    watchdog_parser = subparsers.add_parser("watchdog", help="看门狗：检查核心心跳")
    watchdog_sub = watchdog_parser.add_subparsers(dest="watchdog_command", required=True)
    check_parser = watchdog_sub.add_parser("check", help="单次检查，由计划任务每 5 分钟唤醒")
    check_parser.add_argument("--data-root", default=str(resolve_data_root()))


def _add_maintenance_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """注册 doctor、backup、evidence-mirror、db 与 tasks 维护子命令。"""
    doctor_parser = subparsers.add_parser("doctor", help="宿主环境自检")
    doctor_parser.add_argument("--data-root", default=str(resolve_data_root()))
    backup_parser = subparsers.add_parser("backup", help="每日备份：快照、配置导出与修剪")
    backup_parser.add_argument("--data-root", default=str(resolve_data_root()))
    mirror_parser = subparsers.add_parser(
        "evidence-mirror", help="证据增量镜像：把证据目录搬进备份目标"
    )
    mirror_parser.add_argument("--data-root", default=str(resolve_data_root()))
    db_parser = subparsers.add_parser("db", help="数据库维护")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True)
    db_check_parser = db_sub.add_parser("check", help="完整性检查并迁移到当前版本（恢复流程用）")
    db_check_parser.add_argument("--data-root", default=str(resolve_data_root()))
    tasks_parser = subparsers.add_parser("tasks", help="计划任务注册")
    tasks_sub = tasks_parser.add_subparsers(dest="tasks_command", required=True)
    register_parser = tasks_sub.add_parser("register", help="注册或修复 P0 计划任务")
    register_parser.add_argument("--data-root", default=str(resolve_data_root()))


def _add_upgrade_parsers(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """注册 upgrade 与 rollback 升级回滚子命令。"""
    upgrade_parser = subparsers.add_parser(
        "upgrade", help="升级到 app\\releases 下的新版本：drain、快照、切换与健康检查"
    )
    upgrade_parser.add_argument("target", help="新版本目录，须已在 app\\releases\\<版本>\\ 下")
    upgrade_parser.add_argument("--data-root", default=str(resolve_data_root()))
    rollback_parser = subparsers.add_parser(
        "rollback", help="按升级标记手动回滚到前一版本；无标记即空操作"
    )
    rollback_parser.add_argument("--data-root", default=str(resolve_data_root()))


if __name__ == "__main__":
    raise SystemExit(main())
