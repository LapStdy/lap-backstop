"""lap-backstop 命令行入口：子命令分发、数据根解析与退出码常量。

退出码集中定义：0 正常；1 一般失败；2 doctor 仅有 WARN；3 第二实例；
4 端口冲突与 5 磁盘水位属于核心运行期退出码，由对应机制产生（docs/08）。
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

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
        from core.logging import JsonLineLogger

        data_root = Path(arguments.data_root)
        options = ServeOptions(
            log=JsonLineLogger(data_root / "logs" / "core.jsonl"),
            stop=install_stop_handlers(),
            register_tasks=not arguments.no_register_tasks,
        )
        code = run_serve(data_root, options)
        if code != EXIT_OK:
            print(
                f"serve 退出码 {code}，运行日志见 {data_root / 'logs' / 'core.jsonl'}",
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
    if arguments.command == "db":
        return _run_db_check(Path(arguments.data_root))
    from core.clock import SystemClock
    from core.hostops.tasks import register_registered_tasks

    data_root = Path(arguments.data_root)
    registration = register_registered_tasks(data_root, registered_at=SystemClock().utc_now())
    print(registration.detail)
    return EXIT_OK if registration.ok else EXIT_ERROR


def _run_watchdog_check(data_root: Path) -> int:
    """看门狗单次检查；数值取 [host] 默认值（唯一真源，docs/08 独立看门狗一节）。"""
    from datetime import UTC, datetime

    from core.hostops.watchdog import CheckInputs, CheckPolicy, is_pid_alive, run_check
    from storage.defaults import default

    inputs = CheckInputs(
        now=datetime.now(UTC),
        policy=CheckPolicy(
            stale_seconds=float(default("host", "stale_seconds")),
            acceptance_seconds=float(default("host", "acceptance_seconds")),
            give_up_window=timedelta(minutes=int(default("host", "give_up_window_minutes"))),
            give_up_max_failures=int(default("host", "give_up_max_failures")),
        ),
        runner=subprocess.run,
        pid_alive=is_pid_alive,
    )
    return run_check(data_root, inputs=inputs)


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
        print(f"数据库检查未通过：{error}", file=sys.stderr)
        return EXIT_ERROR
    print(f"数据库完整，schema 版本 {version}：{database_path}")
    return EXIT_OK


def _build_parser() -> argparse.ArgumentParser:
    """子命令表：serve 常驻，watchdog 看门狗，doctor 自检，backup 备份，
    db check 库检查，tasks 计划任务；worker 由 main 拦截转发，不出现在帮助里。"""
    parser = argparse.ArgumentParser(
        prog="lap-backstop",
        description="lap-backstop 宿主操作：常驻核心与排障入口",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
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
    watchdog_parser = subparsers.add_parser("watchdog", help="看门狗：检查核心心跳")
    watchdog_sub = watchdog_parser.add_subparsers(dest="watchdog_command", required=True)
    check_parser = watchdog_sub.add_parser("check", help="单次检查，由计划任务每 5 分钟唤醒")
    check_parser.add_argument("--data-root", default=str(resolve_data_root()))
    doctor_parser = subparsers.add_parser("doctor", help="宿主环境自检")
    doctor_parser.add_argument("--data-root", default=str(resolve_data_root()))
    backup_parser = subparsers.add_parser("backup", help="每日备份：快照、配置导出与修剪")
    backup_parser.add_argument("--data-root", default=str(resolve_data_root()))
    db_parser = subparsers.add_parser("db", help="数据库维护")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True)
    db_check_parser = db_sub.add_parser("check", help="完整性检查并迁移到当前版本（恢复流程用）")
    db_check_parser.add_argument("--data-root", default=str(resolve_data_root()))
    tasks_parser = subparsers.add_parser("tasks", help="计划任务注册")
    tasks_sub = tasks_parser.add_subparsers(dest="tasks_command", required=True)
    register_parser = tasks_sub.add_parser("register", help="注册或修复 P0 计划任务")
    register_parser.add_argument("--data-root", default=str(resolve_data_root()))
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
