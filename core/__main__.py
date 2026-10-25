"""lap-backstop 命令行入口：子命令分发、数据根解析与退出码常量。

退出码集中定义：0 正常；1 一般失败；2 doctor 仅有 WARN；3 第二实例；
4 端口冲突与 5 磁盘水位属于核心运行期退出码，由对应机制产生（docs/08）。
"""

from __future__ import annotations

import argparse
import sys
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
    arguments = _build_parser().parse_args(argv)
    if arguments.command == "serve":
        from core.hostops.serve import ServeOptions, install_stop_handlers, run_serve
        from core.logging import JsonLineLogger

        data_root = Path(arguments.data_root)
        options = ServeOptions(
            log=JsonLineLogger(data_root / "logs" / "core.jsonl"),
            stop=install_stop_handlers(),
        )
        code = run_serve(data_root, options)
        if code != EXIT_OK:
            print(
                f"serve 退出码 {code}，运行日志见 {data_root / 'logs' / 'core.jsonl'}",
                file=sys.stderr,
            )
        return code
    print(f"{arguments.command} 子命令本版本未提供", file=sys.stderr)
    return EXIT_ERROR


def _build_parser() -> argparse.ArgumentParser:
    """子命令表：serve 常驻，doctor 自检，tasks 计划任务注册。"""
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
    subparsers.add_parser("doctor", help="宿主环境自检")
    subparsers.add_parser("tasks", help="计划任务注册")
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
