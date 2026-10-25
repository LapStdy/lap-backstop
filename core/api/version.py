"""发行包版本号：serve 心跳与 /health 共用，置于叶模块供两边引用。"""

from __future__ import annotations

import importlib.metadata


def executable_version() -> str:
    """发行包版本号；源码直跑（环境里未安装发行包）记 ``dev``。"""
    try:
        return importlib.metadata.version("lap-backstop")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


__all__ = ["executable_version"]
