"""默认值加载：唯一真源是 config/defaults.toml（docs/11 配置一节）。

默认值随代码发布；settings 表与启动参数只覆盖，不重复声明同一份数字。
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Final

DEFAULTS_PATH = Path(__file__).resolve().parent.parent / "config" / "defaults.toml"


def load_defaults(path: Path = DEFAULTS_PATH) -> dict[str, Any]:
    """读取默认值文件；文件缺失或 TOML 非法直接抛错，不带病启动。"""
    with path.open("rb") as stream:
        data = tomllib.load(stream)
    return data


DEFAULTS: Final[dict[str, Any]] = load_defaults()


def default(section: str, key: str) -> Any:
    """取某个默认值；缺键说明代码与 defaults.toml 已经不一致。"""
    try:
        return DEFAULTS[section][key]
    except KeyError as error:
        raise KeyError(f"defaults.toml 缺少 {section}.{key}") from error


__all__ = ["DEFAULTS", "DEFAULTS_PATH", "default", "load_defaults"]
