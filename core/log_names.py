r"""核心日志文件名文法的唯一定义点。

`core-YYYY-MM-DD[.N].jsonl` 及其 `.gz` 归档形态是磁盘契约：日志写入
（core.logging 的轮转器）、保留清理（core.hostops.retention）与看门狗
（core.hostops.watchdog）按同一文法匹配与分组，三处引用这里编译出的同一
对象，改文法不会漏改任何一处。只依赖标准库 re：看门狗为在核心损坏时照常
醒来只用标准库，本模块因此不得引入项目内或第三方依赖。
"""

from __future__ import annotations

import re

CORE_LOG_NAME = re.compile(r"^core-(?P<day>\d{4}-\d{2}-\d{2})(?:\.(?P<index>\d+))?\.jsonl$")
CORE_LOG_ARCHIVED_NAME = re.compile(
    r"^core-(?P<day>\d{4}-\d{2}-\d{2})(?:\.(?P<index>\d+))?\.jsonl\.gz$"
)

__all__ = ["CORE_LOG_ARCHIVED_NAME", "CORE_LOG_NAME"]
