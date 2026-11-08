"""脚本会话标记：启动时记录本次运行的日志起点，验证阶段据此只取本轮日志段。

标记落在本次运行的工作目录里，属于插件私有的运行期状态，内核不读。日志路径由
插件配置的 ``maa_dir`` 加固定的 ``debug`` 目录推出，标记只存字节偏移：偏移越界
或字段缺失按没有标记处理，验证阶段据此记 unknown，不猜日志段。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

SESSION_FILE_NAME = "maa-session.json"


@dataclass(frozen=True, slots=True)
class SessionMarker:
    """一次脚本启动的日志定位：两份日志各自从哪个字节起属于本轮运行。"""

    gui_offset: int
    asst_offset: int


def write_marker(work_dir: Path, marker: SessionMarker) -> Path:
    """把标记写进工作目录；写不进去时抛 OSError，让启动路径失败关闭。"""
    path = work_dir / SESSION_FILE_NAME
    work_dir.mkdir(parents=True, exist_ok=True)
    data = {"gui_offset": marker.gui_offset, "asst_offset": marker.asst_offset}
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def read_marker(work_dir: Path) -> SessionMarker | None:
    """读标记；文件缺失、不是对象、字段缺失或非法都返回 None。"""
    try:
        data = json.loads((work_dir / SESSION_FILE_NAME).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    gui_offset = _offset(data.get("gui_offset"))
    asst_offset = _offset(data.get("asst_offset"))
    if gui_offset is None or asst_offset is None:
        return None
    return SessionMarker(gui_offset=gui_offset, asst_offset=asst_offset)


def _offset(value: object) -> int | None:
    """偏移必须是非负整数；bool 是 int 的子类，单独排除。"""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value
