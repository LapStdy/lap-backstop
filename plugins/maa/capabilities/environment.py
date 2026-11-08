"""MAA 环境能力（docs/plugins/maa.md 环境节，EnvironmentProvider 协议见 docs/05）。

``probe`` 经只读端口核对四件事：模拟器窗口、adb 设备状态、游戏是否在前台、截图分辨率，
按环境状态机给出 off / booting / degraded / failed / ready。四个环境动作仍返回
``E_PLUGIN`` 失败且不触碰真机：启动模拟器、拉起游戏与清理属于后续卡片。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from contracts import (
    EnvironmentProbe,
    EnvironmentRef,
    EnvironmentState,
    PhaseResult,
    RunContext,
)
from plugins.maa.windows_host import MachineHost, ScreenSize, WindowsMachineHost

_ACTION_UNAVAILABLE = "环境动作不可用：未执行真机操作"
_DEFAULT_MUMU_DIR = "H:/MuMuPlayer"
_DEFAULT_ADB_ADDRESS = "127.0.0.1:16384"
_DEFAULT_WINDOW_TITLE = "MuMu"
_DEFAULT_GAME_PACKAGE = "com.hypergryph.arknights"
_DEFAULT_SCREEN_SIZE = "1280x720"
_ADB_DEVICE = "device"
_ADB_ABSENT = "absent"


class MaaEnvironment:
    """environment 能力：探测走只读端口，动作一律失败关闭。"""

    def __init__(self, host: MachineHost | None = None) -> None:
        self._host = host

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        """按窗口、adb、前台与分辨率四项给出环境状态；只读且可重复。"""
        config = ctx.config
        host = self._host if self._host is not None else _build_host(config)
        title = _find_window(host.window_titles(), _text(config, "emulator_window_title"))
        if title is None:
            needle = _text(config, "emulator_window_title")
            return _probe(
                ctx, EnvironmentState.OFF, f"模拟器窗口不存在：未找到标题含「{needle}」的窗口"
            )
        address = _text(config, "adb_address")
        adb_state = host.adb_device_state()
        if adb_state == _ADB_ABSENT:
            return _probe(
                ctx,
                EnvironmentState.BOOTING,
                f"模拟器窗口存在，adb 未连接：{address} 不在 adb devices 中",
            )
        if adb_state != _ADB_DEVICE:
            return _probe(ctx, EnvironmentState.DEGRADED, f"adb 设备不可用：{adb_state}")
        size = host.screencap_size()
        if size is None:
            return _probe(ctx, EnvironmentState.DEGRADED, "adb 在线但截图失败")
        game = _text(config, "game_package")
        package = host.foreground_package()
        if package != game:
            return _probe(ctx, EnvironmentState.DEGRADED, f"游戏未在前台：当前 {package or '未知'}")
        expected = _expected_size(config)
        if (size.width, size.height) != (expected.width, expected.height):
            return _probe(
                ctx,
                EnvironmentState.FAILED,
                f"分辨率不符：截图 {size.width}x{size.height}，"
                f"期望 {expected.width}x{expected.height}",
            )
        return _probe(
            ctx,
            EnvironmentState.READY,
            f"环境就绪：{address} 在线，{game} 前台，截图 {size.width}x{size.height}",
        )

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        """返回准备失败；不启动模拟器与游戏。"""
        return _unavailable(ctx)

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        """返回复位失败；不断开也不重连 adb。"""
        return _unavailable(ctx)

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        """返回清理失败；不关闭游戏与模拟器。"""
        return _unavailable(ctx)

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        """返回保温失败；不维持任何环境状态。"""
        return _unavailable(ctx)


def _probe(ctx: RunContext, state: EnvironmentState, detail: str) -> EnvironmentProbe:
    """构造带注入时钟时间戳的探测结果。"""
    return EnvironmentProbe(state=state, detail=detail, observed_at=ctx.clock.utc_now())


def _find_window(titles: Sequence[str], needle: str) -> str | None:
    """按子串找模拟器窗口；找不到返回 None，不做模糊猜测。"""
    for title in titles:
        if needle in title:
            return title
    return None


def _text(config: Mapping[str, Any], key: str) -> str:
    """取配置里的字符串值；缺省值按 schema 默认值补齐。"""
    defaults = {
        "emulator_window_title": _DEFAULT_WINDOW_TITLE,
        "adb_address": _DEFAULT_ADB_ADDRESS,
        "game_package": _DEFAULT_GAME_PACKAGE,
    }
    return str(config.get(key, defaults[key]))


def _expected_size(config: Mapping[str, Any]) -> ScreenSize:
    """解析「宽x高」形式的分辨率；写法非法时退回默认值，避免探测因配置崩掉。"""
    text = str(config.get("screen_size", _DEFAULT_SCREEN_SIZE))
    width_text, separator, height_text = text.partition("x")
    if not separator:
        return _default_size()
    try:
        width, height = int(width_text), int(height_text)
    except ValueError:
        return _default_size()
    if width <= 0 or height <= 0:
        return _default_size()
    return ScreenSize(width=width, height=height)


def _default_size() -> ScreenSize:
    """默认分辨率：方舟横屏 1280x720。"""
    return ScreenSize(width=1280, height=720)


def _build_host(config: Mapping[str, Any]) -> MachineHost:
    """按配置拼出真机端口；adb 与 MuMu 装在同一目录树下。"""
    mumu_dir = Path(str(config.get("mumu_dir", _DEFAULT_MUMU_DIR)))
    return WindowsMachineHost(
        adb_path=mumu_dir / "nx_main" / "adb.exe",
        address=_text(config, "adb_address"),
    )


def _unavailable(ctx: RunContext) -> PhaseResult:
    """构造带时间戳的失败结果；起点与终点同刻，表示未发生任何等待。"""
    now = ctx.clock.utc_now()
    return PhaseResult(
        ok=False,
        error_code="E_PLUGIN",
        detail=_ACTION_UNAVAILABLE,
        started_at=now,
        ended_at=now,
    )
