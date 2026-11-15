"""MAA 环境能力（docs/plugins/maa.md 环境节，EnvironmentProvider 协议见 docs/05）。

``probe`` 经只读端口核对四件事：模拟器窗口、adb 设备状态、游戏是否在前台、截图分辨率，
按环境状态机给出 off / booting / degraded / failed / ready。``prepare`` 执行准备的前三步：
复用或拉起 MuMu、确认 adb 与截图、把游戏拉到前台，随后过主界面门控（``page_gate``
开关，规则见 docs/arknights/03、04），放行才算就绪。``reset`` 断开并重连 adb 后重启游戏；
``cleanup`` 按保温开关关闭游戏与实例；``warm`` 保证游戏在前台。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from contracts import (
    EnvironmentProbe,
    EnvironmentRef,
    EnvironmentState,
    PhaseResult,
    RunContext,
)
from plugins.maa.capabilities.page_gate import PageGate
from plugins.maa.emulator_host import EmulatorHost, WindowsEmulatorHost
from plugins.maa.mumu_instance import MumuInstance
from plugins.maa.vision.frames import AdbFrameSource, FrameSource
from plugins.maa.windows_host import MachineHost, ScreenSize, WindowsMachineHost

_DEFAULT_MUMU_DIR = "H:/MuMuPlayer"
_DEFAULT_ADB_ADDRESS = "127.0.0.1:16384"
_DEFAULT_WINDOW_TITLE = "MuMu"
_DEFAULT_GAME_PACKAGE = "com.hypergryph.arknights"
_DEFAULT_SCREEN_SIZE = "1280x720"
_DEFAULT_MUMU_BOOT_SECONDS = 180.0
_DEFAULT_GAME_LAUNCH_SECONDS = 60.0
_DEFAULT_INSTANCE_INDEX = 0
_POLL_SECONDS = 1.0
_ADB_DEVICE = "device"
_ADB_ABSENT = "absent"


class MaaEnvironment:
    """environment 能力：探测只读，准备做前三步，其余动作失败关闭。"""

    def __init__(
        self,
        host: MachineHost | None = None,
        actions: EmulatorHost | None = None,
        wait: Callable[[float], None] = time.sleep,
        frames: FrameSource | None = None,
    ) -> None:
        self._host = host
        self._actions = actions
        self._wait = wait
        self._frames = frames

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        """按实例、窗口、adb、前台与分辨率给出环境状态；只读且可重复。

        实例状态先判：控制窗口在实例关闭时也留着，只按窗口会把「实例没起来」判成
        正在启动。``instance_state`` 读不到时退回窗口判据。
        """
        config = ctx.config
        host = self._host_for(config)
        index = _instance_index(config)
        instance = host.instance_state(index)
        early = _instance_verdict(instance, index)
        if early is not None:
            return _probe(ctx, early[0], early[1])
        title = _find_window(host.window_titles(), _text(config, "emulator_window_title"))
        if title is None:
            if instance is not None:
                return _probe(
                    ctx, EnvironmentState.BOOTING, f"实例 index {index} 已启动，窗口未出现"
                )
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
        """复用或拉起模拟器、确认 adb 与截图、把游戏拉到前台，再过主界面门控。"""
        config = ctx.config
        started_at = ctx.clock.utc_now()
        host = self._host_for(config)
        actions = self._actions if self._actions is not None else _build_actions(config)
        address = _text(config, "adb_address")
        game = _text(config, "game_package")
        try:
            if not self._instance_running(host, config):
                actions.start_instance(_instance_index(config))
            if not self._wait_adb(host, actions, config, ctx):
                return _failure(ctx, started_at, "E_ENV_UNREACHABLE", "adb 未在限期内连上模拟器")
            if host.screencap_size() is None:
                return _failure(ctx, started_at, "E_ENV_UNREACHABLE", "adb 在线但截图失败")
            if not actions.is_package_running(game):
                actions.launch_package(game)
            if not self._wait_foreground(host, game, config, ctx):
                return _failure(ctx, started_at, "E_ENV_NOT_READY", f"游戏未在限期内到前台：{game}")
        except OSError as error:
            return _failure(ctx, started_at, "E_ENV_UNREACHABLE", f"环境动作失败：{error}")
        gate_summary = ""
        if _is_gate_enabled(config):
            outcome = self._gate(config).run(ctx)
            gate_error = None if outcome.passed else outcome.error_code
            if gate_error is not None:
                return _failure(ctx, started_at, gate_error.value, outcome.detail)
            gate_summary = f"；{outcome.detail}"
        return PhaseResult(
            ok=True,
            detail=f"环境就绪：{address} 在线，{game} 在前台{gate_summary}",
            started_at=started_at,
            ended_at=ctx.clock.utc_now(),
        )

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        """断开并重连 adb、结束并重新拉起游戏，最后确认游戏在前台。"""
        config = ctx.config
        started_at = ctx.clock.utc_now()
        host = self._host_for(config)
        actions = self._actions if self._actions is not None else _build_actions(config)
        game = _text(config, "game_package")
        try:
            actions.disconnect_adb()
            actions.connect_adb()
            if not self._wait_adb(host, actions, config, ctx):
                return _failure(ctx, started_at, "E_ENV_DISCONNECTED", "复位后 adb 未连上模拟器")
            actions.stop_package(game)
            actions.launch_package(game)
            if not self._wait_foreground(host, game, config, ctx):
                return _failure(ctx, started_at, "E_ENV_NOT_READY", f"复位后游戏未到前台：{game}")
        except OSError as error:
            return _failure(ctx, started_at, "E_ENV_DISCONNECTED", f"复位动作失败：{error}")
        return PhaseResult(
            ok=True,
            detail=f"环境已复位：{game} 在前台",
            started_at=started_at,
            ended_at=ctx.clock.utc_now(),
        )

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        """``keep_warm`` 时保留游戏与实例；否则先结束游戏进程再关实例。"""
        config = ctx.config
        started_at = ctx.clock.utc_now()
        ended_at = ctx.clock.utc_now()
        actions = self._actions if self._actions is not None else _build_actions(config)
        game = _text(config, "game_package")
        if keep_warm:
            return PhaseResult(
                ok=True,
                detail="保温：保留游戏与模拟器",
                started_at=started_at,
                ended_at=ended_at,
            )
        try:
            actions.stop_package(game)
            actions.shutdown_instance(_instance_index(config))
        except OSError as error:
            return _failure(ctx, started_at, "E_ENV_UNREACHABLE", f"清理动作失败：{error}")
        return PhaseResult(
            ok=True,
            detail="环境已清理：游戏与模拟器已关闭",
            started_at=started_at,
            ended_at=ctx.clock.utc_now(),
        )

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        """确认游戏在前台；不在就拉起并等，保持环境可执行。

        ``minutes=0`` 是复用确认：只确认现场可用，不延长保温窗口。
        """
        config = ctx.config
        started_at = ctx.clock.utc_now()
        host = self._host_for(config)
        actions = self._actions if self._actions is not None else _build_actions(config)
        game = _text(config, "game_package")
        label = "保温" if minutes > 0 else "复用确认"
        try:
            if host.foreground_package() != game:
                if not actions.is_package_running(game):
                    actions.launch_package(game)
                if not self._wait_foreground(host, game, config, ctx):
                    return _failure(
                        ctx, started_at, "E_ENV_NOT_READY", f"{label}失败：游戏未到前台：{game}"
                    )
        except OSError as error:
            return _failure(ctx, started_at, "E_ENV_UNREACHABLE", f"{label}动作失败：{error}")
        if minutes > 0:
            detail = f"保温 {minutes} 分钟：{game} 在前台"
        else:
            detail = f"复用确认：{game} 在前台"
        return PhaseResult(
            ok=True,
            detail=detail,
            started_at=started_at,
            ended_at=ctx.clock.utc_now(),
        )

    def _host_for(self, config: Mapping[str, Any]) -> MachineHost:
        """真机只读端口按需构造：测试注入假件，生产用 Windows 实现。"""
        return self._host if self._host is not None else _build_host(config)

    def _gate(self, config: Mapping[str, Any]) -> PageGate:
        """主界面门控按需装配：测试注入帧源，生产用 adb 截帧实现。"""
        frames = self._frames if self._frames is not None else _build_frames(config)
        return PageGate(frames=frames, wait=self._wait)

    def _instance_running(self, host: MachineHost, config: Mapping[str, Any]) -> bool:
        """实例在跑就不重复拉起；info 读不到时退回窗口判据。"""
        index = _instance_index(config)
        instance = host.instance_state(index)
        if instance is not None:
            return instance.is_process_started
        titles = host.window_titles()
        return _find_window(titles, _text(config, "emulator_window_title")) is not None

    def _wait_adb(
        self,
        host: MachineHost,
        actions: EmulatorHost,
        config: Mapping[str, Any],
        ctx: RunContext,
    ) -> bool:
        """等 adb 设备上线；未上线时先重连一次，再按配置上限轮询。"""
        if host.adb_device_state() != _ADB_DEVICE:
            actions.connect_adb()
        deadline = ctx.clock.monotonic() + _seconds(
            config, "mumu_boot_timeout_s", _DEFAULT_MUMU_BOOT_SECONDS
        )
        while ctx.clock.monotonic() < deadline:
            if host.adb_device_state() == _ADB_DEVICE:
                return True
            self._wait(_POLL_SECONDS)
        return host.adb_device_state() == _ADB_DEVICE

    def _wait_foreground(
        self,
        host: MachineHost,
        game: str,
        config: Mapping[str, Any],
        ctx: RunContext,
    ) -> bool:
        """等游戏到前台；按配置上限轮询，不猜画面内容。"""
        deadline = ctx.clock.monotonic() + _seconds(
            config, "game_launch_timeout_s", _DEFAULT_GAME_LAUNCH_SECONDS
        )
        while ctx.clock.monotonic() < deadline:
            if host.foreground_package() == game:
                return True
            self._wait(_POLL_SECONDS)
        return host.foreground_package() == game


def _probe(ctx: RunContext, state: EnvironmentState, detail: str) -> EnvironmentProbe:
    """构造带注入时钟时间戳的探测结果。"""
    return EnvironmentProbe(state=state, detail=detail, observed_at=ctx.clock.utc_now())


def _instance_verdict(
    instance: MumuInstance | None,
    index: int,
) -> tuple[EnvironmentState, str] | None:
    """实例状态先判：未启动、Android 未启动完成都轮不到窗口与 adb 判据。"""
    if instance is None:
        return None
    if not instance.is_process_started:
        return EnvironmentState.OFF, f"模拟器实例未启动：index {index}（{instance.name}）"
    if not instance.is_android_started:
        return EnvironmentState.BOOTING, f"实例 index {index} 已启动，Android 未启动完成"
    return None


def _failure(ctx: RunContext, started_at: datetime, code: str, detail: str) -> PhaseResult:
    """构造带错误码与起止时刻的准备失败结果。"""
    return PhaseResult(
        ok=False,
        error_code=code,
        detail=detail,
        started_at=started_at,
        ended_at=ctx.clock.utc_now(),
    )


def _find_window(titles: Sequence[str], needle: str) -> str | None:
    """按子串找模拟器窗口；找不到返回 None，不做模糊猜测。"""
    for title in titles:
        if needle in title:
            return title
    return None


def _is_gate_enabled(config: Mapping[str, Any]) -> bool:
    """主界面门控开关；未配置或写法非法按开启处理（schema 默认 true）。"""
    value = config.get("page_gate")
    return value if isinstance(value, bool) else True


def _build_frames(config: Mapping[str, Any]) -> FrameSource:
    """按配置拼出 adb 帧端口：与真机端口同一目录树取 adb。"""
    mumu_dir = Path(str(config.get("mumu_dir", _DEFAULT_MUMU_DIR)))
    return AdbFrameSource(
        adb_path=mumu_dir / "nx_main" / "adb.exe", address=_text(config, "adb_address")
    )


def _text(config: Mapping[str, Any], key: str) -> str:
    """取配置里的字符串值；缺省值按 schema 默认值补齐。"""
    defaults = {
        "emulator_window_title": _DEFAULT_WINDOW_TITLE,
        "adb_address": _DEFAULT_ADB_ADDRESS,
        "game_package": _DEFAULT_GAME_PACKAGE,
    }
    return str(config.get(key, defaults[key]))


def _seconds(config: Mapping[str, Any], key: str, default: float) -> float:
    """取配置里的秒数上限；非正数或非整数按默认值处理，避免死等或立刻超时。"""
    value = config.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return default
    return float(value) if value > 0 else default


def _instance_index(config: Mapping[str, Any]) -> int:
    """取模拟器实例序号；非法值回落到 0。"""
    value = config.get("mumu_instance_index", _DEFAULT_INSTANCE_INDEX)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return _DEFAULT_INSTANCE_INDEX
    return value


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
    """按配置拼出真机只读端口；adb 与 MuMu 装在同一目录树下。"""
    mumu_dir = Path(str(config.get("mumu_dir", _DEFAULT_MUMU_DIR)))
    return WindowsMachineHost(
        adb_path=mumu_dir / "nx_main" / "adb.exe",
        address=_text(config, "adb_address"),
    )


def _build_actions(config: Mapping[str, Any]) -> EmulatorHost:
    """按配置拼出模拟器动作端口。"""
    mumu_dir = Path(str(config.get("mumu_dir", _DEFAULT_MUMU_DIR)))
    return WindowsEmulatorHost(mumu_dir=mumu_dir, adb_address=_text(config, "adb_address"))
