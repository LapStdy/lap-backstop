"""RPC 能力代理与装配：把 worker 通道包装成 contracts 的能力协议。

执行器只看到 ``contracts`` 的 Protocol；一次调用是否跨进程由这里决定（ADR 0010）。
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from contracts import (
    EnvironmentProbe,
    EnvironmentRef,
    Logger,
    PhaseResult,
    RecoveryAction,
    RecoveryResult,
    RunContext,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    StopMode,
    StopResult,
    Verdict,
)
from core.plugins import rpc_types
from core.plugins.channel import WorkerChannel
from core.plugins.errors import WorkerError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from core.plugins.worker_options import WorkerConfig


class EnvironmentProxy:
    """环境能力：每次调用发给 worker 子进程。"""

    def __init__(self, channel: WorkerChannel) -> None:
        self._channel = channel

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        value = self._channel.call(
            "environment", "probe", {"env": rpc_types.encode_environment_ref(env)}, ctx
        )
        return rpc_types.decode_environment_probe(value)

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        value = self._channel.call(
            "environment", "prepare", {"env": rpc_types.encode_environment_ref(env)}, ctx
        )
        return rpc_types.decode_phase_result(value)

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        value = self._channel.call(
            "environment", "reset", {"env": rpc_types.encode_environment_ref(env)}, ctx
        )
        return rpc_types.decode_phase_result(value)

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        args = {"env": rpc_types.encode_environment_ref(env), "keep_warm": keep_warm}
        return rpc_types.decode_phase_result(
            self._channel.call("environment", "cleanup", args, ctx)
        )

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        args = {"env": rpc_types.encode_environment_ref(env), "minutes": minutes}
        return rpc_types.decode_phase_result(self._channel.call("environment", "warm", args, ctx))


class ScriptProxy:
    """脚本能力：句柄与观察结果按 JSON 往返。"""

    def __init__(self, channel: WorkerChannel) -> None:
        self._channel = channel

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        value = self._channel.call(
            "script", "launch", {"script": rpc_types.encode_script_ref(script)}, ctx
        )
        return rpc_types.decode_script_handle(value)

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        value = self._channel.call(
            "script", "observe", {"handle": rpc_types.encode_script_handle(handle)}, ctx
        )
        return rpc_types.decode_script_observation(value)

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult:
        args = {"handle": rpc_types.encode_script_handle(handle), "mode": mode.value}
        return rpc_types.decode_stop_result(self._channel.call("script", "stop", args, ctx))

    def recover(
        self,
        handle: ScriptHandle,
        ctx: RunContext,
        action: RecoveryAction,
    ) -> RecoveryResult:
        args = {"handle": rpc_types.encode_script_handle(handle), "action": action.value}
        return rpc_types.decode_recovery_result(self._channel.call("script", "recover", args, ctx))


class VerifierProxy:
    """验证能力：结论与证据引用都按契约校验。"""

    def __init__(self, channel: WorkerChannel) -> None:
        self._channel = channel

    def verify(self, ctx: RunContext) -> Verdict:
        return rpc_types.decode_verdict(self._channel.call("verifier", "verify", {}, ctx))


def rpc_capabilities(channel: WorkerChannel) -> PluginCapabilities:
    """按 worker 声明的能力装配一份插件能力表。"""
    declared = channel.capabilities
    return PluginCapabilities(
        plugin_id=channel.plugin_id,
        environment=EnvironmentProxy(channel) if "environment" in declared else None,
        script=ScriptProxy(channel) if "script" in declared else None,
        verifier=VerifierProxy(channel) if "verifier" in declared else None,
    )


def installed_plugin_ids(plugins_root: Path) -> tuple[str, ...]:
    """扫描带 ``plugin.json`` 的插件目录。"""
    return tuple(sorted(path.parent.name for path in plugins_root.glob("*/plugin.json")))


def _manifest_version(plugins_root: Path, plugin_id: str) -> str:
    """读 manifest 里的版本；插件已启动说明文件可解析，读不到按空串记。"""
    try:
        data = json.loads((plugins_root / plugin_id / "plugin.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    version = data.get("version") if isinstance(data, dict) else None
    return version if isinstance(version, str) else ""


class RpcPluginHost:
    """按插件目录装配 RPC 能力表；启动失败的插件跳过，内核继续运行。"""

    def __init__(
        self,
        plugins_root: Path,
        plugin_ids: Sequence[str],
        *,
        log: Logger | None = None,
    ) -> None:
        self.registry = PluginRegistry()
        self._plugins_root = plugins_root
        self._log = log
        self._channels: dict[str, WorkerChannel] = {}
        self._rejected: dict[str, str] = {}
        self._versions: dict[str, str] = {}
        for plugin_id in plugin_ids:
            channel = WorkerChannel(plugin_id, WorkerConfig(plugins_root=plugins_root, log=log))
            try:
                channel.start()
            except (WorkerError, OSError) as error:
                # 起子进程本身也可能失败（可执行文件消失、权限、句柄耗尽）；
                # 插件装不上的后果只能是这个插件不可用，不能拖垮整个核心。
                self._rejected[plugin_id] = str(error)
                channel.close()
                if log is not None:
                    log.error(
                        {
                            "event": "plugin.start_failed",
                            "plugin_id": plugin_id,
                            "error": str(error),
                        }
                    )
                continue
            self._channels[plugin_id] = channel
            self._versions[plugin_id] = _manifest_version(plugins_root, plugin_id)
            self.registry.register(rpc_capabilities(channel))

    @property
    def rejected(self) -> tuple[str, ...]:
        """启动失败、未注册的插件 id。"""
        return tuple(self._rejected)

    @property
    def rejections(self) -> Mapping[str, str]:
        """启动失败的插件与原因，按启动顺序。"""
        return dict(self._rejected)

    @property
    def versions(self) -> Mapping[str, str]:
        """已注册插件的 manifest 版本。"""
        return dict(self._versions)

    @property
    def channels(self) -> Mapping[str, WorkerChannel]:
        """已启动的 worker 通道。"""
        return dict(self._channels)

    @property
    def degraded_plugin_ids(self) -> tuple[str, ...]:
        """处于降级期的插件；路由接入多插件后据此跳过。"""
        return tuple(
            sorted(
                plugin_id for plugin_id, channel in self._channels.items() if channel.is_degraded
            )
        )

    def close(self) -> None:
        """结束全部 worker 进程。"""
        for channel in self._channels.values():
            channel.close()
        self._channels.clear()

    def __enter__(self) -> RpcPluginHost:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
