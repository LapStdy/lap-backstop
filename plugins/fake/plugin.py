"""fake 插件：确定性的环境、脚本与验证器，供内核测试使用。

七种形态由配置的 ``scenario`` 选择：instant、slow、never-ready、fail、hang、partial、crash。
不读写真实系统、不 sleep；耗时与超时由调用方注入的虚拟时钟驱动。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from contracts import (
    CheckResult,
    CheckStatus,
    EnvironmentProbe,
    EnvironmentProvider,
    EnvironmentRef,
    EnvironmentState,
    Manifest,
    PhaseResult,
    RecoveryAction,
    RecoveryResult,
    RunContext,
    ScriptAdapter,
    ScriptHandle,
    ScriptObservation,
    ScriptRef,
    ScriptState,
    Signal,
    StopMode,
    StopResult,
    Verdict,
    VerdictStatus,
    Verifier,
)

SCENARIOS = frozenset({"instant", "slow", "never-ready", "fail", "hang", "partial", "crash"})
DEFAULT_SCENARIO = "instant"
DEFAULT_SLOW_PROBES = 3
DEFAULT_POLICY_ID = "fake-policy"

_MANIFEST_PATH = Path(__file__).with_name("plugin.json")


class FakeEnvironmentProvider:
    """按 scenario 返回探针与动作结果；slow 形态按探测次数逐步就绪。"""

    def __init__(self) -> None:
        self._probes: dict[str, int] = {}

    def probe(self, env: EnvironmentRef, ctx: RunContext) -> EnvironmentProbe:
        scenario = _scenario(ctx.config)
        now = ctx.clock.utc_now()
        if scenario == "never-ready":
            state = EnvironmentState.BOOTING
        elif scenario == "slow":
            count = self._probes.get(env.id, 0) + 1
            self._probes[env.id] = count
            threshold = _slow_probes(ctx.config)
            state = EnvironmentState.READY if count > threshold else EnvironmentState.BOOTING
        else:
            state = EnvironmentState.READY
        return EnvironmentProbe(state=state, detail=f"fake:{scenario}", observed_at=now)

    def prepare(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        scenario = _scenario(ctx.config)
        now = ctx.clock.utc_now()
        if scenario == "never-ready":
            return PhaseResult(
                ok=False,
                error_code="E_ENV_WAKE_TIMEOUT",
                detail="fake 环境不会就绪",
                started_at=now,
                ended_at=now,
            )
        return PhaseResult(ok=True, detail="fake 准备完成", started_at=now, ended_at=now)

    def reset(self, env: EnvironmentRef, ctx: RunContext) -> PhaseResult:
        return PhaseResult(ok=True, detail="fake 复位完成")

    def cleanup(self, env: EnvironmentRef, ctx: RunContext, keep_warm: bool) -> PhaseResult:
        return PhaseResult(ok=True, detail=f"fake 清理完成 keep_warm={keep_warm}")

    def warm(self, env: EnvironmentRef, ctx: RunContext, minutes: int) -> PhaseResult:
        return PhaseResult(ok=True, detail=f"fake 保温 {minutes} 分钟")


class FakeScriptAdapter:
    """按 scenario 返回脚本观察结果；crash 形态在启动时直接失败。"""

    def launch(self, script: ScriptRef, ctx: RunContext) -> ScriptHandle:
        scenario = _scenario(ctx.config)
        if scenario == "crash":
            raise RuntimeError("fake 脚本启动即崩溃")
        return ScriptHandle(id=f"fake-{ctx.run_id}", meta={"scenario": scenario})

    def observe(self, handle: ScriptHandle, ctx: RunContext) -> ScriptObservation:
        scenario = _scenario(ctx.config)
        now = ctx.clock.utc_now()
        if scenario == "hang":
            return ScriptObservation(state=ScriptState.HUNG, since=now)
        kind = {
            "fail": "task_failed",
            "partial": "partial",
        }.get(scenario, "completed")
        signal = Signal(kind=kind, payload={"scenario": scenario}, at=now)
        progress = 1.0 if scenario in {"instant", "fail"} else 0.5
        return ScriptObservation(
            state=ScriptState.EXITED,
            signals=(signal,),
            last_signal=signal,
            progress=progress,
            since=now,
        )

    def stop(self, handle: ScriptHandle, ctx: RunContext, mode: StopMode) -> StopResult:
        return StopResult(ok=True, detail=f"fake 停止 {mode.value}")

    def recover(
        self,
        handle: ScriptHandle,
        ctx: RunContext,
        action: RecoveryAction,
    ) -> RecoveryResult:
        return RecoveryResult(ok=True, detail=f"fake 恢复 {action.value}")


class FakeVerifier:
    """按 scenario 返回三态结论；partial 与 hang 形态给 unknown。"""

    def verify(self, ctx: RunContext) -> Verdict:
        scenario = _scenario(ctx.config)
        now = ctx.clock.utc_now()
        evidence_dir = Path(ctx.evidence_dir)
        evidence_dir.mkdir(parents=True, exist_ok=True)
        (evidence_dir / "fake.log").write_text(f"fake {scenario}\n", encoding="utf-8")
        evidence_id = ctx.record_evidence("log", Path("fake.log"), f"fake {scenario}")
        if scenario in {"partial", "hang"}:
            status = VerdictStatus.UNKNOWN
            reason = "部分任务完成" if scenario == "partial" else "脚本挂起"
            check_status = CheckStatus.UNKNOWN
        elif scenario == "fail":
            status = VerdictStatus.FAILED
            reason = "任务失败"
            check_status = CheckStatus.FAILED
        else:
            status = VerdictStatus.COMPLETED
            reason = "任务完成"
            check_status = CheckStatus.OK
        return Verdict(
            status=status,
            policy_id=_policy_id(ctx.config),
            confidence=0.5 if scenario in {"partial", "hang"} else 1.0,
            reason=reason,
            checks=(
                CheckResult(
                    name="fake",
                    status=check_status,
                    detail=reason,
                    evidence_ids=(evidence_id,),
                    at=now,
                ),
            ),
            evidence_ids=(evidence_id,),
            created_at=now,
        )


@dataclass(frozen=True, slots=True)
class FakePlugin:
    """插件入口对象：manifest 与按能力分组的实现。"""

    manifest: Manifest
    environment: EnvironmentProvider
    script: ScriptAdapter
    verifier: Verifier


def load_manifest(path: Path = _MANIFEST_PATH) -> Manifest:
    """读取并校验 plugin.json。"""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("plugin.json 根必须是对象")
    return Manifest.from_dict(data)


def _scenario(config: Mapping[str, Any]) -> str:
    value = config.get("scenario", DEFAULT_SCENARIO)
    scenario = value if isinstance(value, str) else DEFAULT_SCENARIO
    if scenario not in SCENARIOS:
        raise ValueError(f"未知 fake 形态：{scenario!r}")
    return scenario


def _slow_probes(config: Mapping[str, Any]) -> int:
    value = config.get("slow_probes", DEFAULT_SLOW_PROBES)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return DEFAULT_SLOW_PROBES


def _policy_id(config: Mapping[str, Any]) -> str:
    value = config.get("policy_id", DEFAULT_POLICY_ID)
    return value if isinstance(value, str) and value else DEFAULT_POLICY_ID


PLUGIN = FakePlugin(
    manifest=load_manifest(),
    environment=FakeEnvironmentProvider(),
    script=FakeScriptAdapter(),
    verifier=FakeVerifier(),
)
