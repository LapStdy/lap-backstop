"""MAA 验证能力（docs/plugins/maa.md 验证节，Verifier 协议见 docs/05）。

不读日志也不截图：写一条记录判定过程的 log 证据，结论固定 ``unknown``。
证据与结论同源同刻，同一 RunContext 重复调用结果稳定。
"""

from __future__ import annotations

from pathlib import Path

from contracts import (
    CheckResult,
    CheckStatus,
    RunContext,
    Verdict,
    VerdictStatus,
)

DEFAULT_POLICY_ID = "maa-policy"
_REASON = "验证不可用：脚本与画面证据均未采集"


class MaaVerifier:
    """verifier 能力：证据只描述判定过程，结论固定不明。"""

    def verify(self, ctx: RunContext) -> Verdict:
        """写入判定记录证据并返回 ``unknown``；不降级为完成或失败。"""
        evidence_dir = Path(ctx.evidence_dir)
        evidence_dir.mkdir(parents=True, exist_ok=True)
        (evidence_dir / "verify.log").write_text(f"{_REASON}\n", encoding="utf-8")
        evidence_id = ctx.record_evidence("log", Path("verify.log"), "验证不可用")
        now = ctx.clock.utc_now()
        check = CheckResult(
            name="maa-verify",
            status=CheckStatus.UNKNOWN,
            detail=_REASON,
            evidence_ids=(evidence_id,),
            at=now,
        )
        return Verdict(
            status=VerdictStatus.UNKNOWN,
            policy_id=DEFAULT_POLICY_ID,
            confidence=0.0,
            reason=_REASON,
            checks=(check,),
            evidence_ids=(evidence_id,),
            created_at=now,
        )
