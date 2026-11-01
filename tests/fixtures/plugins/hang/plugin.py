"""测试插件：准备阶段拉起长命子进程后阻塞，用来验证停机强制终止会结束整棵进程树。

自身与子进程的 pid 按 config 的 ``pids_file`` 落盘，测试据此判定进程树是否还活着。
执行器装配要求三个能力齐全，script 与 verifier 给最小实现：正常路径停在 prepare，
永远走不到它们。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from contracts import ScriptHandle, ScriptObservation, ScriptState, Verdict, VerdictStatus

_BLOCK_SECONDS = 120


class Environment:
    def prepare(self, env, ctx):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
        Path(str(ctx.config["pids_file"])).write_text(
            json.dumps({"pid": os.getpid(), "child_pid": child.pid}), encoding="utf-8"
        )
        time.sleep(_BLOCK_SECONDS)
        return None


class Script:
    def launch(self, script, ctx):
        return {"id": "hang-script"}

    def observe(self, handle: ScriptHandle, ctx) -> ScriptObservation:
        return ScriptObservation(state=ScriptState.EXITED, since=ctx.clock.utc_now())

    def stop(self, handle, ctx, mode):
        return {"ok": True}

    def recover(self, handle, ctx, action):
        return {"ok": True}


class Verifier:
    def verify(self, ctx):
        return Verdict(
            status=VerdictStatus.COMPLETED,
            policy_id="hang-policy",
            confidence=1.0,
            reason="测试桩：正常路径停在 prepare，验证阶段走不到",
        )


class Plugin:
    environment = Environment()
    script = Script()
    verifier = Verifier()


PLUGIN = Plugin()
