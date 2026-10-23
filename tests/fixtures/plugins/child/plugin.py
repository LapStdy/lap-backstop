"""测试插件：拉起一个长命子进程后阻塞，用来验证强杀会结束整棵进程树。"""

from __future__ import annotations

import subprocess
import sys
import time

from contracts import EnvironmentProbe, EnvironmentState


class Environment:
    def probe(self, env, ctx):
        ctx.work_dir.mkdir(parents=True, exist_ok=True)
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        (ctx.work_dir / "child.pid").write_text(str(child.pid), encoding="utf-8")
        time.sleep(60)
        return EnvironmentProbe(state=EnvironmentState.READY)


class Plugin:
    environment = Environment()


PLUGIN = Plugin()
