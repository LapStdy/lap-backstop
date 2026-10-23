"""测试插件：把本次调用是否被取消写进探测结果。"""

from __future__ import annotations

from contracts import EnvironmentProbe, EnvironmentState


class Environment:
    def probe(self, env, ctx):
        detail = f"cancelled={ctx.cancel.is_cancelled()}"
        return EnvironmentProbe(state=EnvironmentState.READY, detail=detail)


class Plugin:
    environment = Environment()


PLUGIN = Plugin()
