"""测试插件：按调用配置发一条 emit，用来验证插件事件白名单与载荷校验。"""

from __future__ import annotations

from contracts import EnvironmentProbe, EnvironmentState


class Environment:
    def probe(self, env, ctx):
        event = str(ctx.config.get("event", "run.signal"))
        payload = dict(ctx.config.get("payload", {}))
        ctx.emit(event, payload)
        return EnvironmentProbe(state=EnvironmentState.READY, detail="emitted")


class Plugin:
    environment = Environment()


PLUGIN = Plugin()
