"""测试套的共享夹具：通知假渠道按名取用，supervisor 场景夹具经插件注册可见。"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tests.notify_support import FakeServer

pytest_plugins = ["tests.scenarios.supervisor_scenario_support"]


@pytest.fixture
def fake_server() -> Iterator[FakeServer]:
    """可脚本化的本地假渠道；每个用例独立实例，互不串话。"""
    server = FakeServer()
    try:
        yield server
    finally:
        server.close()
