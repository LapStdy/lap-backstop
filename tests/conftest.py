"""通知相关测试共享的 fixture；单元与集成测试都能按名取用。"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tests.notify_support import FakeServer


@pytest.fixture
def fake_server() -> Iterator[FakeServer]:
    """可脚本化的本地假渠道；每个用例独立实例，互不串话。"""
    server = FakeServer()
    try:
        yield server
    finally:
        server.close()
