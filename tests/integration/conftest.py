"""通知集成测试的 fixtures：临时库、假渠道与可注入的服务工厂。"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from core.clock import VirtualClock
from core.notify.service import NotifyService
from storage import migrate, open_database
from tests.notify_support import ENABLED, NOW, StubChannel, insert_template


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    database = open_database(tmp_path / "backstop.sqlite")
    migrate(database)
    insert_template(database)
    try:
        yield database
    finally:
        database.close()


@pytest.fixture
def stub() -> StubChannel:
    return StubChannel()


@pytest.fixture
def service_factory(
    connection: sqlite3.Connection, stub: StubChannel
) -> Iterator[Callable[..., NotifyService]]:
    created: list[NotifyService] = []

    def make(
        *, settings: dict[str, Any] | None = None, waits: list[float] | None = None
    ) -> NotifyService:
        service = NotifyService(
            connection,
            VirtualClock(NOW),
            data_root=Path("data"),
            settings_reader=lambda: dict(ENABLED) if settings is None else settings,
            channel_factory=lambda config: stub,
            wait=waits.append if waits is not None else (lambda seconds: None),
        )
        created.append(service)
        return service

    yield make
    for service in created:
        service.close()
