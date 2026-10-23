"""worker 协议：消息编解码与 RunContext 载荷。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from contracts import RunContext
from core.clock import VirtualClock
from core.plugins import rpc
from core.plugins.errors import WorkerPayloadError

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)


class _Cancel:
    def is_cancelled(self) -> bool:
        return False


class _Logger:
    def debug(self, fields: Any) -> None:
        return None

    def info(self, fields: Any) -> None:
        return None

    def warn(self, fields: Any) -> None:
        return None

    def error(self, fields: Any) -> None:
        return None


def make_context() -> RunContext:
    clock = VirtualClock(NOW)
    return RunContext(
        run_id="run-1",
        attempt=2,
        template_id="template-1",
        plugin_id="fake",
        environment_id="env-1",
        config={"scenario": "instant", "slow_probes": 2},
        work_dir=Path("D:/data/work/run-1"),
        evidence_dir=Path("D:/data/evidence/run-1"),
        deadline_at=NOW + timedelta(minutes=30),
        cancel=_Cancel(),
        clock=clock,
        secrets={"token": "s3cret"},
        log=_Logger(),
        emit=lambda event, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )


def test_line_round_trip_keeps_newlines_escaped() -> None:
    message = {"kind": rpc.NOTIFY, "text": "第一行\n第二行"}
    line = rpc.encode_line(message)
    assert "\n" not in line
    assert rpc.decode_line(line) == message


def test_line_rejects_invalid_json() -> None:
    with pytest.raises(WorkerPayloadError):
        rpc.decode_line("{不是 JSON")


def test_line_rejects_non_object() -> None:
    with pytest.raises(WorkerPayloadError):
        rpc.decode_line("[1, 2, 3]")


def test_context_round_trip() -> None:
    payload = rpc.decode_context(rpc.encode_context(make_context()))
    assert payload.run_id == "run-1"
    assert payload.attempt == 2
    assert payload.config == {"scenario": "instant", "slow_probes": 2}
    assert payload.work_dir == Path("D:/data/work/run-1")
    assert payload.deadline_at == NOW + timedelta(minutes=30)
    assert payload.secrets == {"token": "s3cret"}


def test_context_rejects_naive_deadline() -> None:
    encoded = rpc.encode_context(make_context())
    encoded["deadline_at"] = "2026-10-23T12:30:00"
    with pytest.raises(WorkerPayloadError):
        rpc.decode_context(encoded)


def test_context_rejects_missing_secrets() -> None:
    encoded = rpc.encode_context(make_context())
    del encoded["secrets"]
    with pytest.raises(WorkerPayloadError, match="secrets"):
        rpc.decode_context(encoded)
