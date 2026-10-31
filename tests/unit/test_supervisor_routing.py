"""派发路由的取舍口径：可用优先、全降级报错、单插件维持现状、判定实时读取。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from contracts import Logger, RunState
from core.supervisor_routing import (
    ALL_DEGRADED_CODE,
    ROUTE_ALL_DEGRADED_EVENT,
    ROUTE_SKIPPED_EVENT,
    RouteAnnouncer,
    RouteCandidate,
    route_dispatch,
)
from storage.repositories.job_runs import JobRunRecord

T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
REASON_A = "连续 3 次失败（启动超时）"
REASON_B = "连续 3 次失败（调用中退出）"


def _candidate(run_id: str, plugin_id: str) -> RouteCandidate:
    record = JobRunRecord(
        id=run_id,
        template_id=f"template-{run_id}",
        schedule_id=None,
        scheduled_at=None,
        created_at=T0,
        idempotency_key=run_id,
        state=RunState.QUEUED,
        attempt=1,
        priority=0,
        deadline_at=None,
        started_at=None,
        finished_at=None,
        verdict_id=None,
        error_code=None,
        detail={},
    )
    return RouteCandidate(run=record, plugin_id=plugin_id)


class _RecordingLog:
    """记录结构化日志调用；断言路由决策的调试留痕。"""

    def __init__(self) -> None:
        self.entries: list[tuple[str, Mapping[str, Any]]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("debug", fields))

    def info(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("info", fields))

    def warn(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("warn", fields))

    def error(self, fields: Mapping[str, Any]) -> None:
        self.entries.append(("error", fields))


def _announcer(
    log: Logger,
    emitted: list[tuple[str, Mapping[str, Any]]],
    degradation_of: Callable[[str], str | None],
) -> RouteAnnouncer:
    return RouteAnnouncer(
        log,
        lambda name, payload: emitted.append((name, dict(payload))),
        degradation_of,
    )


def _skips(emitted: list[tuple[str, Mapping[str, Any]]]) -> list[Mapping[str, Any]]:
    return [payload for name, payload in emitted if name == ROUTE_SKIPPED_EVENT]


def _blocks(emitted: list[tuple[str, Mapping[str, Any]]]) -> list[Mapping[str, Any]]:
    return [payload for name, payload in emitted if name == ROUTE_ALL_DEGRADED_EVENT]


def test_route_prefers_available_plugin_and_records_skip_reason() -> None:
    """两插件其一降级：路由到未降级那个，跳过原因随决策返回。"""
    degraded = {"fake-a": REASON_A}
    decision = route_dispatch(
        (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b")),
        degraded.get,
    )
    assert decision.chosen is not None
    assert decision.chosen.run.id == "run-b"
    assert decision.error_code is None
    assert [(skip.run_id, skip.plugin_id, skip.reason) for skip in decision.skipped] == [
        ("run-a", "fake-a", REASON_A)
    ]


def test_route_reports_all_degraded_with_each_reason() -> None:
    """两插件全部降级：不派发，返回约定错误码并带两条降级原因。"""
    degraded = {"fake-a": REASON_A, "fake-b": REASON_B}
    decision = route_dispatch(
        (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b")),
        degraded.get,
    )
    assert decision.chosen is None
    assert decision.error_code == ALL_DEGRADED_CODE
    assert [skip.plugin_id for skip in decision.skipped] == ["fake-a", "fake-b"]
    assert all(skip.reason for skip in decision.skipped)


def test_route_keeps_queue_head_when_single_plugin_degraded() -> None:
    """候选只有同一个降级插件：维持现状派发队首，不产生跳过记录。"""
    degraded = {"fake-a": REASON_A}
    decision = route_dispatch(
        (_candidate("run-a1", "fake-a"), _candidate("run-a2", "fake-a")),
        degraded.get,
    )
    assert decision.chosen is not None
    assert decision.chosen.run.id == "run-a1"
    assert decision.skipped == ()
    assert decision.error_code is None


def test_route_skips_only_candidates_ahead_of_choice() -> None:
    """排在可用候选之后的降级候选不算被跳过，等后续轮次自然处理。"""
    degraded = {"fake-a": REASON_A, "fake-c": REASON_B}
    decision = route_dispatch(
        (
            _candidate("run-a", "fake-a"),
            _candidate("run-b", "fake-b"),
            _candidate("run-c", "fake-c"),
        ),
        degraded.get,
    )
    assert decision.chosen is not None
    assert decision.chosen.run.id == "run-b"
    assert [skip.run_id for skip in decision.skipped] == ["run-a"]


def test_route_reads_degradation_fresh_on_each_call() -> None:
    """插件在两次请求之间恢复：第二次路由重新参与候选，不被缓存挡住。"""
    reasons: dict[str, str] = {"fake-a": REASON_A}
    candidates = (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b"))

    first = route_dispatch(candidates, reasons.get)
    assert first.chosen is not None
    assert first.chosen.run.id == "run-b"

    reasons.pop("fake-a")
    second = route_dispatch(candidates, reasons.get)
    assert second.chosen is not None
    assert second.chosen.run.id == "run-a"
    assert second.skipped == ()


def test_route_without_degradation_view_routes_queue_head() -> None:
    """没有降级视图时全部视为可用：路由退化为按出队顺序取队首。"""
    decision = route_dispatch(
        (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b")),
        lambda _plugin_id: None,
    )
    assert decision.chosen is not None
    assert decision.chosen.run.id == "run-a"
    assert decision.skipped == ()


def test_announcer_emits_skip_event_once_per_plugin_episode() -> None:
    """同一插件持续降级只报一次跳过事件，恢复后再次降级重新上报。"""
    log = _RecordingLog()
    emitted: list[tuple[str, Mapping[str, Any]]] = []
    reasons: dict[str, str] = {"fake-a": REASON_A}
    announcer = _announcer(log, emitted, reasons.get)
    decision = route_dispatch(
        (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b")),
        reasons.get,
    )

    announcer.record(decision)
    announcer.record(decision)
    skips = _skips(emitted)
    assert len(skips) == 1
    plugins = skips[0]["plugins"]
    assert isinstance(plugins, list) and len(plugins) == 1
    assert plugins[0]["plugin_id"] == "fake-a"
    assert skips[0]["chosen_plugin_id"] == "fake-b"

    reasons.pop("fake-a")
    announcer.record(route_dispatch((_candidate("run-b2", "fake-b"),), reasons.get))
    reasons["fake-a"] = REASON_B
    announcer.record(
        route_dispatch(
            (_candidate("run-a2", "fake-a"), _candidate("run-b3", "fake-b")),
            reasons.get,
        )
    )
    assert len(_skips(emitted)) == 2
    assert any(entry[1].get("event") == "supervisor.route" for entry in log.entries)


def test_announcer_emits_all_degraded_with_error_code() -> None:
    """全部降级事件带约定错误码与两条降级原因，且只报一次。"""
    log = _RecordingLog()
    emitted: list[tuple[str, Mapping[str, Any]]] = []
    degraded = {"fake-a": REASON_A, "fake-b": REASON_B}
    announcer = _announcer(log, emitted, degraded.get)
    decision = route_dispatch(
        (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b")),
        degraded.get,
    )

    announcer.record(decision)
    announcer.record(decision)
    blocked = _blocks(emitted)
    assert len(blocked) == 1
    assert blocked[0]["error_code"] == ALL_DEGRADED_CODE
    plugins = blocked[0]["plugins"]
    assert isinstance(plugins, list)
    assert {item["plugin_id"] for item in plugins} == {"fake-a", "fake-b"}


def test_announcer_reports_skip_after_all_degraded_episode() -> None:
    """全降级与跳过各记各的账：恢复一方再被跳过时仍补写跳过事件。"""
    log = _RecordingLog()
    emitted: list[tuple[str, Mapping[str, Any]]] = []
    reasons: dict[str, str] = {"fake-a": REASON_A, "fake-b": REASON_B}
    announcer = _announcer(log, emitted, reasons.get)

    announcer.record(
        route_dispatch(
            (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b")),
            reasons.get,
        )
    )
    assert len(_blocks(emitted)) == 1

    reasons.pop("fake-a")
    announcer.record(
        route_dispatch(
            (_candidate("run-b2", "fake-b"), _candidate("run-a2", "fake-a")),
            reasons.get,
        )
    )
    skips = _skips(emitted)
    assert len(skips) == 1
    plugins = skips[0]["plugins"]
    assert isinstance(plugins, list) and len(plugins) == 1
    assert plugins[0]["plugin_id"] == "fake-b"
    assert skips[0]["chosen_plugin_id"] == "fake-a"


def test_announcer_keeps_armed_while_plugin_degraded_without_route() -> None:
    """插件仍在降级但某轮没参与路由：不解除武装，恢复后再次降级才重新上报。"""
    log = _RecordingLog()
    emitted: list[tuple[str, Mapping[str, Any]]] = []
    reasons: dict[str, str] = {"fake-a": REASON_A}
    announcer = _announcer(log, emitted, reasons.get)

    announcer.record(
        route_dispatch(
            (_candidate("run-a", "fake-a"), _candidate("run-b", "fake-b")),
            reasons.get,
        )
    )
    # 单插件候选按现状派发队首：没有跳过记录，也不解除已上报的武装。
    announcer.record(route_dispatch((_candidate("run-a3", "fake-a"),), reasons.get))
    reasons.pop("fake-a")
    announcer.record(route_dispatch((_candidate("run-b3", "fake-b"),), reasons.get))
    reasons["fake-a"] = REASON_B
    announcer.record(
        route_dispatch(
            (_candidate("run-a4", "fake-a"), _candidate("run-b4", "fake-b")),
            reasons.get,
        )
    )
    assert len(_skips(emitted)) == 2
