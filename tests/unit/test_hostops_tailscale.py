"""tailscale 状态解析：serve 启动监听集合与 doctor 自检共用同一口径（docs/08）。"""

from __future__ import annotations

import json
import subprocess

from core.hostops import tailscale
from tests.fixtures.subprocess_fakes import completed


def status_payload(state: str, addresses: list[str]) -> bytes:
    return json.dumps({"BackendState": state, "TailscaleIPs": addresses}).encode()


def test_parse_running_keeps_tailnet_ipv4_only() -> None:
    payload = status_payload("Running", ["100.101.102.103", "fd7a:115c:a1e0::1", "192.168.1.5"])
    status = tailscale.parse_status_json(payload.decode())
    assert status.state == "Running"
    assert status.addresses == ("100.101.102.103",)
    assert status.is_running


def test_parse_tailnet_range_boundaries() -> None:
    payload = status_payload(
        "Running",
        ["100.63.0.1", "100.64.0.1", "100.127.255.254", "100.128.0.1"],
    )
    assert tailscale.parse_status_json(payload.decode()).addresses == (
        "100.64.0.1",
        "100.127.255.254",
    )


def test_parse_needs_login_and_stopped_are_not_running() -> None:
    login = tailscale.parse_status_json(status_payload("NeedsLogin", []).decode())
    stopped = tailscale.parse_status_json(status_payload("Stopped", ["100.64.0.1"]).decode())
    assert not login.is_running
    assert login.down_detail == "NeedsLogin"
    assert not stopped.is_running
    assert stopped.addresses == ("100.64.0.1",)


def test_parse_bad_json_and_missing_state_are_unavailable() -> None:
    bad = tailscale.parse_status_json("<html>gateway error</html>")
    empty = tailscale.parse_status_json(json.dumps({"Self": {}}))
    not_object = tailscale.parse_status_json("[1, 2]")
    for status in (bad, empty, not_object):
        assert status.state == ""
        assert status.addresses == ()


def test_parse_running_without_tailnet_ip_has_no_addresses() -> None:
    status = tailscale.parse_status_json(status_payload("Running", ["fd7a:115c:a1e0::1"]).decode())
    assert status.is_running
    assert status.addresses == ()


def test_parse_invalid_address_entries_are_dropped() -> None:
    payload = status_payload("Running", ["not-an-ip", "100.64.0.1"])
    assert tailscale.parse_status_json(payload.decode()).addresses == ("100.64.0.1",)


def test_probe_uses_status_command() -> None:
    seen: list[list[str]] = []

    def run(command: list[str]) -> subprocess.CompletedProcess[bytes]:
        seen.append(command)
        return completed(returncode=-1)

    tailscale.probe(run)
    assert seen == [list(tailscale.STATUS_COMMAND)]


def test_probe_command_missing_reports_reason() -> None:
    status = tailscale.probe(lambda command: completed(returncode=-1))
    assert status.state == ""
    assert status.note == "命令不可用"
    assert status.down_detail == "不可查：命令不可用"


def test_probe_nonzero_exit_reports_exit_code() -> None:
    status = tailscale.probe(lambda command: completed(returncode=1))
    assert status.state == ""
    assert status.note == "命令退出码 1"


def test_probe_parses_running_status() -> None:
    status = tailscale.probe(lambda command: completed(status_payload("Running", ["100.64.0.1"])))
    assert status.is_running
    assert status.addresses == ("100.64.0.1",)


def test_probe_bad_output_reports_unparseable() -> None:
    status = tailscale.probe(lambda command: completed(b"garbage bytes"))
    assert status.note == "输出不可解析"
    assert status.down_detail == "不可查：输出不可解析"


def test_default_runner_survives_missing_command() -> None:
    """命令缺失走 OSError 分支：按退出码 -1 返回，不抛错；-1 的解读由 probe 假件用例覆盖。"""
    outcome = tailscale.default_runner(["lap-backstop-test-no-such-command"])
    assert outcome.returncode == -1
