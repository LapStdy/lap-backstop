"""notify.* 配置解析：缺省关闭不报错，enabled 时按渠道校验并指出键名。"""

from __future__ import annotations

from typing import Any

import pytest

from contracts import ContractError
from core.notify.config import resolve_config


def test_default_settings_keep_notify_disabled() -> None:
    config = resolve_config({})
    assert not config.enabled
    assert config.channel == ""
    assert config.attempts >= 1


def test_defaults_fill_numeric_fields_when_enabled() -> None:
    config = resolve_config({"notify.enabled": True, "notify.token": "SCT123"})
    assert config.enabled
    assert config.channel == "serverchan"
    assert config.token == "SCT123"
    assert config.timeout_seconds == 10.0
    assert config.attempts == 3
    assert config.backoff_seconds == (5.0, 30.0)
    assert not config.use_proxy


def test_serverchan_requires_token() -> None:
    with pytest.raises(ContractError) as excinfo:
        resolve_config({"notify.enabled": True, "notify.channel": "serverchan"})
    assert "notify.token" in excinfo.value.message


def test_dingtalk_requires_webhook() -> None:
    with pytest.raises(ContractError) as excinfo:
        resolve_config({"notify.enabled": True, "notify.channel": "dingtalk"})
    assert "notify.webhook" in excinfo.value.message


def test_dingtalk_webhook_must_be_http_url() -> None:
    settings: dict[str, Any] = {
        "notify.enabled": True,
        "notify.channel": "dingtalk",
        "notify.webhook": "not-a-url",
    }
    with pytest.raises(ContractError) as excinfo:
        resolve_config(settings)
    assert "notify.webhook" in excinfo.value.message


def test_ntfy_requires_single_topic_url() -> None:
    base: dict[str, Any] = {"notify.enabled": True, "notify.channel": "ntfy"}
    for url in ("", "ftp://ntfy.sh/t", "https://ntfy.sh/a/b", "https://ntfy.sh"):
        with pytest.raises(ContractError) as excinfo:
            resolve_config({**base, "notify.url": url})
        assert "notify.url" in excinfo.value.message


def test_unknown_channel_rejected() -> None:
    with pytest.raises(ContractError) as excinfo:
        resolve_config({"notify.enabled": True, "notify.channel": "wechat"})
    assert "notify.channel" in excinfo.value.message


def test_disabled_config_skips_deep_validation() -> None:
    config = resolve_config({"notify.enabled": False, "notify.channel": "wechat"})
    assert not config.enabled


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("notify.enabled", "yes"),
        ("notify.timeout_seconds", 0),
        ("notify.timeout_seconds", -1),
        ("notify.attempts", 0),
        ("notify.backoff_seconds", []),
        ("notify.backoff_seconds", [1, "x"]),
        ("notify.backoff_seconds", [1, -2]),
        ("notify.use_proxy", "true"),
    ],
)
def test_invalid_scalars_report_key(key: str, value: object) -> None:
    settings: dict[str, Any] = {
        "notify.enabled": True,
        "notify.channel": "dingtalk",
        "notify.webhook": "https://oapi.dingtalk.com/robot/send",
        key: value,
    }
    with pytest.raises(ContractError) as excinfo:
        resolve_config(settings)
    assert key in excinfo.value.message


def test_settings_override_defaults() -> None:
    config = resolve_config(
        {
            "notify.enabled": True,
            "notify.channel": "dingtalk",
            "notify.webhook": "https://oapi.dingtalk.com/robot/send",
            "notify.token": "SEC1",
            "notify.timeout_seconds": 2.5,
            "notify.attempts": 5,
            "notify.backoff_seconds": [1, 2, 4],
            "notify.use_proxy": True,
        }
    )
    assert config.channel == "dingtalk"
    assert config.webhook == "https://oapi.dingtalk.com/robot/send"
    assert config.token == "SEC1"
    assert config.timeout_seconds == 2.5
    assert config.attempts == 5
    assert config.backoff_seconds == (1.0, 2.0, 4.0)
    assert config.use_proxy


def test_signature_tracks_channel_fields() -> None:
    first = resolve_config({"notify.enabled": True, "notify.token": "SCT123"})
    second = resolve_config({"notify.enabled": True, "notify.token": "SCT123"})
    third = resolve_config({"notify.enabled": True, "notify.token": "SCT456"})
    assert first.signature == second.signature
    assert first.signature != third.signature
    assert first.signature != resolve_config({}).signature


def test_serverchan_url_override_must_be_http() -> None:
    with pytest.raises(ContractError) as excinfo:
        resolve_config(
            {"notify.enabled": True, "notify.token": "SCT123", "notify.url": "gopher://x"}
        )
    assert "notify.url" in excinfo.value.message


def test_ntfy_accepts_token_and_topic() -> None:
    config = resolve_config(
        {
            "notify.enabled": True,
            "notify.channel": "ntfy",
            "notify.url": "https://ntfy.sh/t",
            "notify.token": "tk",
        }
    )
    assert config.url == "https://ntfy.sh/t"
    assert config.token == "tk"
