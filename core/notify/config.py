"""notify.* 配置解析：settings 表覆盖 defaults.toml，缺省关闭。

enabled=false 只解析开关键本身，其余键不生效也不校验（缺省关闭不报错）；
enabled=true 时按渠道校验必填项，非法抛 E_CONFIG_INVALID 并指出键名（docs/11 配置）。
"""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from contracts import ContractError, ErrorCode
from storage.defaults import default

ENABLED_KEY: Final[str] = "notify.enabled"
CHANNEL_KEY: Final[str] = "notify.channel"
TOKEN_KEY: Final[str] = "notify.token"
WEBHOOK_KEY: Final[str] = "notify.webhook"
URL_KEY: Final[str] = "notify.url"
USE_PROXY_KEY: Final[str] = "notify.use_proxy"

CHANNELS: Final[tuple[str, ...]] = ("serverchan", "dingtalk", "ntfy")
# 各渠道的必填与可选键：token 对 ServerChan 是 SendKey、对钉钉是加签密钥、对 ntfy 是访问令牌。
_REQUIRED_KEYS: Final[dict[str, tuple[str, ...]]] = {
    "serverchan": (TOKEN_KEY,),
    "dingtalk": (WEBHOOK_KEY,),
    "ntfy": (URL_KEY,),
}


@dataclass(frozen=True, slots=True)
class NotifyConfig:
    """生效的 notify 配置；enabled=false 时渠道字段无意义。"""

    enabled: bool
    channel: str
    token: str
    webhook: str
    url: str
    timeout_seconds: float
    attempts: int
    backoff_seconds: tuple[float, ...]
    use_proxy: bool

    @property
    def signature(self) -> tuple[object, ...]:
        """渠道相关字段的快照；变化即重建渠道。"""
        return (
            self.enabled,
            self.channel,
            self.token,
            self.webhook,
            self.url,
            self.timeout_seconds,
            self.attempts,
            self.backoff_seconds,
            self.use_proxy,
        )


def _invalid(message: str) -> ContractError:
    return ContractError(ErrorCode.CONFIG_INVALID, message)


def resolve_config(settings: Mapping[str, Any]) -> NotifyConfig:
    """读出生效配置。

    Args:
        settings: settings 表的键值快照（notify.* 覆盖 defaults.toml）。

    Returns:
        enabled=false 时渠道字段为占位空值；其余字段按默认值或 settings 解析。

    Raises:
        ContractError: enabled 键类型非法；enabled=true 时渠道取值、必填项或数值越界。
    """
    enabled = _enabled(settings)
    if not enabled:
        return NotifyConfig(
            enabled=False,
            channel="",
            token="",
            webhook="",
            url="",
            timeout_seconds=0.0,
            attempts=1,
            backoff_seconds=(),
            use_proxy=False,
        )
    channel = _string(settings, CHANNEL_KEY, str(default("notify", "channel")))
    if channel not in CHANNELS:
        raise _invalid(f"{CHANNEL_KEY} 必须是 {'/'.join(CHANNELS)} 之一，现在是 {channel!r}")
    for key in _REQUIRED_KEYS[channel]:
        value = _string(settings, key, "")
        if not value:
            raise _invalid(f"启用 {channel} 渠道需要非空的 {key}")
    candidate = NotifyConfig(
        enabled=True,
        channel=channel,
        token=_string(settings, TOKEN_KEY, ""),
        webhook=_string(settings, WEBHOOK_KEY, ""),
        url=_string(settings, URL_KEY, ""),
        timeout_seconds=_number(
            settings, "notify.timeout_seconds", default("notify", "timeout_seconds")
        ),
        attempts=_int(settings, "notify.attempts", default("notify", "attempts")),
        backoff_seconds=_backoff(settings),
        use_proxy=_bool(settings, USE_PROXY_KEY, default("notify", "use_proxy")),
    )
    _check_addresses(candidate)
    return candidate


def _enabled(settings: Mapping[str, Any]) -> bool:
    value = settings.get(ENABLED_KEY, default("notify", "enabled"))
    if not isinstance(value, bool):
        raise _invalid(f"{ENABLED_KEY} 必须是布尔值")
    return value


def _string(settings: Mapping[str, Any], key: str, fallback: str) -> str:
    value = settings.get(key, fallback)
    if not isinstance(value, str):
        raise _invalid(f"{key} 必须是字符串")
    return value


def _number(settings: Mapping[str, Any], key: str, fallback: float) -> float:
    value = settings.get(key, fallback)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _invalid(f"{key} 必须是数字")
    if value <= 0:
        raise _invalid(f"{key} 必须大于 0")
    return float(value)


def _int(settings: Mapping[str, Any], key: str, fallback: int) -> int:
    value = settings.get(key, fallback)
    if isinstance(value, bool) or not isinstance(value, int):
        raise _invalid(f"{key} 必须是整数")
    if value < 1:
        raise _invalid(f"{key} 至少为 1（含首次投递）")
    return value


def _backoff(settings: Mapping[str, Any]) -> tuple[float, ...]:
    key = "notify.backoff_seconds"
    value = settings.get(key, default("notify", "backoff_seconds"))
    if not isinstance(value, list) or not value:
        raise _invalid(f"{key} 必须是非空数字数组")
    delays = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)) or item < 0:
            raise _invalid(f"{key} 的每一项都必须是不小于 0 的数字")
        delays.append(float(item))
    return tuple(delays)


def _bool(settings: Mapping[str, Any], key: str, fallback: bool) -> bool:
    value = settings.get(key, fallback)
    if not isinstance(value, bool):
        raise _invalid(f"{key} 必须是布尔值")
    return value


def _check_addresses(config: NotifyConfig) -> None:
    """必填项之外的地址形态规则集中在这里。

    Raises:
        ContractError: 钉钉 webhook 不是 http(s) 地址；ServerChan 或 ntfy 的 url 形态不对。
    """
    if config.channel == "dingtalk":
        parts = urllib.parse.urlsplit(config.webhook)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise _invalid(f"{WEBHOOK_KEY} 必须是 http(s) 地址")
    if config.channel == "serverchan" and config.url:
        parts = urllib.parse.urlsplit(config.url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise _invalid(f"{URL_KEY} 必须是 http(s) 地址")
    if config.channel == "ntfy":
        parts = urllib.parse.urlsplit(config.url)
        topic = parts.path.strip("/")
        if parts.scheme not in ("http", "https") or not parts.netloc or not topic or "/" in topic:
            raise _invalid(f"{URL_KEY} 必须是 http(s) 且指向单个主题（如 https://ntfy.sh/主题）")


__all__ = [
    "CHANNELS",
    "CHANNEL_KEY",
    "ENABLED_KEY",
    "TOKEN_KEY",
    "URL_KEY",
    "USE_PROXY_KEY",
    "WEBHOOK_KEY",
    "NotifyConfig",
    "resolve_config",
]
