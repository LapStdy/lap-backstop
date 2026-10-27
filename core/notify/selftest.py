"""连通性自检：内核投递上下文与一次性测试投递（docs/06 测试连通性）。

与常驻通知服务无关：不建线程、不落事件簿记，配置每次现读、改动立即生效。
API 端点与 NotifyService.send_test 共用这一实现；build_kernel_delivery_context
同时是常驻投递的上下文来源，保证「内核投递身份」只有一处定义。
"""

from __future__ import annotations

import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from contracts import Clock, ContractError, ErrorCode, Logger, Notification, Notifier, RunContext
from contracts.types import DeliveryResult
from core.logging import NullLogger
from core.notify.channels import build_channel
from core.notify.config import NotifyConfig, resolve_config
from core.notify.notifications import no_emit, reject_evidence, test_notification
from core.worker.executor import CancelFlag

_REDACTED_PLACEHOLDER = "[已脱敏]"


@dataclass(frozen=True, slots=True)
class TestDelivery:
    """一次测试投递的完整结果：生效渠道与渠道应答。"""

    channel: str
    result: DeliveryResult


def build_kernel_delivery_context(
    clock: Clock,
    *,
    data_root: Path,
    log: Logger | None = None,
) -> RunContext:
    """内核投递的最小上下文：无运行归属，不接受证据，只满足协议签名。"""
    return RunContext(
        run_id="notify:kernel",
        attempt=0,
        template_id="",
        plugin_id="core.notify",
        environment_id="",
        config={},
        work_dir=data_root / "work" / "notify-kernel",
        evidence_dir=data_root / "evidence" / "notify-kernel",
        deadline_at=clock.utc_now(),
        cancel=CancelFlag(),
        clock=clock,
        secrets={},
        log=NullLogger() if log is None else log,
        emit=no_emit,
        record_evidence=reject_evidence,
    )


def deliver_test_notification(
    settings: Mapping[str, Any],
    context: RunContext,
    *,
    channel_factory: Callable[[NotifyConfig], Notifier] | None = None,
    message: Notification | None = None,
) -> TestDelivery:
    """按给定 settings 做一次真实投递：单次尝试、不重试、不写事件。

    Args:
        settings: settings 表的键值快照（notify.* 覆盖 defaults.toml）。
        context: 投递上下文；渠道实现不消费，仅为满足 Notifier 签名。
        channel_factory: 渠道构造的注入点；默认按配置构造真实 HTTP 渠道。
        message: 要投递的通知；省略时用固定测试文案。

    Returns:
        生效渠道与渠道应答；投递失败体现在应答里（错误码 E_NOTIFY），不抛异常。

    Raises:
        ContractError: 通知未启用或配置非法，消息指出要修改的配置键。
    """
    config = resolve_config(settings)
    if not config.enabled:
        raise ContractError(ErrorCode.CONFIG_INVALID, "通知未启用：先打开 notify.enabled")
    build = channel_factory if channel_factory is not None else build_channel
    channel = build(config)
    result = channel.send(context, test_notification() if message is None else message)
    return TestDelivery(channel=config.channel, result=_redact_secrets(result, config))


def _redact_secrets(result: DeliveryResult, config: NotifyConfig) -> DeliveryResult:
    """渠道应答可能原样回显请求内容；凭据按 docs/08 口径遮掉后再交给调用方。

    URL（webhook、主题地址）脱敏为域名加前 6 位；密钥值整段遮蔽（docs/11 不回显）。
    """
    detail = result.detail
    for url in (config.webhook, config.url):
        if url and url in detail:
            detail = detail.replace(url, _redacted_url(url))
    if config.token and config.token in detail:
        detail = detail.replace(config.token, _REDACTED_PLACEHOLDER)
    if detail == result.detail:
        return result
    return replace(result, detail=detail)


def _redacted_url(url: str) -> str:
    """docs/08 脱敏口径：域名加前 6 位。"""
    parts = urllib.parse.urlsplit(url)
    origin = f"{parts.scheme}://{parts.netloc}"
    return f"{origin}{url[len(origin) :][:6]}"


__all__ = ["TestDelivery", "build_kernel_delivery_context", "deliver_test_notification"]
