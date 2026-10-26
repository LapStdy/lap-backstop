"""通知渠道与投递重试：三渠道客户端、配置解析、事件尾随与投递编排。

docs/05 Notifier 能力的内核侧实现；渠道由 settings 的 notify.* 选择，缺省关闭。
"""

from __future__ import annotations

from core.notify.channels import (
    DingTalkChannel,
    NtfyChannel,
    ServerChanChannel,
    build_channel,
)
from core.notify.config import NotifyConfig, resolve_config
from core.notify.delivery import (
    ChannelSnapshot,
    DeliveryOutcome,
    DeliveryWorker,
    PendingDelivery,
    deliver_one,
)
from core.notify.service import NotifyService

__all__ = [
    "ChannelSnapshot",
    "DeliveryOutcome",
    "DeliveryWorker",
    "DingTalkChannel",
    "NotifyConfig",
    "NotifyService",
    "NtfyChannel",
    "PendingDelivery",
    "ServerChanChannel",
    "build_channel",
    "deliver_one",
    "resolve_config",
]
