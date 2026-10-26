"""外部通知渠道：ServerChan、钉钉机器人与 ntfy 的 HTTP 客户端。

实现 docs/05 的 Notifier 能力：send 只做一次投递并返回 DeliveryResult，
渠道失败不抛异常（契约测试判据）；重试、退避与事件落库由外层负责。
HTTP 只访问构造时给定的地址，显式超时、默认关闭系统代理、校验 TLS（docs/11 外部调用）。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Final

from contracts import (
    ContractError,
    ErrorCode,
    Notification,
    NotificationLevel,
    Notifier,
    RunContext,
)
from contracts.types import DeliveryResult
from core.notify.config import NotifyConfig

SERVERCHAN_ENDPOINT: Final[str] = "https://sctapi.ftqq.com"
DEFAULT_TIMEOUT_SECONDS: Final[float] = 10.0
_RESPONSE_CAP_BYTES: Final[int] = 4096
_DETAIL_CAP_CHARS: Final[int] = 200

# docs/11 外部调用：默认关闭系统代理，除非配置打开。
_NO_PROXY_OPENER: Final[urllib.request.OpenerDirector] = urllib.request.build_opener(
    urllib.request.ProxyHandler({})
)

# ntfy 的优先级与标签按通知级别取值；数字优先级 3/4/5 对应默认/高/紧急。
_NTFY_PRIORITY: Final[dict[NotificationLevel, int]] = {
    NotificationLevel.INFO: 3,
    NotificationLevel.WARN: 4,
    NotificationLevel.ERROR: 5,
}
_NTFY_TAGS: Final[dict[NotificationLevel, str]] = {
    NotificationLevel.INFO: "information_source",
    NotificationLevel.WARN: "warning",
    NotificationLevel.ERROR: "rotating_light",
}


class ChannelSendError(Exception):
    """渠道调用失败的内部载体：失败结果已成形，渠道方法捕获后原样返回。"""

    def __init__(self, result: DeliveryResult) -> None:
        super().__init__(result.detail)
        self.result = result


def _failure(detail: str, **meta: object) -> DeliveryResult:
    """渠道失败的统一出口：错误码固定 E_NOTIFY，meta 供排障。"""
    return DeliveryResult(
        ok=False, error_code=ErrorCode.NOTIFY.value, detail=detail[:_DETAIL_CAP_CHARS], meta=meta
    )


def _success() -> DeliveryResult:
    return DeliveryResult(ok=True, detail="已投递")


def _clip(text: str) -> str:
    return text[:_DETAIL_CAP_CHARS]


def _post(
    request: urllib.request.Request,
    *,
    timeout_seconds: float,
    use_proxy: bool,
) -> tuple[int, bytes]:
    """POST 一次已构造好的请求，返回状态码与截断后的响应体；网络故障抛 ChannelSendError。

    Raises:
        ChannelSendError: 4xx/5xx、超时或连接失败，detail 带人可读原因。
    """
    try:
        if use_proxy:
            opener: urllib.request.OpenerDirector | None = None
        else:
            opener = _NO_PROXY_OPENER
        open_url = urllib.request.urlopen if opener is None else opener.open
        with open_url(request, timeout=timeout_seconds) as response:
            return int(response.status), response.read(_RESPONSE_CAP_BYTES)
    except urllib.error.HTTPError as error:
        detail = _clip(error.read(_RESPONSE_CAP_BYTES).decode("utf-8", errors="replace").strip())
        raise ChannelSendError(
            _failure(f"HTTP {error.code}：{detail}", http_status=error.code)
        ) from None
    except TimeoutError:
        raise ChannelSendError(
            _failure(f"请求超时（上限 {timeout_seconds:g} 秒）", reason="timeout")
        ) from None
    except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
        raise ChannelSendError(_failure(f"连接失败：{error}", reason="network")) from None


def _json_field(payload: bytes, key: str, context: str) -> object:
    """解析渠道应答并取业务码字段；应答不是 JSON 对象视为失败。"""
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ChannelSendError(_failure(f"{context}：应答不是 JSON（{payload[:80]!r}）")) from None
    if not isinstance(parsed, dict):
        raise ChannelSendError(_failure(f"{context}：应答不是 JSON 对象"))
    return parsed.get(key)


@dataclass(frozen=True, slots=True)
class ServerChanChannel:
    """Server酱 Turbo：表单 POST 到 sctapi，应答 JSON code=0 视为成功。"""

    token: str
    endpoint: str = SERVERCHAN_ENDPOINT
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    use_proxy: bool = False

    def send(self, ctx: RunContext, message: Notification) -> DeliveryResult:
        """投递一条通知；ctx 不参与投递，仅为满足 Notifier 契约。"""
        url = f"{self.endpoint.rstrip('/')}/{self.token}.send"
        request = urllib.request.Request(
            url,
            data=urllib.parse.urlencode({"title": message.title, "desp": message.body}).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        try:
            status, payload = _post(
                request, timeout_seconds=self.timeout_seconds, use_proxy=self.use_proxy
            )
            code = _json_field(payload, "code", f"ServerChan HTTP {status}")
        except ChannelSendError as error:
            return error.result
        if code == 0:
            return _success()
        return _failure(f"ServerChan HTTP {status}：业务码 {code!r}", http_status=status)


def _signed_dingtalk_url(webhook: str, secret: str) -> str:
    """钉钉加签：timestamp 与 HMAC-SHA256 签名拼到 webhook 上。"""
    timestamp = str(int(time.time() * 1000))
    digest = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}\n{secret}".encode(),
        hashlib.sha256,
    ).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(digest))
    separator = "&" if "?" in webhook else "?"
    return f"{webhook}{separator}timestamp={timestamp}&sign={sign}"


@dataclass(frozen=True, slots=True)
class DingTalkChannel:
    """钉钉自定义机器人：JSON POST 到 webhook，应答 errcode=0 视为成功。"""

    webhook: str
    secret: str = ""
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    use_proxy: bool = False

    def send(self, ctx: RunContext, message: Notification) -> DeliveryResult:
        """投递一条通知；ctx 不参与投递，仅为满足 Notifier 契约。"""
        url = _signed_dingtalk_url(self.webhook, self.secret) if self.secret else self.webhook
        request = urllib.request.Request(
            url,
            data=json.dumps(
                {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": message.title,
                        "text": f"### {message.title}\n\n{message.body}",
                    },
                },
                ensure_ascii=False,
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            status, response = _post(
                request, timeout_seconds=self.timeout_seconds, use_proxy=self.use_proxy
            )
            errcode = _json_field(response, "errcode", f"钉钉 HTTP {status}")
        except ChannelSendError as error:
            return error.result
        if errcode == 0:
            return _success()
        return _failure(f"钉钉 HTTP {status}：errcode {errcode!r}", http_status=status)


@dataclass(frozen=True, slots=True)
class NtfyChannel:
    """ntfy：JSON 发布到主题；标题与正文都走 UTF-8 JSON，避开 header 编码限制。"""

    topic_url: str
    token: str = ""
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    use_proxy: bool = False

    def send(self, ctx: RunContext, message: Notification) -> DeliveryResult:
        """投递一条通知；ctx 不参与投递，仅为满足 Notifier 契约。"""
        parts = urllib.parse.urlsplit(self.topic_url)
        topic = parts.path.strip("/")
        if not topic or "/" in topic:
            return _failure("ntfy 地址必须指向单个主题（如 https://ntfy.sh/主题）")
        payload = json.dumps(
            {
                "topic": topic,
                "title": message.title,
                "message": message.body,
                "priority": _NTFY_PRIORITY[message.level],
                "tags": [_NTFY_TAGS[message.level]],
            },
            ensure_ascii=False,
        ).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        base = urllib.parse.urlunsplit((parts.scheme, parts.netloc, "", "", ""))
        try:
            status, _ = _post(
                urllib.request.Request(base, data=payload, headers=headers, method="POST"),
                timeout_seconds=self.timeout_seconds,
                use_proxy=self.use_proxy,
            )
        except ChannelSendError as error:
            return error.result
        if 200 <= status < 300:
            return _success()
        return _failure(f"ntfy HTTP {status}", http_status=status)


def build_channel(config: NotifyConfig) -> Notifier:
    """按配置构造渠道实例；渠道取值不支持视为配置错误。

    Raises:
        ContractError: channel 不在支持列表内。
    """
    if config.channel == "serverchan":
        return ServerChanChannel(
            token=config.token,
            endpoint=config.url or SERVERCHAN_ENDPOINT,
            timeout_seconds=config.timeout_seconds,
            use_proxy=config.use_proxy,
        )
    if config.channel == "dingtalk":
        return DingTalkChannel(
            webhook=config.webhook,
            secret=config.token,
            timeout_seconds=config.timeout_seconds,
            use_proxy=config.use_proxy,
        )
    if config.channel == "ntfy":
        return NtfyChannel(
            topic_url=config.url,
            token=config.token,
            timeout_seconds=config.timeout_seconds,
            use_proxy=config.use_proxy,
        )
    raise ContractError(ErrorCode.CONFIG_INVALID, f"notify.channel 取值不支持：{config.channel}")


__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "SERVERCHAN_ENDPOINT",
    "DingTalkChannel",
    "NtfyChannel",
    "ServerChanChannel",
    "build_channel",
]
