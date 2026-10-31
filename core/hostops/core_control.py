r"""升级期对核心进程与 API 的控制：排水、就绪等待、拉起与退场。

排水经 ``POST /api/v1/host/drain`` 复用 serve 的停机路径（docs/08 升级与回滚）；
拉起只走计划任务 lap-backstop-core，与日常启动同一口径。等待一律按注入时钟的
单调值计时（docs/11 单调时钟用于时长判定），轮询间隔的睡眠经 wait 注入：生产
睡真实时间，测试推进虚拟时钟。
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from contracts import Clock
from core.api.app import API_PREFIX
from core.api.server import DEFAULT_HOST, DEFAULT_PORT
from core.api.store import all_settings, bearer_token
from core.clock import SystemClock
from core.hostops import tasks
from core.hostops.heartbeat import (
    HEARTBEAT_FILENAME,
    HeartbeatContent,
    heartbeat_age_seconds,
    read_heartbeat,
)
from core.hostops.releases import UpgradeLayout
from core.hostops.upgrade_state import UpgradeError
from core.hostops.watchdog import is_pid_alive
from storage import open_database
from storage.defaults import default

POLL_SECONDS = 2.0
_API_TIMEOUT_SECONDS = 5.0
_COMMAND_TIMEOUT_SECONDS = 30.0
CORE_TASK_NAME = "lap-backstop-core"
# 排水等待上限：在跑作业要推进到阶段边界；worker 默认截止 30 分钟，放宽到 1 小时。
DRAIN_TIMEOUT_SECONDS = 3600.0
# 健康检查口径（docs/08）：拉起后 60 秒内 /health 须就绪。
HEALTH_CHECK_SECONDS = 60.0

HttpCaller = Callable[[str, str, str], tuple[int, Mapping[str, object]]]
"""(method, url, token) -> (状态码, JSON 载荷)；连不上时状态码为 0、载荷为空对象。

0 在合法 HTTP 状态码值域（100-599）之外，不会与真实应答混淆。
"""


def http_json(method: str, url: str, token: str) -> tuple[int, Mapping[str, object]]:
    """带 Bearer 的一次 API 调用：显式超时、关闭系统代理（docs/11 外部调用）。

    连接失败、超时或非 JSON 响应都归为 ``(0, {})``；HTTP 错误状态连同错误
    载荷原样返回，由调用方判定。
    """
    request = urllib.request.Request(
        url, method=method, headers={"Authorization": f"Bearer {token}"}
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=_API_TIMEOUT_SECONDS) as response:
            return response.status, _decode_json(response.read())
    except urllib.error.HTTPError as error:
        return error.code, _decode_json(error.read())
    except (urllib.error.URLError, OSError, ValueError):
        return 0, {}


def _decode_json(raw: bytes) -> Mapping[str, object]:
    """响应体转 JSON 对象；空体、非 UTF-8 或非对象都按空对象处理。"""
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


@dataclass(frozen=True, slots=True)
class UpgradeInputs:
    """升级编排的注入项：外部动作全部可替换，生产路径用缺省值。"""

    clock: Clock = field(default_factory=SystemClock)
    http: HttpCaller = http_json
    runner: tasks.Runner = subprocess.run
    pid_alive: Callable[[int], bool] = is_pid_alive
    # 轮询间隔的等待动作；生产睡真实时间，测试推进虚拟时钟（单调值随之前进）。
    wait: Callable[[float], None] = time.sleep


@dataclass(frozen=True, slots=True)
class ApiEndpoint:
    """核心 API 的联系点：基础 URL 与访问令牌。"""

    base_url: str
    token: str


@dataclass(frozen=True, slots=True)
class StartOutcome:
    """一次按计划任务拉起核心的结果。"""

    ok: bool
    detail: str


def api_endpoint(database: Path) -> ApiEndpoint:
    """读核心 API 的联系点；配置或 token 不可用按升级失败处理。

    Raises:
        UpgradeError: 库不可读、token 未生成或监听配置类型非法。
    """
    connection = open_database(database)
    try:
        settings = all_settings(connection)
        token = bearer_token(connection)
    finally:
        connection.close()
    host = settings.get("api.host", DEFAULT_HOST)
    port = settings.get("api.port", DEFAULT_PORT)
    if not isinstance(host, str) or not host:
        raise UpgradeError("api.host 配置非法，无法联系核心 API")
    if not isinstance(port, int) or isinstance(port, bool):
        raise UpgradeError("api.port 配置非法，无法联系核心 API")
    if token is None:
        raise UpgradeError("访问令牌未生成：先启动一次 serve 完成初始化")
    return ApiEndpoint(base_url=f"http://{host}:{port}", token=token)


def _poll_until(
    predicate: Callable[[], bool], *, inputs: UpgradeInputs, timeout_seconds: float
) -> bool:
    """轮询谓词直到成立或超时；时限按单调时钟计，轮询间隔经 wait 流逝。

    Args:
        predicate: 每轮判定的谓词，先判后等。
        inputs: 提供时钟与等待动作。
        timeout_seconds: 自首次判定起的时限（单调秒）。

    Returns:
        bool: 时限内谓词成立返回 True，否则 False。
    """
    deadline = inputs.clock.monotonic() + timeout_seconds
    while not predicate():
        if inputs.clock.monotonic() >= deadline:
            return False
        inputs.wait(POLL_SECONDS)
    return True


def wait_core_exit(pid: int, *, inputs: UpgradeInputs, timeout_seconds: float) -> bool:
    """等待进程退出：时限内退出返回 True，超时返回 False（不强杀）。"""

    def exited() -> bool:
        return not inputs.pid_alive(pid)

    return _poll_until(exited, inputs=inputs, timeout_seconds=timeout_seconds)


def wait_ready(inputs: UpgradeInputs, *, endpoint: ApiEndpoint, timeout_seconds: float) -> bool:
    """就绪等待：时限内 /health 返回 200 且 status=ok 才算就绪（docs/08 口径）。"""

    def ready() -> bool:
        status, payload = inputs.http(
            "GET", f"{endpoint.base_url}{API_PREFIX}/health", endpoint.token
        )
        return status == 200 and payload.get("status") == "ok"

    return _poll_until(ready, inputs=inputs, timeout_seconds=timeout_seconds)


def start_core(inputs: UpgradeInputs) -> StartOutcome:
    """按计划任务拉起核心：启动参数的唯一口径是任务 lap-backstop-core（docs/08）。"""
    try:
        completed = inputs.runner(
            ["schtasks", "/run", "/tn", CORE_TASK_NAME],
            capture_output=True,
            check=False,
            timeout=_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return StartOutcome(ok=False, detail=f"按计划任务拉起核心失败：{error}")
    if completed.returncode != 0:
        return StartOutcome(ok=False, detail=f"schtasks 退出码 {completed.returncode}")
    return StartOutcome(ok=True, detail=f"已按计划任务 {CORE_TASK_NAME} 拉起核心")


def drain_core(core_pid: int, *, inputs: UpgradeInputs, endpoint: ApiEndpoint) -> None:
    """请求排水并等核心退场；未按期完成转成升级失败，不切版本。

    排水是异步停机：置位停机事件后轮询核心进程退场（docs/08 升级与回滚）。

    Raises:
        UpgradeError: 排水被拒绝，或核心未在时限内完成阶段边界收尾。
    """
    url = f"{endpoint.base_url}{API_PREFIX}/host/drain"
    status, payload = inputs.http("POST", url, endpoint.token)
    if status != 200:
        raise UpgradeError(f"排水请求被拒绝：HTTP {status}（{error_detail(payload)}）")
    if not wait_core_exit(core_pid, inputs=inputs, timeout_seconds=DRAIN_TIMEOUT_SECONDS):
        raise UpgradeError(
            f"核心在 {DRAIN_TIMEOUT_SECONDS:g} 秒内未完成在跑作业的阶段边界收尾，"
            "升级中止；核心继续在跑，未做任何切换"
        )


def terminate_pid(pid: int, *, inputs: UpgradeInputs) -> str:
    """结束未就绪的新版本进程；结果是人话 detail，失败不抛（回滚继续）。"""
    try:
        completed = inputs.runner(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            capture_output=True,
            check=False,
            timeout=_COMMAND_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"结束新版本进程（pid {pid}）失败：{error}"
    if completed.returncode != 0:
        return f"taskkill 退出码 {completed.returncode}（pid {pid} 可能已自行退出）"
    return f"已结束未就绪的新版本进程（pid {pid}）"


def launch_and_check(app: UpgradeLayout, *, inputs: UpgradeInputs) -> None:
    """按计划任务拉起核心并做健康检查；任一步不就绪都转成升级失败。

    Raises:
        UpgradeError: 计划任务启动失败，或健康检查时限内未就绪。
    """
    parts: list[str] = []
    if not restart_and_check(app, inputs=inputs, parts=parts):
        raise UpgradeError("；".join(parts))


def restart_and_check(app: UpgradeLayout, *, inputs: UpgradeInputs, parts: list[str]) -> bool:
    """拉起核心并做健康检查，结果并进 parts；返回新拉起的核心是否就绪。"""
    start = start_core(inputs)
    parts.append(start.detail)
    if not start.ok:
        return False
    try:
        endpoint = api_endpoint(app.database)
    except UpgradeError as error:
        parts.append(f"健康检查未执行：{error}")
        return False
    is_healthy = wait_ready(inputs, endpoint=endpoint, timeout_seconds=HEALTH_CHECK_SECONDS)
    parts.append("健康检查通过" if is_healthy else "健康检查未通过")
    return is_healthy


def running_core_pid(data_root: Path, *, inputs: UpgradeInputs) -> int | None:
    """在跑核心的 pid：心跳新鲜且 pid 存活才算在跑；否则 None。

    心跳陈旧度与看门狗同一口径（[host].stale_seconds，按墙钟对 mtime）。
    """
    content = _fresh_heartbeat(data_root, inputs=inputs)
    if content is None:
        return None
    pid = content["pid"]
    return pid if inputs.pid_alive(pid) else None


def unready_core_pid(data_root: Path, target_version: str, *, inputs: UpgradeInputs) -> int | None:
    """未就绪新版本的 pid：心跳版本是目标版本、心跳新鲜且 pid 存活；否则 None。

    心跳里的 version 由核心进程自带，据此把新版本进程与残留的旧版本心跳区分开；
    不新鲜的心跳不作数。pid 复用窗口内可能误认，方向是宁可多杀一次无在跑作业
    的残留进程，也不让未就绪版本占着端口。
    """
    content = _fresh_heartbeat(data_root, inputs=inputs)
    if content is None or content["version"] != target_version:
        return None
    pid = content["pid"]
    return pid if inputs.pid_alive(pid) else None


def _fresh_heartbeat(data_root: Path, *, inputs: UpgradeInputs) -> HeartbeatContent | None:
    """新鲜的心跳内容；缺失、损坏或超过陈旧阈值按 None 处理（与看门狗同口径）。"""
    path = data_root / "run" / HEARTBEAT_FILENAME
    content = read_heartbeat(path)
    if content is None:
        return None
    age = heartbeat_age_seconds(path, now=inputs.clock.utc_now().timestamp())
    if age is None or age > float(default("host", "stale_seconds")):
        return None
    return content


def error_detail(payload: Mapping[str, object]) -> str:
    """错误应答里的人话原因；取不到就按未知处理。"""
    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message:
            return message
    return "无错误详情"


__all__ = [
    "CORE_TASK_NAME",
    "DRAIN_TIMEOUT_SECONDS",
    "HEALTH_CHECK_SECONDS",
    "POLL_SECONDS",
    "ApiEndpoint",
    "HttpCaller",
    "StartOutcome",
    "UpgradeInputs",
    "api_endpoint",
    "drain_core",
    "error_detail",
    "http_json",
    "launch_and_check",
    "restart_and_check",
    "running_core_pid",
    "start_core",
    "terminate_pid",
    "unready_core_pid",
    "wait_core_exit",
    "wait_ready",
]
