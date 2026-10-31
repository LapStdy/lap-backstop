r"""计划任务定义：XML 生成、落盘与 schtasks 注册。

四个任务按 docs/08 计划任务一节：触发器与动作固定，实例策略「不启动新实例」、
不限运行时长、不关 AC 电源条件都写进 XML 字段。任务以 XML 导入，文件留在
``data\host\tasks\`` 供比对与重建；备份每日 04:00、证据镜像每周日 04:30 注册；
预热没有计划任务，由调度在 ``preheat_minutes`` 之前建运行完成。
"""

from __future__ import annotations

import locale
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape

TASKS_DIRNAME = "host/tasks"
TASK_CORE = "lap-backstop-core"
TASK_WATCHDOG = "lap-backstop-watchdog"
TASK_BACKUP = "lap-backstop-backup"
TASK_EVIDENCE_MIRROR = "lap-backstop-evidence-mirror"
# 注册结果的落点事件（docs/03 host.event 取值表）；serve 启动与本端点共用。
TASKS_REGISTERED_EVENT = "host.tasks_registered"

# 注册清单：动作只依赖本命令行（docs/08 计划任务一节）。
REGISTERED_TASKS = (TASK_CORE, TASK_WATCHDOG, TASK_BACKUP, TASK_EVIDENCE_MIRROR)

# schtasks 偶发慢盘会到秒级；注册发生在 serve 启动路径上，超时按失败处理可重试。
_REGISTER_TIMEOUT_SECONDS = 10.0

Runner = Callable[..., subprocess.CompletedProcess[bytes]]

_TASK_TEMPLATE = """<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>{description}</Description>
    <URI>\\{name}</URI>
  </RegistrationInfo>
  <Triggers>
{triggers}  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>false</StartWhenAvailable>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>{wake_to_run}</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
{restart_on_failure}  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{command}</Command>
      <Arguments>{arguments}</Arguments>
    </Exec>
  </Actions>
</Task>
"""

_RESTART_ON_FAILURE = """    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>3</Count>
    </RestartOnFailure>
"""


@dataclass(frozen=True, slots=True)
class TaskAction:
    """任务动作：可执行体与参数；命令行由这一处拼，不散落任务定义里。"""

    command: str
    arguments: str


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """一个任务的模板输入；生成器只产出它，渲染只消费它。"""

    name: str
    description: str
    triggers: str
    action: TaskAction
    restart_on_failure: str = ""
    wake_to_run: bool = False


def core_task_xml(action: TaskAction) -> str:
    """登录后延迟 30 秒拉起核心；失败每分钟重试、最多 3 次。"""
    return _render(
        TaskSpec(
            name=TASK_CORE,
            description="lap-backstop 核心进程：登录后延迟 30 秒启动",
            triggers="    <LogonTrigger>\n      <Delay>PT30S</Delay>\n    </LogonTrigger>\n",
            action=action,
            restart_on_failure=_RESTART_ON_FAILURE,
        )
    )


def watchdog_task_xml(action: TaskAction, *, registered_at: datetime) -> str:
    """登录时与之后每 5 分钟各查一次核心心跳。"""
    triggers = (
        "    <LogonTrigger />\n"
        "    <TimeTrigger>\n"
        f"      <StartBoundary>{registered_at:%Y-%m-%dT%H:%M:%S}</StartBoundary>\n"
        "      <Repetition>\n"
        "        <Interval>PT5M</Interval>\n"
        "        <StopAtDurationEnd>false</StopAtDurationEnd>\n"
        "      </Repetition>\n"
        "    </TimeTrigger>\n"
    )
    return _render(
        TaskSpec(
            name=TASK_WATCHDOG,
            description="lap-backstop 看门狗：登录时与每 5 分钟检查核心心跳",
            triggers=triggers,
            action=action,
        )
    )


def backup_task_xml(action: TaskAction, *, registered_at: datetime) -> str:
    """每日 04:00 备份；允许唤醒计算机。"""
    triggers = (
        "    <CalendarTrigger>\n"
        f"      <StartBoundary>{registered_at:%Y-%m-%d}T04:00:00</StartBoundary>\n"
        "      <ScheduleByDay>\n"
        "        <DaysInterval>1</DaysInterval>\n"
        "      </ScheduleByDay>\n"
        "    </CalendarTrigger>\n"
    )
    return _render(
        TaskSpec(
            name=TASK_BACKUP,
            description="lap-backstop 备份：每日 04:00 执行",
            triggers=triggers,
            action=action,
            wake_to_run=True,
        )
    )


def evidence_mirror_task_xml(action: TaskAction, *, registered_at: datetime) -> str:
    """每周日 04:30 证据增量镜像；允许唤醒计算机。"""
    triggers = (
        "    <CalendarTrigger>\n"
        f"      <StartBoundary>{registered_at:%Y-%m-%d}T04:30:00</StartBoundary>\n"
        "      <ScheduleByWeek>\n"
        "        <WeeksInterval>1</WeeksInterval>\n"
        "        <DaysOfWeek>\n"
        "          <Sunday />\n"
        "        </DaysOfWeek>\n"
        "      </ScheduleByWeek>\n"
        "    </CalendarTrigger>\n"
    )
    return _render(
        TaskSpec(
            name=TASK_EVIDENCE_MIRROR,
            description="lap-backstop 证据增量镜像：每周日 04:30 执行",
            triggers=triggers,
            action=action,
            wake_to_run=True,
        )
    )


def write_definitions(
    tasks_dir: Path,
    actions: Mapping[str, TaskAction],
    *,
    registered_at: datetime,
) -> dict[str, Path]:
    r"""把任务定义写成 ``tasks_dir\*.xml``，返回任务名到文件路径的映射。

    Raises:
        OSError: 目录不可写等。
        KeyError: actions 里出现没有生成器的任务名。
    """
    generators: Mapping[str, Callable[[TaskAction, datetime], str]] = {
        TASK_CORE: lambda action, _registered_at: core_task_xml(action),
        TASK_WATCHDOG: lambda action, at: watchdog_task_xml(action, registered_at=at),
        TASK_BACKUP: lambda action, at: backup_task_xml(action, registered_at=at),
        TASK_EVIDENCE_MIRROR: lambda action, at: evidence_mirror_task_xml(action, registered_at=at),
    }
    tasks_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for name, action in actions.items():
        generator = generators.get(name)
        if generator is None:
            raise KeyError(f"没有任务 {name} 的 XML 生成器")
        path = tasks_dir / f"{name}.xml"
        path.write_text(generator(action, registered_at), encoding="utf-16")
        written[name] = path
    return written


@dataclass(frozen=True, slots=True)
class TaskRegisterOutcome:
    """一个任务的一次注册结果；detail 是人可读的成功确认或失败原因。"""

    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class TaskRegistration:
    """一次批量注册的结果：逐任务明细与汇总口径（serve 启动与手动注册共用）。"""

    outcomes: tuple[TaskRegisterOutcome, ...]

    @property
    def ok(self) -> bool:
        """全部任务注册成功才算成功。"""
        return all(outcome.ok for outcome in self.outcomes)

    @property
    def detail(self) -> str:
        """一句话汇总，作为 host.tasks_registered 事件的 detail。"""
        failed = [outcome.name for outcome in self.outcomes if not outcome.ok]
        if failed:
            return "注册失败：" + "、".join(failed)
        return "已注册并写入定义：" + "、".join(outcome.name for outcome in self.outcomes)


def register_task(
    name: str, xml_path: Path, *, runner: Runner = subprocess.run
) -> TaskRegisterOutcome:
    """用 schtasks 覆盖式注册一个任务，返回结果与失败原因。

    覆盖式（/f）即修复：定义文件是唯一真源，注册结果以它为准。超时按失败
    处理，下次 serve 启动会再修复。
    """
    try:
        completed = runner(
            ["schtasks", "/create", "/f", "/tn", name, "/xml", str(xml_path)],
            capture_output=True,
            check=False,
            timeout=_REGISTER_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return TaskRegisterOutcome(name, False, "schtasks 超时")
    if completed.returncode == 0:
        return TaskRegisterOutcome(name, True, "已注册")
    return TaskRegisterOutcome(name, False, _failure_reason(completed))


def _failure_reason(completed: subprocess.CompletedProcess[bytes]) -> str:
    """失败原因：stderr 优先，空输出落到退出码。

    schtasks 的输出编码跟系统区域走（中文 Windows 是 GBK），先按 UTF-8
    试读，失败再用系统区域编码，仍不行才降级为替换字符。
    """
    stderr = completed.stderr.strip()
    if not stderr:
        return f"schtasks 退出码 {completed.returncode}"
    for encoding in ("utf-8", locale.getencoding()):
        try:
            return stderr.decode(encoding)
        except UnicodeDecodeError:
            continue
    return stderr.decode("utf-8", errors="replace")


def startup_actions(data_root: Path) -> dict[str, TaskAction]:
    r"""按运行形态取注册动作：打包件指向自身 exe，源码直跑指向当前解释器。

    打包件就位前（app\current 布局），源码形态用于开发联调；两种形态的任务
    动作只有这一处定义（docs/08 独立看门狗一节：启动参数与环境只有一处定义）。
    """
    if getattr(sys, "frozen", False):
        executable = sys.executable
        return {
            TASK_CORE: TaskAction(executable, "serve"),
            TASK_WATCHDOG: TaskAction(executable, "watchdog check"),
            TASK_BACKUP: TaskAction(executable, "backup"),
            TASK_EVIDENCE_MIRROR: TaskAction(executable, "evidence-mirror"),
        }
    python = sys.executable
    root = str(data_root.resolve())
    return {
        TASK_CORE: TaskAction(python, f"-m core serve --data-root {root}"),
        TASK_WATCHDOG: TaskAction(python, f"-m core watchdog check --data-root {root}"),
        TASK_BACKUP: TaskAction(python, f"-m core backup --data-root {root}"),
        TASK_EVIDENCE_MIRROR: TaskAction(python, f"-m core evidence-mirror --data-root {root}"),
    }


def register_registered_tasks(
    data_root: Path,
    *,
    runner: Runner = subprocess.run,
    registered_at: datetime,
) -> TaskRegistration:
    """生成注册清单的任务定义并逐个注册，返回明细与汇总。

    供 serve 启动、``lap-backstop tasks register`` 手动入口与设置页重新注册
    端点共用；汇总口径只有这一份，写成 ``host.tasks_registered`` 事件的 detail。
    """
    tasks_dir = data_root / TASKS_DIRNAME
    actions = startup_actions(data_root)
    written = write_definitions(
        tasks_dir,
        {name: actions[name] for name in REGISTERED_TASKS},
        registered_at=registered_at,
    )
    return TaskRegistration(
        tuple(register_task(name, path, runner=runner) for name, path in written.items())
    )


def _render(spec: TaskSpec) -> str:
    """填模板；插入值经 XML 转义，保证同输入产出同字节。"""
    return _TASK_TEMPLATE.format(
        description=escape(spec.description),
        name=escape(spec.name),
        triggers=spec.triggers,
        restart_on_failure=spec.restart_on_failure,
        wake_to_run=str(spec.wake_to_run).lower(),
        command=escape(spec.action.command),
        arguments=escape(spec.action.arguments),
    )


__all__ = [
    "REGISTERED_TASKS",
    "TASKS_DIRNAME",
    "TASKS_REGISTERED_EVENT",
    "TASK_BACKUP",
    "TASK_CORE",
    "TASK_EVIDENCE_MIRROR",
    "TASK_WATCHDOG",
    "TaskAction",
    "TaskRegisterOutcome",
    "TaskRegistration",
    "TaskSpec",
    "backup_task_xml",
    "core_task_xml",
    "evidence_mirror_task_xml",
    "register_registered_tasks",
    "register_task",
    "startup_actions",
    "watchdog_task_xml",
    "write_definitions",
]
