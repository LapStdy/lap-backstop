"""MAA 配置读写工具（测试/运维用）。

为什么存在：测试 MAA 交接时，需要频繁切换配置、增删改任务、改任务参数，
手工点 GUI 太慢，而且容易点错。

MAA 的 GUI 配置在 <MAA目录>/config/gui.new.json，格式是 System.Text.Json（缩进 2）。
结构来自源码 src/MaaWpfGui/Configuration/：
  Root            : Configurations{名字: 配置}, Current(当前配置名), Gui(全局), Timers, ConfigVersion
  SpecificConfig  : TaskQueue[任务], Gui(该配置的界面设置), Toolbox, Copilot, TaskSelectedIndex
  TaskQueue[i]    : BaseTask 多态，用 "$type" 区分：StartUpTask / FightTask / AwardTask /
                    InfrastTask / RecruitTask / MallTask / RoguelikeTask / ReclamationTask /
                    CustomTask / DepotMaintainTask / UserDataUpdateTask / SwitchThemeTask /
                    OperProgressTask / CloseDownTask / CopilotTask ...
  Gui.PostActions : [Flags] 枚举，逗号字符串：None / ExitArknights / BackToAndroidHome /
                    ExitEmulator / ExitSelf / IfNoOtherMaa / Hibernate / Shutdown / Sleep /
                    SkipOnError / LockScreen

写配置的前提：MAA 必须没有在跑（它退出时会把内存里的配置写回文件，会覆盖我们的改动）。

用法（--maa-dir 指向 MAA 目录，例如 D:\\MAA-Test）：
  python scripts/maa_config.py --maa-dir D:\\MAA-Test profiles
  python scripts/maa_config.py --maa-dir D:\\MAA-Test use 项目测试
  python scripts/maa_config.py --maa-dir D:\\MAA-Test new 项目测试        # 复制当前配置成新配置
  python scripts/maa_config.py --maa-dir D:\\MAA-Test tasks
  python scripts/maa_config.py --maa-dir D:\\MAA-Test add FightTask --name 理智作战
  python scripts/maa_config.py --maa-dir D:\\MAA-Test rm 3
  python scripts/maa_config.py --maa-dir D:\\MAA-Test set 3 IsEnable false
  python scripts/maa_config.py --maa-dir D:\\MAA-Test set 3 StagePlan '["1-7"]'
  python scripts/maa_config.py --maa-dir D:\\MAA-Test move 3 1
  python scripts/maa_config.py --maa-dir D:\\MAA-Test post-actions None
  python scripts/maa_config.py --maa-dir D:\\MAA-Test run-directly false
  python scripts/maa_config.py --maa-dir D:\\MAA-Test title-prefix 测试·
  python scripts/maa_config.py --maa-dir D:\\MAA-Test show 3
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

CONFIG_REL = Path("config") / "gui.new.json"

POST_ACTIONS = {
    "None",
    "ExitArknights",
    "BackToAndroidHome",
    "ExitEmulator",
    "ExitSelf",
    "IfNoOtherMaa",
    "Hibernate",
    "Shutdown",
    "Sleep",
    "SkipOnError",
    "LockScreen",
}

# 新增任务时的最小骨架；$type 决定反序列化成哪个类，其余字段用 MAA 自己的默认值。
# 这些字段是列表，写错类型会让 MAA 整个配置解析失败。
LIST_FIELDS = {"StagePlan", "RoomList", "StageList"}

TASK_TYPES = {
    "StartUpTask",
    "CloseDownTask",
    "FightTask",
    "OperProgressTask",
    "AwardTask",
    "MallTask",
    "InfrastTask",
    "RecruitTask",
    "RoguelikeTask",
    "CopilotTask",
    "SSSCopilotTask",
    "SingleStepTask",
    "DepotTask",
    "OperBoxTask",
    "UserDataUpdateTask",
    "ReclamationTask",
    "DepotMaintainTask",
    "SwitchThemeTask",
    "CustomTask",
}


def fail(msg: str) -> None:
    print("错误：" + msg, file=sys.stderr)
    raise SystemExit(1)


def resolve_config(maa_dir: str) -> Path:
    root = Path(maa_dir).expanduser().resolve()
    if not (root / "MAA.exe").exists():
        fail(f"{root} 下没有 MAA.exe，--maa-dir 要指向 MAA 目录")
    return root / CONFIG_REL


def maa_running(maa_dir: Path) -> bool:
    """MAA.exe 是否在跑；同名进程从别的目录起的也算，保守拒绝。

    查询不可用或超时同样按「在跑」处置：写配置的前提是 MAA 必须没在跑，
    查不出来时放行等于绕过这个前提。
    """
    try:
        completed = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq MAA.exe", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return True
    return "MAA.exe" in completed.stdout


def load(cfg_path: Path) -> dict[str, Any]:
    with cfg_path.open(encoding="utf-8") as f:
        return json.load(f)


def save(cfg_path: Path, data: dict[str, Any], *, backup: bool = True) -> None:
    if backup and cfg_path.exists():
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        cfg_path.with_name(cfg_path.name + f".{stamp}.bak").write_bytes(cfg_path.read_bytes())
    tmp = cfg_path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, cfg_path)


def current_profile(data: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    name = data.get("Current") or "Default"
    confs = data.get("Configurations") or {}
    if name not in confs:
        fail(f"Current={name!r} 不在 Configurations 里（现有：{', '.join(confs)}）")
    return name, confs[name]


def task_queue(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    q = cfg.get("TaskQueue")
    if not isinstance(q, list):
        fail("该配置没有 TaskQueue 列表")
    return q


def parse_value(text: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def default_task(data: dict[str, Any], type_name: str, name: str | None) -> dict[str, Any]:
    """优先复制现有同类任务（保留 MAA 的字段与默认值），没有就造最小骨架。"""
    for conf in (data.get("Configurations") or {}).values():
        for t in conf.get("TaskQueue") or []:
            if t.get("$type") == type_name:
                tpl = copy.deepcopy(t)
                tpl["Name"] = name or ""
                tpl["IsEnable"] = True
                return tpl
    return {"$type": type_name, "Name": name or "", "IsEnable": True}


def cmd_profiles(data: dict[str, Any], _args: argparse.Namespace) -> None:
    cur = data.get("Current")
    for name, conf in (data.get("Configurations") or {}).items():
        mark = "*" if name == cur else " "
        n = len(conf.get("TaskQueue") or [])
        print(f"{mark} {name}  （{n} 个任务）")


def cmd_use(data: dict[str, Any], args: argparse.Namespace) -> None:
    if args.name not in (data.get("Configurations") or {}):
        fail(f"没有配置 {args.name!r}")
    data["Current"] = args.name
    print(f"当前配置 -> {args.name}")


def cmd_new(data: dict[str, Any], args: argparse.Namespace) -> None:
    confs = data.setdefault("Configurations", {})
    if args.name in confs:
        fail(f"配置 {args.name!r} 已存在")
    src = confs.get(data.get("Current") or "Default")
    if src is None:
        fail("没有可复制的当前配置")
    confs[args.name] = copy.deepcopy(src)
    print(f"已创建配置 {args.name}（复制自 {data.get('Current')}）")


def cmd_tasks(data: dict[str, Any], _args: argparse.Namespace) -> None:
    name, conf = current_profile(data)
    print(f"配置：{name}")
    for i, t in enumerate(conf.get("TaskQueue") or []):
        state = "开" if t.get("IsEnable", True) else "关"
        print(f"{i:>2} [{state}] {t.get('$type', '?'):<18} {t.get('Name') or ''}")


def cmd_add(data: dict[str, Any], args: argparse.Namespace) -> None:
    if args.type not in TASK_TYPES:
        fail(f"未知任务类型 {args.type!r}，可用：{', '.join(sorted(TASK_TYPES))}")
    _, conf = current_profile(data)
    q = task_queue(conf)
    task = default_task(data, args.type, args.name)
    if args.index is None:
        q.append(task)
        i = len(q) - 1
    else:
        i = max(0, min(args.index, len(q)))
        q.insert(i, task)
    print(f"已添加 {args.type} 到位置 {i}（名字：{task.get('Name') or '（空）'}）")


def cmd_rm(data: dict[str, Any], args: argparse.Namespace) -> None:
    _, conf = current_profile(data)
    q = task_queue(conf)
    if not 0 <= args.index < len(q):
        fail(f"索引越界：{args.index}（共 {len(q)} 个）")
    t = q.pop(args.index)
    print(f"已删除 {args.index}: {t.get('$type')} {t.get('Name') or ''}")


def cmd_move(data: dict[str, Any], args: argparse.Namespace) -> None:
    _, conf = current_profile(data)
    q = task_queue(conf)
    if not (0 <= args.index < len(q) and 0 <= args.to < len(q)):
        fail("索引越界")
    q.insert(args.to, q.pop(args.index))
    print(f"已移动 {args.index} -> {args.to}")


def cmd_set(data: dict[str, Any], args: argparse.Namespace) -> None:
    _, conf = current_profile(data)
    q = task_queue(conf)
    if not 0 <= args.index < len(q):
        fail(f"索引越界：{args.index}")
    value = parse_value(args.value)
    if args.field in LIST_FIELDS and not isinstance(value, list):
        fail(f"{args.field} 必须是 JSON 数组，例如 '[\"1-7\"]'")
    q[args.index][args.field] = value
    print(f"已设置任务 {args.index} 的 {args.field} = {json.dumps(value, ensure_ascii=False)}")


def cmd_show(data: dict[str, Any], args: argparse.Namespace) -> None:
    _, conf = current_profile(data)
    q = task_queue(conf)
    if not 0 <= args.index < len(q):
        fail(f"索引越界：{args.index}")
    print(json.dumps(q[args.index], ensure_ascii=False, indent=2))


def cmd_post_actions(data: dict[str, Any], args: argparse.Namespace) -> None:
    names = [x.strip() for x in args.value.split(",") if x.strip()]
    bad = [x for x in names if x not in POST_ACTIONS]
    if bad:
        fail(f"未知动作 {bad}，可用：{', '.join(sorted(POST_ACTIONS))}")
    _, conf = current_profile(data)
    conf.setdefault("Gui", {})["PostActions"] = ", ".join(names)
    print("PostActions = " + (", ".join(names) or "None"))


def cmd_run_directly(data: dict[str, Any], args: argparse.Namespace) -> None:
    _, conf = current_profile(data)
    v = parse_value(args.value)
    conf.setdefault("Gui", {}).setdefault("StartUpSettings", {})["RunDirectly"] = bool(v)
    print(f"RunDirectly = {bool(v)}")


def cmd_title_prefix(data: dict[str, Any], args: argparse.Namespace) -> None:
    _, conf = current_profile(data)
    conf.setdefault("Gui", {})["WindowTitlePrefix"] = args.value
    print(f"WindowTitlePrefix = {args.value!r}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="读写 MAA 的 gui.new.json（MAA 需先关闭）")
    ap.add_argument("--maa-dir", required=True, help="MAA 目录，例如 D:\\MAA-Test")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("profiles").set_defaults(func=cmd_profiles)
    p = sub.add_parser("use")
    p.add_argument("name")
    p.set_defaults(func=cmd_use)
    p = sub.add_parser("new")
    p.add_argument("name")
    p.set_defaults(func=cmd_new)
    sub.add_parser("tasks").set_defaults(func=cmd_tasks)
    p = sub.add_parser("add")
    p.add_argument("type")
    p.add_argument("--name", default=None)
    p.add_argument("--index", type=int, default=None)
    p.set_defaults(func=cmd_add)
    p = sub.add_parser("rm")
    p.add_argument("index", type=int)
    p.set_defaults(func=cmd_rm)
    p = sub.add_parser("move")
    p.add_argument("index", type=int)
    p.add_argument("to", type=int)
    p.set_defaults(func=cmd_move)
    p = sub.add_parser("set")
    p.add_argument("index", type=int)
    p.add_argument("field")
    p.add_argument("value")
    p.set_defaults(func=cmd_set)
    p = sub.add_parser("show")
    p.add_argument("index", type=int)
    p.set_defaults(func=cmd_show)
    p = sub.add_parser("post-actions")
    p.add_argument("value")
    p.set_defaults(func=cmd_post_actions)
    p = sub.add_parser("run-directly")
    p.add_argument("value")
    p.set_defaults(func=cmd_run_directly)
    p = sub.add_parser("title-prefix")
    p.add_argument("value")
    p.set_defaults(func=cmd_title_prefix)

    args = ap.parse_args(argv)
    cfg_path = resolve_config(args.maa_dir)
    maa_dir = Path(args.maa_dir).expanduser().resolve()
    if maa_running(maa_dir) and args.cmd not in {"profiles", "tasks", "show"}:
        fail("MAA 正在运行：它退出时会用内存里的配置覆盖文件，先关掉再改")

    data = load(cfg_path)
    args.func(data, args)
    if args.cmd not in {"profiles", "tasks", "show"}:
        save(cfg_path, data)
        print(f"已写入 {cfg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
