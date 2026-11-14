#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交付物通道检查器。

只查一件事：交付物里有没有「关于过去、关于过程、关于施工」的字。

交付物 = 文档正文、界面文案、代码注释。它们只回答「此刻为真」。
历史、备忘、聊天腔、占位字，属于别的通道，写进来就是噪音。

用法：
    python scripts/check_artifacts.py [路径 ...] [--strict] [--selftest] [--max N]

退出码：
    0  没有高风险命中
    1  有高风险命中（--strict 下任意命中即 1）
    2  参数或环境错误
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from artifact_rules import (
    EXEMPT_DIRS,
    EXEMPT_MARK_FILE,
    EXEMPT_MARK_LINE,
    FENCE,
    RULES,
    SCANNABLE_EXTS,
    Rule,
    classify,
    comment_part,
    is_exempt,
    is_fence,
    quoted,
)

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ---------------------------------------------------------------- 扫描


@dataclass
class Hit:
    rule: Rule
    path: str
    line: int
    text: str


@dataclass
class Report:
    files: int = 0
    hits: list[Hit] = field(default_factory=list)

    def count(self, level: str) -> int:
        return sum(1 for h in self.hits if h.rule.level == level)


def _load_scan_lines(path: Path) -> list[str] | None:
    """读入待扫行；超出大小上限、读不出来或文件头带豁免标记时返回 None。"""
    try:
        if path.stat().st_size > 2 * 1024 * 1024:
            return None
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    lines = text.splitlines()
    if EXEMPT_MARK_FILE in "\n".join(lines[:5]):
        return None
    return lines


def scan_file(path: Path, root: Path, hits: list[Hit]) -> int:
    """逐行扫一个文件，命中追加进 hits；返回 1 表示计入文件数，跳过返回 0。"""
    lines = _load_scan_lines(path)
    if lines is None:
        return 0
    is_doc, is_code, is_ui = classify(path)
    if not (is_doc or is_code or is_ui):
        return 0
    ext = path.suffix.lower()
    rel = display_path(path, root)

    in_fence = False
    for n, line in enumerate(lines, 1):
        if is_doc:
            if is_fence(line):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
        if EXEMPT_MARK_LINE in line:
            continue
        if not line.strip():
            continue

        cpart = comment_part(line, ext) if is_code else ""
        probe = re.sub(r"<[^<>\n]{0,60}>", " ", line) if is_doc else line

        for rule in RULES:
            if "doc" in rule.applies:
                if not is_doc:
                    continue
                target = probe
            elif "ui" in rule.applies:
                if not is_ui:
                    continue
                target = line
            elif "comment" in rule.applies:
                if not (is_code and cpart):
                    continue
                target = cpart
            else:
                continue
            m = rule.pattern.search(target)
            if m and not quoted(target, m):
                hits.append(Hit(rule, rel, n, line.strip()[:160]))
    return 1


def display_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def collect(paths: list[str], root: Path) -> list[Path]:
    files = []
    for raw in paths:
        p = Path(raw)
        if p.is_file():
            if not is_exempt(p, root):
                files.append(p.resolve())
        elif p.is_dir():
            for dirpath, dirnames, filenames in os.walk(p):
                dirnames[:] = [d for d in dirnames if d.lower() not in EXEMPT_DIRS]
                for name in filenames:
                    f = Path(dirpath) / name
                    if not f.is_file() or is_exempt(f, root):
                        continue
                    if f.suffix.lower() not in SCANNABLE_EXTS:
                        continue
                    files.append(f.resolve())
    return files


# ---------------------------------------------------------------- 输出

LEVEL_ORDER = {"high": 0, "mid": 1, "low": 2}
LEVEL_NAME = {"high": "高", "mid": "中", "low": "低"}


def render(report: Report, max_per_rule: int) -> None:
    hits = sorted(report.hits, key=lambda h: (LEVEL_ORDER[h.rule.level], h.path, h.line))
    hi, mid, lo = report.count("high"), report.count("mid"), report.count("low")
    total = len(hits)

    if total:
        if hi:
            print(
                f"交付物检查：FAIL（扫描 {report.files} 个文件，命中 {total} 处："
                f"高 {hi} / 中 {mid} / 低 {lo}）"
            )
        else:
            print(
                f"交付物检查：WARN（扫描 {report.files} 个文件，命中 {total} 处："
                f"高 0 / 中 {mid} / 低 {lo}；不阻断，但交付前应清零）"
            )
    else:
        print(f"交付物检查：PASS（扫描 {report.files} 个文件，未发现可疑文字）")
    if not total:
        return

    print()
    shown_rule = None
    per_rule = 0
    for h in hits:
        if h.rule.rid != shown_rule:
            shown_rule = h.rule.rid
            per_rule = 0
            print(f"【{LEVEL_NAME[h.rule.level]}】{h.rule.title}")
            print(f"  为什么：{h.rule.why}")
            print(f"  改法：{h.rule.fix}")
        per_rule += 1
        if per_rule > max_per_rule:
            continue
        print(f"  {h.path}:{h.line}")
        print(f"      {h.text}")
    print()
    print(
        "提示：CHANGELOG、交接账本、archive/、迁移说明 本来就记历史，已自动豁免；"
        "确属误报可在该行加 artifact-check: ignore。"
    )


# ---------------------------------------------------------------- 自检

SELFTEST_CASES = {
    "doc.md": (
        "# 结构\n\n原先的 6 目录结构已改为 5 个。\n\n" + FENCE + "text\nTODO 不算\n" + FENCE + "\n",
        {"history_high", "history_mid"},
    ),
    "ui.tsx": ("export const T = () => <div>待定</div>;\n", {"placeholder_ui"}),
    "code.py": (
        "def f():\n    # 改成这样，之前会崩\n    return 1\n",
        {"comment_changelog"},
    ),
    "CHANGELOG.md": ("已改为新结构。\n", set()),
    "clean.md": ("# 用法\n\n运行 python scripts/check_artifacts.py 即可。\n", set()),
}


def selftest() -> int:
    ok = True
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        for name, (content, _) in SELFTEST_CASES.items():
            (root / name).write_text(content, encoding="utf-8")
        files = collect([str(root)], root)
        hits = []
        for f in files:
            scan_file(f, root, hits)
        for name, (_, expect) in SELFTEST_CASES.items():
            got = {h.rule.rid for h in hits if h.path == name}
            flag = "PASS" if got == expect else "FAIL"
            if got != expect:
                ok = False
            print(f"自检 {flag}  {name}  期望={sorted(expect) or '无'}  实际={sorted(got) or '无'}")
    print("自检结果：" + ("全部通过" if ok else "有失败"))
    return 0 if ok else 1


# ---------------------------------------------------------------- 入口


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="交付物通道检查：查文档/界面文案/注释里的历史、备忘、占位、聊天腔。"
    )
    ap.add_argument("paths", nargs="*", default=["."], help="要扫描的目录或文件，默认当前目录")
    ap.add_argument("--strict", action="store_true", help="任意命中即返回 1（中风险也要修）")
    ap.add_argument("--max", type=int, default=8, help="每条规则最多展开几处，默认 8")
    ap.add_argument("--selftest", action="store_true", help="跑内置样例自检")
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()

    targets = args.paths or ["."]
    missing = [t for t in targets if not Path(t).exists()]
    if missing:
        print("路径不存在：" + "、".join(missing), file=sys.stderr)
        return 2

    root = Path.cwd().resolve()
    files = collect(targets, root)
    report = Report(files=0)
    for f in files:
        report.files += scan_file(f, root, report.hits)

    render(report, args.max)
    if args.strict:
        return 1 if report.hits else 0
    return 1 if report.count("high") else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
