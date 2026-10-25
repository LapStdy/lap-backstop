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
import fnmatch
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

FENCE = chr(96) * 3

# ---------------------------------------------------------------- 规则


@dataclass(frozen=True)
class Rule:
    rid: str
    level: str
    applies: tuple[str, ...]
    title: str
    why: str
    fix: str
    pattern: re.Pattern[str]


RULES = [
    Rule(
        "history_high",
        "high",
        ("doc",),
        "历史渗入：写进了旧版本或变更说明",
        "读者不需要知道以前是什么。这句话只对「刚和你聊过天的人」有意义，而交付物的读者是陌生人。",
        "删掉这句；旧版本归提交说明、CHANGELOG 或 archive/ 管。",
        re.compile(
            r"(已(改为|更新为|调整为|修正为|改成|删掉|移除)|上一版|旧版(?:是|为|中|里|的)|"
            r"(变更|更新|修订|修改)(记录|说明|日志|历史))"
        ),
    ),
    Rule(
        "history_mid",
        "mid",
        ("doc",),
        "历史渗入：拿「原来」作参照",
        "用「原来」来铺垫，等于把读者拉进你上一次的上下文。",
        "改写成现状陈述；确实需要对比时，放迁移说明或 CHANGELOG。",
        re.compile(
            r"((原先|原来|原本)(是|的|版本|方案|设计|实现|只|有)|"
            r"之前版本|与之前不同|不同于之前|以前(是|的)|"
            r"本次(修改|更新|变更|调整|改动)|这次(的)?改动)"
        ),
    ),
    Rule(
        "placeholder_ui",
        "high",
        ("ui",),
        "界面文案里有施工字",
        "界面读者是没参与施工的陌生人。「待定/此处/占位」是给施工队看的告示。",
        "换成用户看得懂的话；没内容就不放这一行。",
        re.compile(
            r"(待定|待补|待确认|待核实|此处(待|显示|放|填|插入)|占位|"
            r"TODO|TBD|FIXME|暂未实现|后续(再)?补充|临时(方案|处理|占位)|先这样)"
        ),
    ),
    Rule(
        "placeholder_doc",
        "mid",
        ("doc",),
        "文档里有占位/待办字",
        "「待定/待补」是工作状态，不是内容。它会让读者以为这份文档还没写完。",
        "写完再发；确实要留空就删掉整段，把待办放进任务清单。",
        re.compile(
            r"(待定|待补充?|待确认|待核实|此处(待|显示|放|填|插入)|占位|"
            r"TODO|TBD|FIXME|暂未实现|后续(再)?补充|临时(方案|处理|占位)|先这样)"
        ),
    ),
    Rule(
        "placeholder_comment",
        "low",
        ("comment",),
        "注释里有占位字",
        "TODO/FIXME 属于任务清单，不属于代码；它会永远留在那里。",
        "能在本轮做完就做完；做不完记进任务清单并删掉这一行。",
        re.compile(r"(TODO|FIXME|TBD|待定|待补|占位|XXX)"),
    ),
    Rule(
        "memo_doc",
        "mid",
        ("doc",),
        "文档里有备忘口吻",
        "「记一下/回头再改」是写给自己的字条，收件人不是读者。",
        "删掉；要留就去任务清单或交接账本。",
        re.compile(
            r"(备忘|记一下|提醒自己|回头(再)?(改|补|看)|以后再说|"
            r"先放着|暂时先这样|此处备注)"
        ),
    ),
    Rule(
        "chat_doc",
        "mid",
        ("doc",),
        "文档里有聊天腔",
        "「刚才/接下来我/你说过」默认读者和你共享上下文，而读者没有。",
        "改成第三人称的现状陈述。",
        re.compile(
            r"(刚才我|你刚才|你说过|接下来我|我把它|我这边|"
            r"更正一下|上面说错|你说的)"
        ),
    ),
    Rule(
        "comment_changelog",
        "mid",
        ("comment",),
        "注释在讲改动",
        "注释该说「为什么这么写」；「改成/原来是/修正」是在讲过程，代码读起来会像日记。",
        "删掉过程，只留原因；改动由版本历史回答。",
        re.compile(
            r"((之前|原来|原先|以前)(是|的|会|只)|改为|改成|调整为|"
            r"删掉|移除|修正|修复\s*#\d+)"
        ),
    ),
]

LEVEL_ORDER = {"high": 0, "mid": 1, "low": 2}
LEVEL_NAME = {"high": "高", "mid": "中", "low": "低"}

# ---------------------------------------------------------------- 文件分类

DOC_EXT = {".md", ".mdx", ".txt", ".rst", ".adoc"}
CODE_EXT = {
    ".py",
    ".js",
    ".mjs",
    ".cjs",
    ".ts",
    ".tsx",
    ".jsx",
    ".vue",
    ".java",
    ".go",
    ".rs",
    ".cs",
    ".cpp",
    ".cc",
    ".c",
    ".h",
    ".hpp",
    ".rb",
    ".php",
    ".swift",
    ".kt",
    ".kts",
    ".lua",
    ".sh",
    ".ps1",
    ".bat",
    ".cmd",
}
UI_EXT = {
    ".html",
    ".htm",
    ".vue",
    ".tsx",
    ".jsx",
    ".xaml",
    ".axaml",
    ".resx",
    ".properties",
    ".strings",
    ".po",
    ".pot",
}
DATA_EXT = {".json", ".yaml", ".yml"}

LOCALE_HINTS = (
    "locale",
    "locales",
    "i18n",
    "lang",
    "langs",
    "translation",
    "translations",
    "messages",
    "strings",
)

EXEMPT_DIRS = {
    ".git",
    "node_modules",
    "dist",
    "build",
    "out",
    "target",
    ".venv",
    "venv",
    "__pycache__",
    "archive",
    "handoff",
    ".dsh-memory",
    ".tmp",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".idea",
    ".vscode",
    "coverage",
    ".next",
    "expected",
    "golden",
    "snapshots",
}

EXEMPT_GLOBS = (
    "CHANGELOG*",
    "CHANGES*",
    "HISTORY*",
    "NEWS*",
    "RELEASE*",
    "HANDOFF*",
    "CONTINUE*",
    "*迁移*",
    "*migration*",
    "LICENSE*",
    "*.lock",
    "*.min.js",
    "*.map",
    "*.log",
)

EXEMPT_MARK_FILE = "artifact-check: off"
EXEMPT_MARK_LINE = "artifact-check: ignore"


def classify(path: Path) -> tuple:
    """返回 (is_doc, is_code, is_ui)。"""
    ext = path.suffix.lower()
    is_doc = ext in DOC_EXT
    is_code = ext in CODE_EXT
    is_ui = ext in UI_EXT
    if ext in DATA_EXT:
        is_ui = any(p.lower() in LOCALE_HINTS for p in path.parts[:-1])
    return is_doc, is_code, is_ui


def is_exempt(path: Path, root: Path) -> bool:
    try:
        rel = path.resolve().relative_to(root.resolve())
        parts = {p.lower() for p in rel.parts}
    except ValueError:
        parts = {p.lower() for p in path.parts}
    if parts & EXEMPT_DIRS:
        return True
    return any(fnmatch.fnmatch(path.name, g) for g in EXEMPT_GLOBS)


# ---------------------------------------------------------------- 行内处理

HASH_LANGS = {".py", ".rb", ".sh", ".ps1", ".yml", ".yaml", ".toml", ".cfg", ".ini"}
SLASH_LANGS = {
    ".js",
    ".mjs",
    ".cjs",
    ".ts",
    ".tsx",
    ".jsx",
    ".vue",
    ".java",
    ".go",
    ".rs",
    ".cs",
    ".cpp",
    ".cc",
    ".c",
    ".h",
    ".hpp",
    ".php",
    ".swift",
    ".kt",
    ".kts",
    ".scss",
    ".css",
}
DASH_LANGS = {".lua", ".sql"}
HTML_LANGS = {".html", ".htm", ".xaml", ".axaml"}


def _py_comment(line: str) -> str:
    """Python 的行内注释：跳过字符串里的 #，避免把字符串当注释。"""
    i, n = 0, len(line)
    quote = ""
    triple = False
    while i < n:
        ch = line[i]
        if quote:
            if ch == "\\":
                i += 2
                continue
            if triple and line.startswith(quote * 3, i):
                quote, triple = "", False
                i += 3
                continue
            if not triple:
                if ch == quote:
                    quote = ""
                i += 1
                continue
            i += 1
            continue
        if ch in ("'", '"'):
            if line.startswith(ch * 3, i):
                quote, triple = ch, True
                i += 3
                continue
            quote, triple = ch, False
            i += 1
            continue
        if ch == "#":
            return line[i:]
        i += 1
    return ""


PAIRS = (("「", "」"), ("“", "”"), ("『", "』"), (chr(96), chr(96)))


def quoted(text: str, m: re.Match[str]) -> bool:
    """命中词在引号里，通常是在讨论这个词，不是在用它。"""
    head = text[: m.start()]
    for open_q, close_q in PAIRS:
        if open_q == close_q:
            if head.count(open_q) % 2 == 1:
                return True
        elif head.count(open_q) > head.count(close_q):
            return True
    return False


def comment_part(line: str, ext: str) -> str:
    """粗略切出注释部分；切不准时宁可漏报，不误报。"""
    s = line
    if ext == ".py":
        return _py_comment(s)
    if ext in HASH_LANGS:
        i = s.find("#")
        return s[i:] if i >= 0 else ""
    if ext in SLASH_LANGS:
        cand = [i for i in (s.find("//"), s.find("/*")) if i >= 0]
        return s[min(cand) :] if cand else ""
    if ext in HTML_LANGS:
        i = s.find("<!--")
        return s[i:] if i >= 0 else ""
    if ext in DASH_LANGS:
        i = s.find("--")
        return s[i:] if i >= 0 else ""
    if ext in {".bat", ".cmd"}:
        low = s.lower()
        for m in ("rem ", "::"):
            i = low.find(m)
            if i >= 0:
                return s[i:]
        return ""
    return ""


def is_fence(line: str) -> bool:
    t = line.strip()
    return t.startswith(FENCE)


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


def scan_file(path: Path, root: Path, hits: list[Hit]) -> int:
    try:
        if path.stat().st_size > 2 * 1024 * 1024:
            return 0
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return 0
    lines = text.splitlines()
    if EXEMPT_MARK_FILE in "\n".join(lines[:5]):
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
                    if f.suffix.lower() not in (DOC_EXT | CODE_EXT | UI_EXT | DATA_EXT):
                        continue
                    files.append(f.resolve())
    return files


# ---------------------------------------------------------------- 输出


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
