"""交付物检查的规则表与豁免判定。

规则表定义交付物里查什么：历史渗入、施工占位、备忘与聊天腔，按 doc/ui/comment
三类文本生效。豁免判定定义哪里不查：目录与文件名通配、文件头与行内标记、
引号内的引用与 Markdown 代码围栏。扫描流程与命令行入口在 check_artifacts.py。
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass
from pathlib import Path

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

# 可扫描扩展名的并集：collect 用它决定哪些文件进扫描队列。
SCANNABLE_EXTS = DOC_EXT | CODE_EXT | UI_EXT | DATA_EXT

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
    ".agents",
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


# ---------------------------------------------------------------- 注释与围栏

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
