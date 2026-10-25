#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""契约引用检查器：拦 docs 对契约的重复定义。

真源分工：事件名与错误码的唯一真源是 docs/03 的「SSE 事件」「错误码」两节；
能力签名、RunContext 与契约数据类型的唯一真源是 docs/05 与 contracts/ 代码。
docs/02 与 docs/09 只引用；docs/03 是事件与错误码注册表的宿主，这两类豁免。

「重复定义」按形态判定，宁漏报不误报：
- 能力签名：出现 `class X(Protocol)`、`class <契约类型名>` 或 `def <能力方法>(`。
  02/03/09 不定义接口，正文里任何 Protocol 类都按复制处理；散文引用不带 class/def 前缀。
- 事件名/错误码：表格单元格或列表项与注册名完全相等，单元格里的顿号串拆开比对。
  句子中间点名单个名目是引用，放行。
- 契约字段：表格首列或列表项冒号前，连续 3 行是契约类型的字段名；或表格首列
  直接是契约类型名（复制 05 类型表的形态）。单行撞名难免，连续三行才算枚举。

两类不查：docs/03 的数据表列名与契约类型字段天然同名（run_id、status），03 的
字段检查整体豁免；manifest 字段与枚举取值的字段名（id、name、version）无法与
常规表格区分，代码侧由 contracts/manifest.schema.json 与 manifest.py 守门。

用法：
    python scripts/check_contracts.py [文档 ...]    不传参数检查 docs/02、03、09

退出码：
    0  无违规
    1  有违规
    2  参数或环境错误（文档缺失、契约注册表解析失败）
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError, ValueError):
    # 无控制台或重定向到不支持重配的流时，退回默认编码，不阻断检查。
    pass

ROOT = Path(__file__).resolve().parent.parent
CONTRACTS_DIR = ROOT / "contracts"
DEFAULT_DOCS = ("docs/02-架构.md", "docs/03-数据模型与接口.md", "docs/09-调度与作业语义.md")

# 单行撞上字段名（detail、status）可能是巧合；连续三行只能是枚举。
FIELD_STREAK = 3
# 违规摘录只取行首一段，控制报告宽度；整行内容以文档为准。
SNIPPET_WIDTH = 120


class ContractScanError(RuntimeError):
    """契约注册表读不出来，检查无法进行。"""


# ---------------------------------------------------------------- 注册表


@dataclass
class Registry:
    events: frozenset[str]
    error_codes: frozenset[str]
    capabilities: frozenset[str]
    methods: frozenset[str]
    type_names: frozenset[str]
    fields: dict[str, frozenset[str]]
    owners: dict[str, set[str]] = field(init=False)
    field_names: frozenset[str] = field(init=False)

    def __post_init__(self) -> None:
        """派生字段归属索引，供扫描器免于逐次重建。"""
        self.owners = _field_owners(self.fields)
        self.field_names = frozenset(self.owners)


def _parse(path: Path) -> ast.Module:
    try:
        return ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as exc:
        raise ContractScanError(f"契约文件解析失败：{path.name}（{exc}）") from exc


def _enum_values(cls: ast.ClassDef) -> list[str]:
    values = []
    for node in cls.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            values.append(node.value.value)
    return values


def _is_subclass(cls: ast.ClassDef, base: str) -> bool:
    return any(isinstance(b, ast.Name) and b.id == base for b in cls.bases)


def _is_dataclass(cls: ast.ClassDef) -> bool:
    for dec in cls.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == "dataclass":
            return True
        if (
            isinstance(dec, ast.Call)
            and isinstance(dec.func, ast.Name)
            and dec.func.id == "dataclass"
        ):
            return True
    return False


def _method_names(cls: ast.ClassDef) -> list[str]:
    return [
        node.name
        for node in cls.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and not node.name.startswith("_")
    ]


def _field_names(cls: ast.ClassDef) -> list[str]:
    return [
        node.target.id
        for node in cls.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    ]


def _classes(tree: ast.Module) -> list[ast.ClassDef]:
    return [node for node in tree.body if isinstance(node, ast.ClassDef)]


def load_registry(contracts_dir: Path) -> Registry:
    """从 contracts/ 代码提取四类契约名目；文档检查以代码侧取值为准。

    契约文件缺失、语法错误或注册表为空时抛 ContractScanError。
    """
    events = []
    error_codes = []
    for path, sink in (
        (contracts_dir / "events.py", events),
        (contracts_dir / "errors.py", error_codes),
    ):
        for cls in _classes(_parse(path)):
            if _is_subclass(cls, "StrEnum"):
                sink.extend(_enum_values(cls))
    if not events or not error_codes:
        raise ContractScanError("contracts/events.py 或 errors.py 里找不到 StrEnum 注册表")

    capabilities = []
    methods = []
    for cls in _classes(_parse(contracts_dir / "abilities.py")):
        if _is_subclass(cls, "Protocol"):
            capabilities.append(cls.name)
            methods.extend(_method_names(cls))
    if not capabilities:
        raise ContractScanError("contracts/abilities.py 里找不到能力协议")

    fields: dict[str, frozenset[str]] = {}
    runtime_protocols: list[str] = []
    for path in (contracts_dir / "types.py", contracts_dir / "context.py"):
        for cls in _classes(_parse(path)):
            if _is_dataclass(cls):
                fields[cls.name] = frozenset(_field_names(cls))
            elif _is_subclass(cls, "Protocol"):
                runtime_protocols.append(cls.name)
    if "RunContext" not in fields:
        raise ContractScanError("contracts/context.py 里找不到 RunContext")

    return Registry(
        events=frozenset(events),
        error_codes=frozenset(error_codes),
        capabilities=frozenset(capabilities),
        methods=frozenset(methods),
        type_names=frozenset(fields) | frozenset(capabilities) | frozenset(runtime_protocols),
        fields=fields,
    )


# ---------------------------------------------------------------- 规则


@dataclass(frozen=True)
class Rule:
    rid: str
    title: str
    why: str
    fix: str


RULES: tuple[Rule, ...] = (
    Rule(
        "signature",
        "能力签名被复制",
        "能力接口与运行时协议只活在 docs/05 与 contracts/ 代码里；别处出现 class/def 定义，契约一改两处就漂移。",
        "删掉定义；叙述行为只写能力名与方法名，接口细节写「见 05 的能力接口」。",
    ),
    Rule(
        "event",
        "事件清单被复制",
        "事件注册表只在 docs/03 的「SSE 事件」维护；02/09 各留一份清单，事件增删时必有文档说错。",
        "删掉表格或列表，改为「事件类型与载荷见 03 的事件注册表」；行为叙述里点名单个事件可保留。",
    ),
    Rule(
        "error",
        "错误码清单被复制",
        "错误码表只在 docs/03 的「错误码」维护；别处复述清单，码一增删就漂移。",
        "删掉表格或列表，改为「错误码见 03 的错误码表」；句子中间引用单个码可保留。",
    ),
    Rule(
        "field",
        "契约类型字段被复制",
        "RunContext 与契约数据类型的字段只在 docs/05 与 contracts/types.py 维护；复述字段表会让结构变更漏改。",
        "删掉字段表，改为「结构见 05 的类型表」。",
    ),
)


@dataclass
class Hit:
    rule: str
    path: str
    line: int
    text: str


# ---------------------------------------------------------------- 判形

_PROTOCOL_CLASS = re.compile(r"class\s+\w+\s*\([^)]*Protocol")
_CLASS_DEF = re.compile(r"^\s*class\s+(\w+)")
_METHOD_DEF = re.compile(r"^\s*(?:async\s+)?def\s+(\w+)\s*\(")
_LIST_ITEM = re.compile(r"^\s*(?:[-*+]|\d+[.、])\s+(.+)$")
_LABEL = re.compile(r"^(.+?)[：:]")
_SEPARATOR_CELL = re.compile(r":?-{3,}:?")
_ENUM_SPLIT = re.compile(r"[、，,；;]")


@dataclass
class ScanCtx:
    """单份文档的只读扫描上下文：文档身份、注册表与宿主豁免。"""

    rel: str
    reg: Registry
    host: bool


def strip_marks(text: str) -> str:
    """去掉名目外围的反引号与加粗记号。"""
    return text.strip().strip("`*").strip()


def split_table_cells(line: str) -> list[str] | None:
    """表格数据行拆单元格；分隔行与普通文本返回 None。"""
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|") and len(stripped) > 1):
        return None
    cells = [c.strip() for c in stripped[1:-1].split("|")]
    non_empty = [c for c in cells if c]
    if non_empty and all(_SEPARATOR_CELL.fullmatch(c) for c in non_empty):
        return None
    return cells


def split_enum_tokens(text: str) -> list[str]:
    """按顿号、逗号与分号拆出待比对的名目。"""
    return [strip_marks(part) for part in _ENUM_SPLIT.split(text) if part.strip()]


def _field_owners(fields: dict[str, frozenset[str]]) -> dict[str, set[str]]:
    owners: dict[str, set[str]] = {}
    for type_name, names in fields.items():
        for name in names:
            owners.setdefault(name, set()).add(type_name)
    return owners


def signature_hit(line: str, reg: Registry) -> str | None:
    """契约签名定义行：class 契约类型或 Protocol、def 能力方法；否则 None。"""
    class_def = _CLASS_DEF.match(line)
    if class_def and (class_def.group(1) in reg.type_names or _PROTOCOL_CLASS.search(line)):
        return f"签名定义 {line.strip()[:SNIPPET_WIDTH]}"
    method_def = _METHOD_DEF.match(line)
    if method_def and method_def.group(1) in reg.methods:
        return f"签名定义 {line.strip()[:SNIPPET_WIDTH]}"
    return None


def name_hits(ctx: ScanCtx, names: Iterable[str], line_no: int, form: str) -> list[Hit]:
    """名目与注册表完全相等才算枚举命中；form 写「表格」或「列表」。"""
    hits = []
    for name in names:
        if name in ctx.reg.events:
            hits.append(Hit("event", ctx.rel, line_no, f"{form}枚举事件名 {name}"))
        elif name in ctx.reg.error_codes:
            hits.append(Hit("error", ctx.rel, line_no, f"{form}枚举错误码 {name}"))
    return hits


def table_step(ctx: ScanCtx, cells: list[str], line_no: int) -> tuple[list[Hit], str | None]:
    """表格行判定；返回命中与首列命中的契约字段名（供连击计数）。"""
    hits: list[Hit] = []
    feed: str | None = None
    first = strip_marks(cells[0])
    if ctx.host:
        return hits, feed
    for cell in cells:
        hits.extend(name_hits(ctx, split_enum_tokens(cell), line_no, "表格"))
    if first in ctx.reg.type_names:
        hits.append(Hit("field", ctx.rel, line_no, f"表格枚举契约类型 {first}"))
    if first in ctx.reg.field_names:
        feed = first
    return hits, feed


def list_step(ctx: ScanCtx, content: str, line_no: int) -> tuple[list[Hit], str | None]:
    """列表项判定；返回命中与冒号前标签命中的契约字段名。"""
    hits = name_hits(ctx, split_enum_tokens(content), line_no, "列表")
    feed: str | None = None
    label = _LABEL.match(content)
    if label is not None:
        name = strip_marks(label.group(1))
        hits.extend(name_hits(ctx, (name,), line_no, "列表"))
        if name in ctx.reg.field_names:
            feed = name
    return hits, feed


class FieldStreak:
    """契约字段连击计数：单行撞名放行，连续 FIELD_STREAK 行才算枚举。"""

    def __init__(self, rel: str) -> None:
        self._rel = rel
        self.reset()

    def count(self, name: str, kinds: set[str], line_no: int) -> Hit | None:
        """记一行字段命中；达到阈值时返回这批首行的命中，之后不再重复。"""
        self._len += 1
        if self._len == 1:
            self._start = line_no
            self._first = name
            self._kinds = set(kinds)
        else:
            self._kinds |= kinds
        if self._len >= FIELD_STREAK and not self._reported:
            self._reported = True
            owners = "、".join(sorted(self._kinds))
            return Hit(
                "field",
                self._rel,
                self._start,
                f"连续 {FIELD_STREAK} 行以上枚举 {owners} 的字段（{self._first} 起）",
            )
        return None

    def reset(self) -> None:
        """连击归零：非命中行、围栏行都调用。"""
        self._len = 0
        self._start = 0
        self._first = ""
        self._kinds = set()
        self._reported = False


def check_text(text: str, rel: str, reg: Registry, host: bool) -> list[Hit]:
    """扫描一份文档；host 为真时只查能力签名（注册表宿主豁免清单类）。"""
    ctx = ScanCtx(rel, reg, host)
    streak = FieldStreak(rel)
    hits: list[Hit] = []
    in_fence = False

    for line_no, line in enumerate(text.splitlines(), 1):
        if line.strip().startswith(("```", "~~~")):
            in_fence = not in_fence
            streak.reset()
            continue
        signature = signature_hit(line, reg)
        if signature is not None:
            hits.append(Hit("signature", rel, line_no, signature))
            continue
        if in_fence:
            continue

        cells = split_table_cells(line)
        if cells is not None:
            row_hits, matched_field = table_step(ctx, cells, line_no)
            hits.extend(row_hits)
        else:
            item = _LIST_ITEM.match(line)
            if item is None or ctx.host:
                streak.reset()
                continue
            row_hits, matched_field = list_step(ctx, item.group(1), line_no)
            hits.extend(row_hits)
            if matched_field is None:
                streak.reset()
                continue
        if matched_field is not None:
            hit = streak.count(matched_field, ctx.reg.owners[matched_field], line_no)
            if hit is not None:
                hits.append(hit)
        else:
            streak.reset()

    return hits


# ---------------------------------------------------------------- 输出


def render(hits: list[Hit], scanned: int, reg: Registry, max_per_rule: int) -> None:
    """按规则分组打印违规清单；无违规打印 PASS 与注册表规模。"""
    total = len(hits)
    if total:
        print(f"契约检查：FAIL（扫描 {scanned} 个文档，违规 {total} 处）")
    else:
        print(
            f"契约检查：PASS（扫描 {scanned} 个文档，违规 0；注册表 "
            f"事件 {len(reg.events)}、错误码 {len(reg.error_codes)}、"
            f"能力方法 {len(reg.methods)}、契约类型 {len(reg.type_names)}）"
        )
        return

    print()
    for rule in RULES:
        rule_hits = [h for h in hits if h.rule == rule.rid]
        if not rule_hits:
            continue
        print(f"【{rule.title}】")
        print(f"  为什么：{rule.why}")
        print(f"  改法：{rule.fix}")
        for hit in rule_hits[:max_per_rule]:
            print(f"  {hit.path}:{hit.line}")
            print(f"      {hit.text}")
        hidden = len(rule_hits) - max_per_rule
        if hidden > 0:
            print(f"  ……另有 {hidden} 处未展开")
    print()
    print(
        "提示：事件与错误码清单只在 docs/03 维护；能力签名与契约类型字段只在 docs/05 与 "
        "contracts/ 维护。句子中间点名单个名目属于引用，可保留。"
    )


# ---------------------------------------------------------------- 入口


def is_host_doc(path: Path) -> bool:
    """docs/03 是事件与错误码注册表的宿主；按文件名前缀识别。"""
    return path.stem.startswith("03-")


def display_path(path: Path) -> str:
    """优先输出相对仓库根的正斜杠路径，便于命中行跨平台点击。"""
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def main(argv: list[str]) -> int:
    """解析参数、加载注册表并检查文档；返回进程退出码。"""
    ap = argparse.ArgumentParser(
        description="契约引用检查：docs/02、03、09 只引用契约，"
        "不重复定义事件名、错误码、字段与能力签名。"
    )
    ap.add_argument("docs", nargs="*", help="要检查的文档；不传则检查仓库内 docs/02、03、09")
    ap.add_argument("--max", type=int, default=8, help="每条规则最多展开几处，默认 8")
    args = ap.parse_args(argv)

    try:
        reg = load_registry(CONTRACTS_DIR)
    except ContractScanError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    targets = [Path(d) for d in args.docs] or [ROOT / name for name in DEFAULT_DOCS]
    missing = [str(t) for t in targets if not t.is_file()]
    if missing:
        print("文档不存在：" + "、".join(missing), file=sys.stderr)
        return 2

    hits: list[Hit] = []
    for target in targets:
        text = target.read_text(encoding="utf-8", errors="replace")
        hits.extend(check_text(text, display_path(target), reg, host=is_host_doc(target)))
    render(hits, len(targets), reg, args.max)
    return 1 if hits else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
