"""docs/11「规模上限」的机械检查：扫 core/、storage/、contracts/ 全部 .py。

口径：函数行数按 ast 节点首尾行计（含 def/class 行与 docstring），嵌套函数
另立条目、限定名 ``Class.method`` 形式；参数个数含仅位置、仅关键字与
``*args``/``**kwargs``，方法去掉首个 ``self``/``cls``；模块行数是全文物理行。
模块级超限的名字记 ``<module>``。

允许清单逐项写「文件、名字、类别、记账时实测值、为什么现在不改、欠账编号」，
匹配只按文件加名字：类别与实测值允许随并行卡漂移，清单项变合规不要求删
（后续卡收缩），但不许通配符、目录级或整文件豁免，清单外超限即失败。
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCAN_ROOTS = ("core", "storage", "contracts")

FUNCTION_MAX_LINES = 50
FUNCTION_MAX_PARAMS = 4
CLASS_MAX_LINES = 300
MODULE_MAX_LINES = 500

# 模块级超限（模块行数）没有函数或类名，统一用这个名字入报告与清单。
MODULE_NAME = "<module>"


@dataclass(frozen=True, slots=True)
class _KindRule:
    """一类超限的报告要素：中文类别、实测值单位与改法。"""

    label: str
    unit: str
    remedy: str


_KINDS: dict[str, _KindRule] = {
    "function_lines": _KindRule("函数行数", "行", "拆函数"),
    "function_params": _KindRule("参数个数", "个", "收敛参数（dataclass 或拆函数）"),
    "class_lines": _KindRule("类行数", "行", "拆类"),
    "module_lines": _KindRule("模块行数", "行", "拆模块"),
}


@dataclass(frozen=True, slots=True)
class Violation:
    """一处超限：相对路径、行号、限定名、类别、实测值与上限。"""

    path: str
    line: int
    name: str
    kind: str
    actual: int
    limit: int

    @property
    def message(self) -> str:
        """「文件:行:名字:实测值/上限」加改法，门禁失败时逐项列出。"""
        rule = _KINDS[self.kind]
        return (
            f"{self.path}:{self.line}:{self.name}:"
            f"{self.actual}/{self.limit}{rule.unit}（{rule.label}），改法：{rule.remedy}"
        )


@dataclass(frozen=True, slots=True)
class AllowEntry:
    """允许清单条目：谁、哪类超限、记账时实测多少、为什么留、欠账编号。"""

    path: str
    name: str
    kind: str
    measured: str
    why: str
    debt: str


# D-013：执行器与 serve 的既有行数与参数超限，随执行器拆分卡偿还。
_D013_ENTRIES: tuple[AllowEntry, ...] = (
    AllowEntry(
        "core/worker/executor.py",
        MODULE_NAME,
        "module_lines",
        "748 行",
        "执行器是改动热点，模块拆分随执行器重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/worker/executor.py",
        "WorkerExecutor",
        "class_lines",
        "633 行",
        "类拆分随执行器重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/worker/executor.py",
        "WorkerExecutor.__init__",
        "function_params",
        "8 个",
        "注入点收敛随执行器重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/worker/executor.py",
        "WorkerExecutor.execute",
        "function_lines",
        "61 行",
        "阶段编排拆分随执行器重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/worker/executor.py",
        "WorkerExecutor._run_script",
        "function_lines",
        "62 行",
        "脚本执行拆分随执行器重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/hostops/serve.py",
        MODULE_NAME,
        "module_lines",
        "656 行",
        "serve 装配拆分随宿主重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/hostops/serve.py",
        "_run_core",
        "function_lines",
        "88 行",
        "主循环拆分随宿主重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/hostops/serve.py",
        "ServeLoop.__init__",
        "function_params",
        "7 个",
        "注入点收敛随宿主重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/hostops/serve.py",
        "_start_api",
        "function_params",
        "5 个",
        "注入点收敛随宿主重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/hostops/serve.py",
        "_bind_api",
        "function_params",
        "6 个",
        "注入点收敛随宿主重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/supervisor.py",
        "Supervisor.__init__",
        "function_lines",
        "53 行",
        "组合根装配拆分随执行器重构卡偿还",
        "D-013",
    ),
    AllowEntry(
        "core/supervisor.py",
        "Supervisor.__init__",
        "function_params",
        "7 个",
        "组合根注入点收敛随执行器重构卡偿还",
        "D-013",
    ),
)

# 其余既有超限不在 D-013 范围，打包记 D-018 由后续重构卡偿还。
_D018_ENTRIES: tuple[AllowEntry, ...] = (
    AllowEntry(
        "core/api/app.py",
        "ApiApp.__init__",
        "function_params",
        "7 个",
        "API 装配注入点收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/events/evidence.py",
        "EvidenceService.record",
        "function_params",
        "5 个",
        "证据登记参数收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/hostops/environment_actions.py",
        "EnvironmentActionService.__init__",
        "function_params",
        "5 个",
        "环境动作注入点收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/hostops/retention.py",
        "RetentionRunner.__init__",
        "function_params",
        "5 个",
        "保留清理注入点收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/hostops/upgrade_state.py",
        "record_failed",
        "function_params",
        "5 个",
        "升级事件参数收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/notify/service.py",
        "NotifyService.__init__",
        "function_params",
        "7 个",
        "通知装配注入点收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/plugins/worker.py",
        "_Reader.__init__",
        "function_params",
        "5 个",
        "worker 通道读取注入点收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/supervisor_environments.py",
        "EnvironmentSupervision.__init__",
        "function_params",
        "6 个",
        "环境监督注入点收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "core/supervisor_leases.py",
        "LeaseSupervision.__init__",
        "function_params",
        "5 个",
        "租约监督注入点收敛属独立重构",
        "D-018",
    ),
    AllowEntry(
        "storage/db/backup.py",
        MODULE_NAME,
        "module_lines",
        "513 行",
        "备份编排拆分属独立重构",
        "D-018",
    ),
    AllowEntry(
        "storage/repositories/resources.py",
        "ResourceRepository.ensure",
        "function_params",
        "5 个",
        "资源注册参数收敛属独立重构",
        "D-018",
    ),
)

_ALLOWLIST: tuple[AllowEntry, ...] = _D013_ENTRIES + _D018_ENTRIES


def scan_source(source: str, path: str) -> tuple[Violation, ...]:
    """扫一段源码并返回全部超限项；path 是报告用的相对路径。"""
    violations: list[Violation] = []
    total = len(source.splitlines())
    if total > MODULE_MAX_LINES:
        violations.append(Violation(path, 1, MODULE_NAME, "module_lines", total, MODULE_MAX_LINES))
    _LimitScanner(path, violations).scan(ast.parse(source))
    return tuple(violations)


def scan_repo(root: Path) -> tuple[Violation, ...]:
    """扫仓库三个包的全部 .py，返回全部超限项；顺序稳定，失败输出可 diff。"""
    violations: list[Violation] = []
    for path in _py_files(root):
        source = path.read_text(encoding="utf-8")
        violations.extend(scan_source(source, path.relative_to(root).as_posix()))
    return tuple(violations)


def _py_files(root: Path) -> tuple[Path, ...]:
    """按固定顺序收集三个包下的全部 .py 文件。"""
    return tuple(path for subdir in _SCAN_ROOTS for path in sorted((root / subdir).rglob("*.py")))


class _LimitScanner:
    """遍历 AST 收集超限项；报告路径与输出列表挂实例，递归只传作用域。"""

    def __init__(self, path: str, out: list[Violation]) -> None:
        self._path = path
        self._out = out

    def scan(self, node: ast.AST, prefix: str = "", in_class: bool = False) -> None:
        """递归收集函数与类的超限项；prefix 拼出 ``Class.method`` 限定名。"""
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = prefix + child.name
                self._check_function(child, name, in_class)
                self.scan(child, f"{name}.", False)
            elif isinstance(child, ast.ClassDef):
                name = prefix + child.name
                assert child.end_lineno is not None
                lines = child.end_lineno - child.lineno + 1
                if lines > CLASS_MAX_LINES:
                    self._out.append(
                        Violation(
                            self._path, child.lineno, name, "class_lines", lines, CLASS_MAX_LINES
                        )
                    )
                self.scan(child, f"{name}.", True)
            else:
                self.scan(child, prefix, in_class)

    def _check_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        name: str,
        in_class: bool,
    ) -> None:
        """对一个函数定义核对行数与参数个数两条上限。"""
        assert node.end_lineno is not None
        lines = node.end_lineno - node.lineno + 1
        if lines > FUNCTION_MAX_LINES:
            self._out.append(
                Violation(
                    self._path, node.lineno, name, "function_lines", lines, FUNCTION_MAX_LINES
                )
            )
        params = _param_count(node.args, in_class)
        if params > FUNCTION_MAX_PARAMS:
            self._out.append(
                Violation(
                    self._path, node.lineno, name, "function_params", params, FUNCTION_MAX_PARAMS
                )
            )


def _param_count(arguments: ast.arguments, in_class: bool) -> int:
    """参数个数；类里定义的函数去掉首个 self/cls，其余形参全算。"""
    count = (
        len(arguments.posonlyargs)
        + len(arguments.args)
        + len(arguments.kwonlyargs)
        + (1 if arguments.vararg is not None else 0)
        + (1 if arguments.kwarg is not None else 0)
    )
    positional = arguments.posonlyargs + arguments.args
    if in_class and positional and positional[0].arg in ("self", "cls"):
        count -= 1
    return count


def _allowed(violation: Violation) -> bool:
    """清单只按文件加名字匹配；类别与实测值允许随并行卡漂移。"""
    return any(
        entry.path == violation.path and entry.name == violation.name for entry in _ALLOWLIST
    )


def test_function_over_line_limit_reports_violation_with_remedy() -> None:
    source = "def wide() -> None:\n" + "".join(f"    value_{i} = {i}\n" for i in range(51))
    (violation,) = scan_source(source, "sample.py")
    assert violation.kind == "function_lines"
    assert (violation.actual, violation.limit) == (52, 50)
    assert violation.message == "sample.py:1:wide:52/50行（函数行数），改法：拆函数"


def test_function_over_param_limit_reports_violation_with_remedy() -> None:
    source = "def f(a: int, b: int, c: int, d: int, e: int) -> None:\n    return None\n"
    (violation,) = scan_source(source, "sample.py")
    assert violation.kind == "function_params"
    assert (violation.actual, violation.limit) == (5, 4)
    assert "收敛参数" in violation.message
    assert "sample.py:1:f:5/4" in violation.message


def test_class_over_line_limit_reports_violation_with_remedy() -> None:
    body = "".join(f"    value_{i} = {i}\n" for i in range(301))
    source = f"class Huge:\n{body}"
    (violation,) = scan_source(source, "sample.py")
    assert violation.kind == "class_lines"
    assert (violation.actual, violation.limit) == (302, 300)
    assert "拆类" in violation.message


def test_module_over_line_limit_reports_violation_with_remedy() -> None:
    source = "".join(f"value_{i} = {i}\n" for i in range(501))
    (violation,) = scan_source(source, "sample.py")
    assert violation.kind == "module_lines"
    assert (violation.actual, violation.limit) == (501, 500)
    assert violation.name == MODULE_NAME
    assert "拆模块" in violation.message


def test_method_self_and_cls_params_not_counted_in_param_limit() -> None:
    within = (
        "class C:\n    def m(self, a: int, b: int, c: int, d: int) -> None:\n        return None\n"
    )
    over = (
        "class C:\n"
        "    def m(self, a: int, b: int, c: int, d: int, e: int) -> None:\n"
        "        return None\n"
    )
    classmethod_over = (
        "class C:\n"
        "    @classmethod\n"
        "    def m(cls, a: int, b: int, c: int, d: int, e: int) -> None:\n"
        "        return None\n"
    )
    assert scan_source(within, "sample.py") == ()
    (method_hit,) = scan_source(over, "sample.py")
    (classmethod_hit,) = scan_source(classmethod_over, "sample.py")
    assert method_hit.actual == 5
    assert classmethod_hit.actual == 5


def test_allowlist_entries_complete_fields_explicit_keys_no_wildcards() -> None:
    keys: set[tuple[str, str, str]] = set()
    for entry in _ALLOWLIST:
        assert entry.path and entry.name and entry.kind and entry.measured and entry.why
        assert entry.kind in _KINDS
        assert re.fullmatch(r"D-\d{3}", entry.debt), entry.debt
        assert "/" in entry.path, entry.path
        assert not any(ch in f"{entry.path}{entry.name}" for ch in "*?["), entry
        keys.add((entry.path, entry.name, entry.kind))
    assert len(keys) == len(_ALLOWLIST)


def test_py_files_covers_core_storage_contracts() -> None:
    relatives = {p.relative_to(_REPO_ROOT).as_posix() for p in _py_files(_REPO_ROOT)}
    assert relatives, "三个包下应至少收集到一个 .py 文件"
    for subdir in _SCAN_ROOTS:
        assert any(rel.startswith(f"{subdir}/") for rel in relatives), subdir
    assert all(rel.split("/")[0] in _SCAN_ROOTS for rel in relatives)


def test_real_repo_no_violation_outside_allowlist() -> None:
    unexpected = [v for v in scan_repo(_REPO_ROOT) if not _allowed(v)]
    assert not unexpected, "\n".join(v.message for v in unexpected)


def test_key_entry_functions_within_limits() -> None:
    targets = {
        ("core/__main__.py", "_build_parser"),
        ("core/api/server.py", "start_api_server"),
        ("core/events/verdicts.py", "VerdictService.record"),
        ("core/jobs/run_service.py", "JobRunService.transition"),
        ("core/resources/service.py", "LeaseService.acquire"),
    }
    hits = [v for v in scan_repo(_REPO_ROOT) if (v.path, v.name) in targets]
    assert not hits, "\n".join(v.message for v in hits)


def test_debt_files_hits_all_allowlisted() -> None:
    violations = scan_repo(_REPO_ROOT)
    for path in ("core/worker/executor.py", "core/hostops/serve.py", "core/supervisor.py"):
        unlisted = [v for v in violations if v.path == path and not _allowed(v)]
        assert not unlisted, f"{path}\n" + "\n".join(v.message for v in unlisted)
