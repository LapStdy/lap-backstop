"""MAA 配置投影能力（docs/plugins/maa.md 配置投影节，协议见 docs/05）。

``diff`` 是纯比较：对象字段递归展开，数组按整体值比较，按键名排序保证输出
确定。``apply`` 与 ``rollback`` 不读写 ``gui.new.json``，一律返回 ``E_CONFIG``
失败；平台配置文件不被本能力修改。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from contracts import (
    ApplyResult,
    ConfigDiff,
    ConfigDiffKind,
    RunContext,
)

_WRITE_UNAVAILABLE = "配置写入不可用：未修改 gui.new.json"


class MaaConfigProjection:
    """config_projection 能力：差异可算，写入拒绝。"""

    def diff(
        self,
        ctx: RunContext,
        desired: Mapping[str, Any],
        actual: Mapping[str, Any],
    ) -> list[ConfigDiff]:
        """返回 desired 相对 actual 的逐字段差异；对称输入给出互为镜像的结果。"""
        return _diff_mappings(desired, actual, path="")

    def apply(self, ctx: RunContext, desired: Mapping[str, Any]) -> ApplyResult:
        """返回写入失败；不备份也不替换配置文件。"""
        return ApplyResult(ok=False, error_code="E_CONFIG", detail=_WRITE_UNAVAILABLE)

    def rollback(self, ctx: RunContext, revision: str) -> ApplyResult:
        """返回回滚失败；不读取也不恢复任何备份。"""
        return ApplyResult(ok=False, error_code="E_CONFIG", detail=_WRITE_UNAVAILABLE)


def _diff_mappings(
    desired: Mapping[str, Any],
    actual: Mapping[str, Any],
    path: str,
) -> list[ConfigDiff]:
    """递归比较两个对象；字段路径用点连接，键按名排序稳定输出。"""
    diffs: list[ConfigDiff] = []
    for key in sorted(set(desired) | set(actual)):
        child = f"{path}.{key}" if path else str(key)
        if key not in actual:
            diffs.append(
                ConfigDiff(path=child, desired=desired[key], actual=None, kind=ConfigDiffKind.ADDED)
            )
        elif key not in desired:
            diffs.append(
                ConfigDiff(
                    path=child, desired=None, actual=actual[key], kind=ConfigDiffKind.REMOVED
                )
            )
        else:
            diffs.extend(_diff_values(child, desired[key], actual[key]))
    return diffs


def _diff_values(path: str, desired: Any, actual: Any) -> list[ConfigDiff]:
    """比较同名键的值：双侧对象时下钻，其余按整体相等性比较。"""
    if isinstance(desired, Mapping) and isinstance(actual, Mapping):
        return _diff_mappings(desired, actual, path)
    if desired != actual:
        return [ConfigDiff(path=path, desired=desired, actual=actual, kind=ConfigDiffKind.CHANGED)]
    return []
