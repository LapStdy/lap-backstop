"""插件 manifest：字段校验与契约版本范围判断。

唯一真源是 docs/05 的 manifest 一节；JSON Schema 副本在 contracts/manifest.schema.json。
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from contracts.errors import ContractError, ErrorCode
from contracts.types import ErrorCategory, RecoveryAction

CAPABILITIES: Final[frozenset[str]] = frozenset(
    {"environment", "script", "verifier", "config_projection", "notifier", "advisor"}
)

_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]{1,31}$")
_VERSION_RE: Final[re.Pattern[str]] = re.compile(r"^\d+\.\d+\.\d+$")
_RANGE_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"^(>=|<=|==|>|<)?(\d+)$")


@dataclass(frozen=True, slots=True)
class ExecutableRequirement:
    name: str
    min_version: str | None = None


@dataclass(frozen=True, slots=True)
class ConfigMigration:
    from_version: int
    to_version: int
    handler: str


@dataclass(frozen=True, slots=True)
class ErrorMapEntry:
    category: ErrorCategory
    retryable: bool
    recovery: RecoveryAction
    message: str = ""


@dataclass(frozen=True, slots=True)
class Requires:
    contract: str
    platform: str = "windows"
    python: str | None = None
    executables: tuple[ExecutableRequirement, ...] = ()


@dataclass(frozen=True, slots=True)
class Manifest:
    id: str
    name: str
    version: str
    api_version: int
    capabilities: tuple[str, ...]
    config_schema: str
    requires: Requires
    description: str = ""
    error_map: Mapping[str, ErrorMapEntry] = field(default_factory=dict)
    config_migrations: tuple[ConfigMigration, ...] = ()

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Manifest:
        manifest = cls(
            id=_require_str(data, "id"),
            name=_require_str(data, "name"),
            version=_require_str(data, "version"),
            api_version=_require_int(data, "api_version"),
            capabilities=tuple(_require_list(data, "capabilities")),
            config_schema=_require_str(data, "config_schema"),
            requires=_parse_requires(data.get("requires")),
            description=_optional_str(data, "description"),
            error_map=_parse_error_map(data.get("error_map")),
            config_migrations=_parse_migrations(data.get("config_migrations")),
        )
        manifest.validate()
        return manifest

    def validate(self) -> None:
        """字面规则与自洽校验；失败抛 E_PLUGIN_MANIFEST。"""
        if not _ID_RE.match(self.id):
            raise _bad(f"id 不合规则：{self.id!r}")
        if not _VERSION_RE.match(self.version):
            raise _bad(f"version 不是语义化版本：{self.version!r}")
        if not self.capabilities:
            raise _bad("capabilities 不能为空")
        unknown = set(self.capabilities) - CAPABILITIES
        if unknown:
            raise _bad(f"未知能力：{sorted(unknown)}")
        schema_path = self.config_schema
        if not schema_path or schema_path.startswith(("/", "\\")) or ".." in schema_path:
            raise _bad(f"config_schema 必须是包内相对路径：{schema_path!r}")
        if not range_covers(self.requires.contract, self.api_version):
            raise _bad(
                f"api_version={self.api_version} "
                f"不在 requires.contract={self.requires.contract!r} 内"
            )
        if self.requires.platform != "windows":
            raise _bad(f"只支持 windows：{self.requires.platform!r}")
        for migration in self.config_migrations:
            if migration.to_version <= migration.from_version:
                raise _bad(
                    "config_migrations 版本区间不递增："
                    f"{migration.from_version}->{migration.to_version}"
                )
            if not migration.handler:
                raise _bad("config_migrations 缺少 handler")


def range_covers(spec: str, version: int) -> bool:
    """判断整数契约版本是否落在 spec 描述的范围内。

    支持的写法：`>=1 <2`、`>=1,<2`、`==1`、`1`；空字符串视为不限制。
    """
    text = spec.strip()
    if not text:
        return True
    tokens = [token for token in re.split(r"[\s,]+", text) if token]
    checks: list[tuple[str, int]] = []
    for token in tokens:
        match = _RANGE_TOKEN_RE.match(token)
        if match is None:
            raise _bad(f"requires.contract 写法不认识：{token!r}")
        checks.append((match.group(1) or "==", int(match.group(2))))
    return all(_compare(op, version, bound) for op, bound in checks)


def _compare(op: str, version: int, bound: int) -> bool:
    if op == ">=":
        return version >= bound
    if op == "<=":
        return version <= bound
    if op == ">":
        return version > bound
    if op == "<":
        return version < bound
    return version == bound


def _parse_requires(raw: object) -> Requires:
    if not isinstance(raw, Mapping):
        raise _bad("requires 必须是对象")
    contract = raw.get("contract")
    if not isinstance(contract, str) or not contract:
        raise _bad("requires.contract 必须是非空字符串")
    platform = raw.get("platform", "windows")
    if not isinstance(platform, str):
        raise _bad("requires.platform 必须是字符串")
    python = raw.get("python")
    if python is not None and not isinstance(python, str):
        raise _bad("requires.python 必须是字符串")
    executables_raw = raw.get("executables", [])
    if not isinstance(executables_raw, list):
        raise _bad("requires.executables 必须是数组")
    executables: list[ExecutableRequirement] = []
    for item in executables_raw:
        if not isinstance(item, Mapping):
            raise _bad("requires.executables 的每项必须是对象")
        name = item.get("name")
        if not isinstance(name, str) or not name:
            raise _bad("requires.executables[].name 必须是非空字符串")
        min_version = item.get("min_version")
        if min_version is not None and not isinstance(min_version, str):
            raise _bad("requires.executables[].min_version 必须是字符串")
        executables.append(ExecutableRequirement(name=name, min_version=min_version))
    return Requires(
        contract=contract,
        platform=platform,
        python=python,
        executables=tuple(executables),
    )


def _parse_error_map(raw: object) -> dict[str, ErrorMapEntry]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise _bad("error_map 必须是对象")
    entries: dict[str, ErrorMapEntry] = {}
    for code, value in raw.items():
        if not isinstance(code, str) or not code.startswith("E_"):
            raise _bad(f"error_map 的键必须是 E_* 错误码：{code!r}")
        if not isinstance(value, Mapping):
            raise _bad(f"error_map[{code}] 必须是对象")
        try:
            category = ErrorCategory(value["category"])
            recovery = RecoveryAction(value["recovery"])
        except KeyError as exc:
            raise _bad(f"error_map[{code}] 缺少字段：{exc}") from exc
        except ValueError as exc:
            raise _bad(f"error_map[{code}] 取值不合法：{exc}") from exc
        retryable = value.get("retryable", False)
        if not isinstance(retryable, bool):
            raise _bad(f"error_map[{code}].retryable 必须是布尔")
        message = value.get("message", "")
        if not isinstance(message, str):
            raise _bad(f"error_map[{code}].message 必须是字符串")
        entries[code] = ErrorMapEntry(
            category=category,
            retryable=retryable,
            recovery=recovery,
            message=message,
        )
    return entries


def _parse_migrations(raw: object) -> tuple[ConfigMigration, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise _bad("config_migrations 必须是数组")
    migrations: list[ConfigMigration] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise _bad("config_migrations 的每项必须是对象")
        from_version = item.get("from")
        to_version = item.get("to")
        handler = item.get("handler")
        if not isinstance(from_version, int) or not isinstance(to_version, int):
            raise _bad("config_migrations[].from/to 必须是整数")
        if not isinstance(handler, str) or not handler:
            raise _bad("config_migrations[].handler 必须是非空字符串")
        migrations.append(
            ConfigMigration(from_version=from_version, to_version=to_version, handler=handler)
        )
    return tuple(migrations)


def _require_str(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise _bad(f"字段 {key} 必须是非空字符串")
    return value


def _optional_str(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key, "")
    if not isinstance(value, str):
        raise _bad(f"字段 {key} 必须是字符串")
    return value


def _require_int(data: Mapping[str, Any], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise _bad(f"字段 {key} 必须是整数")
    return value


def _require_list(data: Mapping[str, Any], key: str) -> list[str]:
    value = data.get(key)
    if not isinstance(value, list) or not value:
        raise _bad(f"字段 {key} 必须是非空数组")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise _bad(f"字段 {key} 的元素必须是字符串")
        items.append(item)
    return items


def _bad(message: str) -> ContractError:
    return ContractError(ErrorCode.PLUGIN_MANIFEST, message)


def kernel_rejects(manifest: Manifest, kernel_contract_version: int) -> bool:
    """内核加载前的版本门槛：插件声明的范围不覆盖内核版本即拒绝加载。"""
    return not range_covers(manifest.requires.contract, kernel_contract_version)
