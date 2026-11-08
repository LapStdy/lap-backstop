"""MAA 配置投影能力（docs/plugins/maa.md 配置投影节，协议见 docs/05）。

``diff`` 是纯比较：对象字段递归展开，数组按整体值比较，按键名排序保证输出确定。

``apply`` 把期望配置作为整份目标写进 ``gui.new.json``，其中两处平台接管值写死为
程序值：``Gui.PostActions`` 固定 ``ExitSelf``、``Gui.ExternalNotification.Delivery.Enable``
置 false。写入顺序是「确认 MAA 没在跑 → 读现行文件 → 备份 → 原子替换 → 读回校验」，
读回不一致时把备份写回。``rollback`` 按修订号取回备份。文件与进程收在 ``ConfigStore``
端口后面，单元测试注入假件。
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from contracts import (
    ApplyResult,
    ConfigDiff,
    ConfigDiffKind,
    RunContext,
)
from plugins.maa.config_host import ConfigStore, WindowsConfigStore

DEFAULT_MAA_DIR = "H:/MAA"
CONFIG_REL = Path("config") / "gui.new.json"
DEFAULT_BACKUP_KEEP = 20
MAA_IMAGE_NAME = "MAA.exe"

_POST_ACTIONS_FIELD = "Gui.PostActions"
_PLATFORM_POST_ACTIONS = "ExitSelf"
_FORBIDDEN_POST_ACTIONS = frozenset({"ExitArknights", "ExitEmulator"})
_REVISION_PATTERN = re.compile(r"^[0-9]{8}-[0-9]{6}(?:-[0-9]+)?$")
_REVISION_FORMAT = "%Y%m%d-%H%M%S"


class _Rejected(Exception):
    """写入前的可预期拒绝：带契约错误码、人可读说明与插件私有的原因码。"""

    def __init__(self, code: str, detail: str, reason: str | None = None) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.reason = reason

    def result(self) -> ApplyResult:
        """把拒绝转成契约结果；原因码随 meta 交给内核与界面。"""
        meta: dict[str, Any] = {} if self.reason is None else {"reason": self.reason}
        return ApplyResult(ok=False, error_code=self.code, detail=self.detail, meta=meta)


@dataclass(frozen=True, slots=True)
class _Pending:
    """一次写入的输入快照：目标路径、现行字节、合并后的配置与冲突清单。"""

    config_path: Path
    raw: bytes
    merged: Mapping[str, Any]
    conflicts: list[dict[str, Any]]


class MaaConfigProjection:
    """config_projection 能力：差异可算，写入按平台规则接管两处字段。"""

    def __init__(self, store: ConfigStore | None = None) -> None:
        self._store = store

    def diff(
        self,
        ctx: RunContext,
        desired: Mapping[str, Any],
        actual: Mapping[str, Any],
    ) -> list[ConfigDiff]:
        """返回 desired 相对 actual 的逐字段差异；对称输入给出互为镜像的结果。"""
        return _diff_mappings(desired, actual, path="")

    def apply(self, ctx: RunContext, desired: Mapping[str, Any]) -> ApplyResult:
        """把整份期望配置写成平台值并落盘；成功返回修订号与写入哈希。"""
        store = self._store_for()
        config_path = _config_path(ctx.config)
        try:
            _ensure_unlocked(store)
            raw = store.read(config_path)
            current = _parse(raw, config_path)
            target, conflicts = _platform_target(ctx.config, desired, current)
        except _Rejected as rejected:
            return rejected.result()
        except OSError as error:
            return _failure("E_CONFIG", f"读取现行配置失败：{error}")
        if target == current:
            return ApplyResult(
                ok=True,
                detail="配置已一致：未写入",
                meta={"changed": False, "sha256": _sha256(raw), "conflicts": conflicts},
            )
        return self._commit(ctx, _Pending(config_path, raw, target, conflicts))

    def rollback(self, ctx: RunContext, revision: str) -> ApplyResult:
        """按修订号取回备份、原子写回并读回校验。"""
        if not isinstance(revision, str) or _REVISION_PATTERN.fullmatch(revision) is None:
            return _failure("E_CONFIG", f"修订号不合法：{revision!r}")
        store = self._store_for()
        config_path = _config_path(ctx.config)
        backup = _backup_path(config_path, revision)
        try:
            _ensure_unlocked(store)
            if backup not in store.backups(config_path):
                return _failure("E_CONFIG", f"没有修订 {revision} 的备份：{backup.name}")
            data = store.read(backup)
            restored = _parse(data, backup)
            store.write(config_path, data)
            if _parse(store.read(config_path), config_path) != restored:
                return _failure("E_CONFIG_INVALID", "回滚后读回内容与备份不一致")
        except _Rejected as rejected:
            return rejected.result()
        except OSError as error:
            return _failure("E_CONFIG", f"回滚配置失败：{error}")
        return ApplyResult(
            ok=True,
            detail=f"已恢复到修订 {revision}",
            meta={"revision": revision, "sha256": _sha256(data)},
        )

    def _store_for(self) -> ConfigStore:
        """真机端口按需构造：测试注入假件，生产用 Windows 实现。"""
        return self._store if self._store is not None else WindowsConfigStore()

    def _commit(self, ctx: RunContext, pending: _Pending) -> ApplyResult:
        """备份现行文件、原子写入并读回校验；不一致时把备份写回。"""
        store = self._store_for()
        revision = _next_revision(store, pending.config_path, ctx.clock.utc_now())
        backup = _backup_path(pending.config_path, revision)
        payload = _encode(pending.merged)
        try:
            store.write(backup, pending.raw)
            store.write(pending.config_path, payload)
            readback = _parse(store.read(pending.config_path), pending.config_path)
        except _Rejected as rejected:
            _restore(store, pending.config_path, pending.raw)
            return _invalid(revision, backup, f"{rejected.detail}；已回滚到备份")
        except OSError as error:
            return _failure("E_CONFIG", f"写入配置失败：{error}")
        if readback != pending.merged:
            _restore(store, pending.config_path, pending.raw)
            return _invalid(revision, backup, "读回内容与写入内容不一致，已回滚到备份")
        _prune(store, pending.config_path, _backup_keep(ctx.config))
        _log_conflicts(ctx, pending.config_path, pending.conflicts)
        return ApplyResult(
            ok=True,
            detail=f"配置已写入：修订 {revision}，备份 {backup.name}",
            meta={
                "changed": True,
                "revision": revision,
                "backup": str(backup),
                "sha256": _sha256(payload),
                "conflicts": pending.conflicts,
            },
        )


def _config_path(config: Mapping[str, Any]) -> Path:
    """插件配置的 maa_dir 指向 MAA 根目录；平台只写根下的 gui.new.json。"""
    return Path(str(config.get("maa_dir", DEFAULT_MAA_DIR))) / CONFIG_REL


def _ensure_unlocked(store: ConfigStore) -> None:
    """MAA 退出时会把内存配置写回文件，它在跑时写入会被覆盖。"""
    if store.process_running(MAA_IMAGE_NAME):
        raise _Rejected(
            "E_CONFIG_LOCKED",
            "MAA 正在运行：退出时会把内存配置写回文件，先停止 MAA 再写配置",
            reason="maa.config_locked",
        )


def _parse(raw: bytes, path: Path) -> dict[str, Any]:
    """把配置字节解析成对象；解析不了按配置错误拒绝，不写任何文件。"""
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _Rejected("E_CONFIG", f"配置文件无法解析：{path.name}（{error}）") from error
    if not isinstance(data, dict):
        raise _Rejected("E_CONFIG", f"配置文件的根不是对象：{path.name}")
    return data


def _profile_name(config: Mapping[str, Any], root: Mapping[str, Any]) -> str:
    """选配置段：插件配置的 profile 优先，空值时按现行文件的 Current。"""
    profile = config.get("profile")
    if isinstance(profile, str) and profile.strip():
        return profile.strip()
    current = root.get("Current")
    if isinstance(current, str) and current:
        return current
    raise _Rejected("E_CONFIG", "现行配置没有 Current，无法确定要写的配置段")


def _platform_target(
    config: Mapping[str, Any],
    desired: Mapping[str, Any],
    current: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """按期望配置构造写入内容：整份目标，两处接管值写死为平台值。"""
    target = copy.deepcopy(dict(desired))
    profile = _profile_name(config, target)
    conflicts = _current_conflicts(current, profile)
    _enforce_platform_fields(target, profile)
    return target, conflicts


def _current_conflicts(root: Mapping[str, Any], profile: str) -> list[dict[str, Any]]:
    """读现行文件的 PostActions，发现退出游戏或退出模拟器时记冲突。"""
    configs = root.get("Configurations")
    section = configs.get(profile) if isinstance(configs, Mapping) else None
    gui = section.get("Gui") if isinstance(section, Mapping) else None
    return _conflicts(gui.get("PostActions") if isinstance(gui, Mapping) else None)


def _enforce_platform_fields(root: dict[str, Any], profile: str) -> None:
    """写两处平台接管值；配置段缺失或结构不对时按配置错误拒绝。"""
    configs = root.get("Configurations")
    section = configs.get(profile) if isinstance(configs, Mapping) else None
    if not isinstance(section, dict):
        raise _Rejected("E_CONFIG", f"期望配置里没有配置段 {profile!r}")
    gui = _ensure_object(section, "Gui")
    gui["PostActions"] = _PLATFORM_POST_ACTIONS
    delivery = _ensure_object(_ensure_object(gui, "ExternalNotification"), "Delivery")
    delivery["Enable"] = False


def _ensure_object(parent: dict[str, Any], key: str) -> dict[str, Any]:
    """取或建一个对象子项；同名子项不是对象时按配置错误拒绝。"""
    value = parent.get(key)
    if value is None:
        created: dict[str, Any] = {}
        parent[key] = created
        return created
    if not isinstance(value, dict):
        raise _Rejected("E_CONFIG", f"配置项 {key} 不是对象")
    return value


def _conflicts(actual: object) -> list[dict[str, Any]]:
    """PostActions 含退出游戏或退出模拟器时记冲突；平台值本身不算冲突。"""
    if not isinstance(actual, str):
        return []
    tokens = {token.strip() for token in actual.split(",") if token.strip()}
    forbidden = sorted(tokens & _FORBIDDEN_POST_ACTIONS)
    if not forbidden:
        return []
    return [
        {
            "path": _POST_ACTIONS_FIELD,
            "actual": actual,
            "forbidden": forbidden,
            "error_code": "E_CONFIG_CONFLICT",
            "reason": "maa.post_actions_conflict",
        }
    ]


def _log_conflicts(
    ctx: RunContext,
    config_path: Path,
    conflicts: list[dict[str, Any]],
) -> None:
    """冲突已按平台值重写，仍写警告日志让事件链与排查有据可查。"""
    for conflict in conflicts:
        ctx.log.warn(
            {
                "event": "maa.config_conflict",
                "run_id": ctx.run_id,
                "target": str(config_path),
                **conflict,
            }
        )


def _backup_keep(config: Mapping[str, Any]) -> int:
    """取备份保留份数；缺省或非法值按 schema 默认值处理。"""
    settings = config.get("config_projection")
    value = settings.get("backup_keep") if isinstance(settings, Mapping) else None
    if isinstance(value, int) and not isinstance(value, bool) and value >= 1:
        return value
    return DEFAULT_BACKUP_KEEP


def _backup_path(config_path: Path, revision: str) -> Path:
    """备份文件名：``gui.new.json.<修订>.bak``，与 docs/plugins/maa.md 的写法一致。"""
    return config_path.with_name(f"{config_path.name}.{revision}.bak")


def _next_revision(store: ConfigStore, config_path: Path, now: datetime) -> str:
    """用 UTC 时刻做修订号；同一秒内多次写入追加序号，不覆盖已有备份。"""
    stamp = now.strftime(_REVISION_FORMAT)
    taken = {path.name for path in store.backups(config_path)}
    revision = stamp
    index = 2
    while _backup_path(config_path, revision).name in taken:
        revision = f"{stamp}-{index}"
        index += 1
    return revision


def _prune(store: ConfigStore, config_path: Path, keep: int) -> None:
    """只保留最近 ``keep`` 份备份；删不掉的备份不阻断本次写入。"""
    backups = store.backups(config_path)
    for stale in backups[: max(0, len(backups) - keep)]:
        try:
            store.delete(stale)
        except OSError:
            continue


def _restore(store: ConfigStore, config_path: Path, raw: bytes) -> None:
    """把备份内容写回；恢复失败由调用方的错误说明承担，不再抛给内核。"""
    try:
        store.write(config_path, raw)
    except OSError:
        return None


def _encode(data: Mapping[str, Any]) -> bytes:
    """按 MAA 的文件口径序列化：UTF-8 无 BOM、缩进 2、LF、末尾换行。"""
    return (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _sha256(data: bytes) -> str:
    """写入后哈希用于核对文件内容，不涉及密钥。"""
    return hashlib.sha256(data).hexdigest()


def _failure(code: str, detail: str) -> ApplyResult:
    """构造失败结果；原因码只在有对应插件码时由 ``_Rejected`` 附带。"""
    return ApplyResult(ok=False, error_code=code, detail=detail)


def _invalid(revision: str, backup: Path, detail: str) -> ApplyResult:
    """读回校验失败的收口：把修订与备份路径交给调用方，便于人工核对。"""
    return ApplyResult(
        ok=False,
        error_code="E_CONFIG_INVALID",
        detail=detail,
        meta={"revision": revision, "backup": str(backup)},
    )


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
