"""MAA 配置投影的写入、接管、备份与回滚（docs/plugins/maa.md 配置投影节）。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from plugins.maa.capabilities.config_projection import MaaConfigProjection
from tests.config_store_support import (
    FakeConfigStore,
    RecordingLogger,
    config_path,
    encode,
    platform_config,
    projection_context,
    sample_config,
)

REQS = ("M-107", "M-108")

REVISION = "20261025-120000"


def _desired(task_queue: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """平台期望的整份配置；默认清空任务队列，制造一次真实写入。"""
    config = platform_config()
    config["Configurations"]["Default"]["TaskQueue"] = [] if task_queue is None else task_queue
    return config


def _store(tmp_path: Path, *, running: bool = False) -> tuple[FakeConfigStore, Path]:
    path = config_path(tmp_path)
    return FakeConfigStore({path: encode(sample_config())}, running=running), path


def _context(tmp_path: Path, logger: RecordingLogger | None = None, **config: Any) -> Any:
    return projection_context({"maa_dir": str(tmp_path), **config}, logger=logger)


def _apply(
    tmp_path: Path,
    store: FakeConfigStore,
    desired: dict[str, Any] | None = None,
    **config: Any,
) -> Any:
    projection = MaaConfigProjection(store=store)
    target = _desired() if desired is None else desired
    return projection.apply(_context(tmp_path, **config), target)


def _written(store: FakeConfigStore, path: Path) -> dict[str, Any]:
    return json.loads(store.files[path].decode("utf-8"))


def test_apply_writes_takeover_fields_and_keeps_a_backup(tmp_path: Path) -> None:
    """写入后两处平台接管值生效，原内容进备份，结果带修订号、哈希与冲突。"""
    store, path = _store(tmp_path)
    result = _apply(tmp_path, store)
    assert result.ok is True
    assert result.meta["changed"] is True
    assert result.meta["revision"] == REVISION
    backup = Path(str(result.meta["backup"]))
    assert backup.name == f"gui.new.json.{REVISION}.bak"
    assert store.files[backup] == encode(sample_config())
    written = _written(store, path)
    assert written["Current"] == "Default"
    assert written["Configurations"]["Default"]["TaskQueue"] == []
    gui = written["Configurations"]["Default"]["Gui"]
    assert gui["PostActions"] == "ExitSelf"
    assert gui["ExternalNotification"]["Delivery"]["Enable"] is False
    assert written["ConfigVersion"] == 1
    assert result.meta["sha256"] == hashlib.sha256(store.files[path]).hexdigest()
    assert [item["error_code"] for item in result.meta["conflicts"]] == ["E_CONFIG_CONFLICT"]


def test_apply_logs_conflict_when_post_actions_exit_game_or_emulator(tmp_path: Path) -> None:
    """PostActions 含退出游戏或退出模拟器时按平台值重写，并留一条冲突警告。"""
    store, _ = _store(tmp_path)
    logger = RecordingLogger()
    projection = MaaConfigProjection(store=store)
    result = projection.apply(_context(tmp_path, logger=logger), _desired())
    assert result.meta["conflicts"][0]["reason"] == "maa.post_actions_conflict"
    events = [record.get("event") for record in logger.records]
    assert "maa.config_conflict" in events


def test_apply_is_idempotent_after_first_write(tmp_path: Path) -> None:
    """同一期望再写一次不落盘：不产生新备份，冲突为空。"""
    store, path = _store(tmp_path)
    assert _apply(tmp_path, store).ok is True
    second = _apply(tmp_path, store)
    assert second.ok is True
    assert second.meta["changed"] is False
    assert second.meta["conflicts"] == []
    assert len(store.backups(path)) == 1


def test_apply_refuses_when_maa_running(tmp_path: Path) -> None:
    """MAA 在跑时拒绝写入：不备份、不替换，原因码随 meta 返回。"""
    store, _ = _store(tmp_path, running=True)
    result = _apply(tmp_path, store)
    assert result.ok is False
    assert result.error_code == "E_CONFIG_LOCKED"
    assert result.meta["reason"] == "maa.config_locked"
    assert store.written == []


def test_apply_restores_original_when_readback_differs(tmp_path: Path) -> None:
    """读回内容与写入内容不一致时把原内容写回，按 E_CONFIG_INVALID 失败。"""
    store, path = _store(tmp_path)
    original = store.files[path]

    def corrupt(target: Path, data: bytes) -> bytes:
        if target == path:
            return data.replace(b'"ExitSelf"', b'"None"')
        return data

    store.on_write = corrupt
    result = _apply(tmp_path, store)
    assert result.ok is False
    assert result.error_code == "E_CONFIG_INVALID"
    assert store.files[path] == original


def test_apply_prunes_old_backups(tmp_path: Path) -> None:
    """备份只保留最近 backup_keep 份，更早的删掉。"""
    store, path = _store(tmp_path)
    stale = [path.with_name(f"gui.new.json.20261024-12000{index}.bak") for index in range(3)]
    for item in stale:
        store.files[item] = b"stale"
    result = _apply(tmp_path, store, config_projection={"backup_keep": 3})
    assert result.ok is True
    assert len(store.backups(path)) == 3
    assert store.deleted == [stale[0]]


def test_apply_reports_missing_profile_section(tmp_path: Path) -> None:
    """插件配置指定的配置段不在期望配置里时拒绝写入。"""
    store, _ = _store(tmp_path)
    result = _apply(tmp_path, store, profile="Missing")
    assert result.ok is False
    assert result.error_code == "E_CONFIG"
    assert store.written == []


def test_apply_reports_unparsable_current_file(tmp_path: Path) -> None:
    """现行文件解析不了时拒绝写入，不动原文件。"""
    store, path = _store(tmp_path)
    store.files[path] = b"{not json"
    result = _apply(tmp_path, store)
    assert result.ok is False
    assert result.error_code == "E_CONFIG"
    assert store.written == []


def test_apply_writes_utf8_without_bom_and_lf_endings(tmp_path: Path) -> None:
    """写入形态与 MAA 的文件口径一致：无 BOM、LF、末尾换行。"""
    store, path = _store(tmp_path)
    assert _apply(tmp_path, store).ok is True
    payload = store.files[path]
    assert payload.startswith(b"{\n")
    assert payload.endswith(b"\n")
    assert b"\r\n" not in payload
    assert not payload.startswith(b"\xef\xbb\xbf")


def test_platform_first_apply_leaves_empty_diff(tmp_path: Path) -> None:
    """docs/05 契约测试表 config_projection 行：`platform_first` 写整份目标，apply 后 diff 为空。"""
    store, path = _store(tmp_path)
    projection = MaaConfigProjection(store=store)
    ctx = _context(tmp_path, config_projection={"strategy": "platform_first"})
    assert projection.apply(ctx, _desired()).ok is True
    assert _written(store, path) == _desired()
    assert projection.diff(ctx, _desired(), _written(store, path)) == []


def test_script_first_keeps_script_side_fields_and_writes_platform_fields(tmp_path: Path) -> None:
    """默认策略：现行文件的普通字段原样保留，平台只覆盖 Current、TaskQueue 与接管值。"""
    store, path = _store(tmp_path)
    script_side = sample_config()
    script_side["ConfigVersion"] = 2
    script_side["Configurations"]["Default"]["Gui"]["MaaStartupDelay"] = 30
    script_side["Configurations"]["Default"]["Copilot"] = {"Enabled": False}
    store.files[path] = encode(script_side)
    result = _apply(tmp_path, store)
    assert result.ok is True
    written = _written(store, path)
    assert written["ConfigVersion"] == 2
    assert written["Configurations"]["Default"]["Gui"]["MaaStartupDelay"] == 30
    assert written["Configurations"]["Default"]["Copilot"] == {"Enabled": False}
    assert written["Configurations"]["Default"]["TaskQueue"] == []


def test_platform_first_drops_script_side_fields(tmp_path: Path) -> None:
    """`platform_first` 按整份目标覆盖：现行文件里目标未覆盖的键不留。"""
    store, path = _store(tmp_path)
    script_side = sample_config()
    script_side["ConfigVersion"] = 2
    script_side["Configurations"]["Default"]["Gui"]["MaaStartupDelay"] = 30
    store.files[path] = encode(script_side)
    result = _apply(tmp_path, store, config_projection={"strategy": "platform_first"})
    assert result.ok is True
    written = _written(store, path)
    assert "MaaStartupDelay" not in written["Configurations"]["Default"]["Gui"]
    assert written["ConfigVersion"] == 1
    assert _desired() == written


def test_unknown_strategy_falls_back_to_script_first(tmp_path: Path) -> None:
    """非法策略值按默认处理：保留普通字段，不因拼写错误整份覆盖。"""
    store, path = _store(tmp_path)
    script_side = sample_config()
    script_side["ConfigVersion"] = 3
    store.files[path] = encode(script_side)
    assert _apply(tmp_path, store, config_projection={"strategy": "nonsense"}).ok is True
    assert _written(store, path)["ConfigVersion"] == 3


def test_script_first_creates_profile_section_when_current_file_lacks_it(tmp_path: Path) -> None:
    """现行文件只有 Current、没有配置段时补建该段，平台字段照常写入。"""
    store, path = _store(tmp_path)
    store.files[path] = encode({"Current": "Default"})
    result = _apply(tmp_path, store)
    assert result.ok is True
    written = _written(store, path)
    assert written["Configurations"]["Default"]["TaskQueue"] == []
    assert written["Configurations"]["Default"]["Gui"]["PostActions"] == "ExitSelf"


def test_rollback_restores_original_bytes(tmp_path: Path) -> None:
    """rollback 按修订号取回备份，文件字节回到写入前。"""
    store, path = _store(tmp_path)
    original = store.files[path]
    projection = MaaConfigProjection(store=store)
    ctx = _context(tmp_path)
    applied = projection.apply(ctx, _desired())
    rolled = projection.rollback(ctx, str(applied.meta["revision"]))
    assert rolled.ok is True
    assert store.files[path] == original
    assert projection.diff(ctx, _desired(), _written(store, path))


def test_rollback_rejects_unexpected_revision(tmp_path: Path) -> None:
    """修订号只接受备份文件的时刻形态，拒绝路径片段。"""
    store, _ = _store(tmp_path)
    projection = MaaConfigProjection(store=store)
    ctx = _context(tmp_path)
    assert projection.rollback(ctx, "../gui.new.json").error_code == "E_CONFIG"
    assert projection.rollback(ctx, "2026-10-25 12:00").error_code == "E_CONFIG"
    assert store.written == []


def test_rollback_reports_missing_backup(tmp_path: Path) -> None:
    """没有对应修订的备份时按配置错误失败。"""
    store, _ = _store(tmp_path)
    result = MaaConfigProjection(store=store).rollback(_context(tmp_path), "20260101-000000")
    assert result.ok is False
    assert result.error_code == "E_CONFIG"


def test_rollback_refuses_when_maa_running(tmp_path: Path) -> None:
    """MAA 在跑时回滚同样拒绝，文件保持写入后的内容。"""
    store, path = _store(tmp_path)
    projection = MaaConfigProjection(store=store)
    ctx = _context(tmp_path)
    applied = projection.apply(ctx, _desired())
    after_apply = store.files[path]
    store.running = True
    result = projection.rollback(ctx, str(applied.meta["revision"]))
    assert result.ok is False
    assert result.error_code == "E_CONFIG_LOCKED"
    assert store.files[path] == after_apply


def test_second_write_in_same_second_gets_suffixed_revision(tmp_path: Path) -> None:
    """同一秒内两次不同的写入各占一个备份，修订号追加序号。"""
    store, path = _store(tmp_path)
    first = _apply(tmp_path, store)
    second = _apply(tmp_path, store, _desired([{"$type": "CustomTask"}]))
    assert first.meta["revision"] == REVISION
    assert second.meta["revision"] == f"{REVISION}-2"
    assert len(store.backups(path)) == 2
