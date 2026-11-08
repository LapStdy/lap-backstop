"""配置投影用例的共享假件：内存文件树、固定时钟与最小 RunContext。"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from contracts import RunContext

FIXED_NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
CONFIG_NAME = "gui.new.json"


def config_path(maa_dir: Path) -> Path:
    """被测代码的落点：``<maa_dir>/config/gui.new.json``。"""
    return maa_dir / "config" / CONFIG_NAME


def sample_config() -> dict[str, Any]:
    """贴近现行 gui.new.json 的最小结构：一套配置、两处接管字段与一条任务。"""
    return {
        "ConfigVersion": 1,
        "Current": "Default",
        "Configurations": {
            "Default": {
                "TaskQueue": [{"$type": "StartUpTask", "Name": "", "IsEnable": True}],
                "Gui": {
                    "PostActions": "ExitEmulator, ExitSelf",
                    "ExternalNotification": {
                        "Delivery": {"Enable": True},
                    },
                },
            }
        },
    }


def platform_config() -> dict[str, Any]:
    """平台视角的整份期望配置：任务队列保留一条，两处接管值已是平台值。"""
    config = sample_config()
    section = config["Configurations"]["Default"]
    section["Gui"]["PostActions"] = "ExitSelf"
    section["Gui"]["ExternalNotification"]["Delivery"]["Enable"] = False
    return config


def encode(data: Any) -> bytes:
    """与 MAA 文件同口径的字节形态：UTF-8、缩进 2、LF、末尾换行。"""
    return (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


class FakeConfigStore:
    """内存文件树假件：记录写入与删除，可模拟进程占用与写入被篡改。"""

    def __init__(
        self,
        files: Mapping[Path, bytes] | None = None,
        *,
        running: bool = False,
        on_write: Callable[[Path, bytes], bytes] | None = None,
    ) -> None:
        self.files: dict[Path, bytes] = {Path(path): data for path, data in (files or {}).items()}
        self.running = running
        self.on_write = on_write
        self.written: list[Path] = []
        self.deleted: list[Path] = []

    def read(self, path: Path) -> bytes:
        """读不到的文件按 FileNotFoundError 上抛，与真机端口的 OSError 语义一致。"""
        try:
            return self.files[Path(path)]
        except KeyError:
            raise FileNotFoundError(str(path)) from None

    def write(self, path: Path, data: bytes) -> None:
        """写入落盘；``on_write`` 可改写字节，用来模拟读回与写入不一致。"""
        target = Path(path)
        stored = self.on_write(target, data) if self.on_write is not None else data
        self.files[target] = stored
        self.written.append(target)

    def backups(self, config_path: Path) -> tuple[Path, ...]:
        """按文件名升序列出同目录的 ``<文件名>.<修订>.bak``。"""
        config = Path(config_path)
        prefix = f"{config.name}."
        found = (
            path
            for path in self.files
            if path.parent == config.parent
            and path.name.startswith(prefix)
            and path.name.endswith(".bak")
        )
        return tuple(sorted(found, key=lambda path: path.name))

    def delete(self, path: Path) -> None:
        """删除备份；文件不在时静默返回，与真机端口一致。"""
        self.deleted.append(Path(path))
        self.files.pop(Path(path), None)

    def process_running(self, image_name: str) -> bool:
        """按构造时的开关回答进程占用。"""
        return self.running


class RecordingLogger:
    """结构化日志假件：留下每条字段，供断言冲突日志用。"""

    def __init__(self) -> None:
        self.records: list[Mapping[str, Any]] = []

    def debug(self, fields: Mapping[str, Any]) -> None:
        """记录 debug 字段。"""
        self.records.append(fields)

    def info(self, fields: Mapping[str, Any]) -> None:
        """记录 info 字段。"""
        self.records.append(fields)

    def warn(self, fields: Mapping[str, Any]) -> None:
        """记录 warn 字段。"""
        self.records.append(fields)

    def error(self, fields: Mapping[str, Any]) -> None:
        """记录 error 字段。"""
        self.records.append(fields)


class _Clock:
    """虚拟时钟：时间恒定，测试不读系统时间。"""

    def monotonic(self) -> float:
        return 0.0

    def utc_now(self) -> datetime:
        return FIXED_NOW


class _CancelFlag:
    """取消令牌假件：永不取消。"""

    def is_cancelled(self) -> bool:
        return False


def projection_context(
    config: Mapping[str, Any],
    logger: RecordingLogger | None = None,
) -> RunContext:
    """最小 RunContext：固定时钟、可记录的日志、不取消。"""
    return RunContext(
        run_id="run-1",
        attempt=1,
        template_id="template-1",
        plugin_id="maa",
        environment_id="env-1",
        config=config,
        work_dir=Path("."),
        evidence_dir=Path("."),
        deadline_at=FIXED_NOW + timedelta(minutes=5),
        cancel=_CancelFlag(),
        clock=_Clock(),
        secrets={},
        log=logger if logger is not None else RecordingLogger(),
        emit=lambda event, payload: None,
        record_evidence=lambda kind, path, summary: "evidence-1",
    )
