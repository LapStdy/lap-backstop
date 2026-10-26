"""导出端点：配置、模板、调度与运行摘要的 zip（docs/03 GET /export）。

zip 在内存组装，不往数据目录写临时文件；条目内容与 JSON 文件导出共用
同一收集器，口径不会漂移。token 与密钥键不入包（docs/03 备份口径）。
"""

from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC

from contracts import Clock
from core.api import catalog_store, run_store
from core.api.ctx import RequestCtx
from core.api.wire import ApiResponse, BinaryBody
from storage.db.backup import TIMESTAMP_FORMAT
from storage.repositories.config_export import collect_export_data
from storage.repositories.iso_time import format_at


@dataclass(frozen=True, slots=True)
class ExportBundle:
    """一次导出的成品：zip 字节与下载文件名。"""

    filename: str
    body: bytes


def get_export(ctx: RequestCtx) -> ApiResponse:
    """导出 zip：settings、plugins、job_templates、schedules、runs 摘要。

    Raises:
        sqlite3.Error: 导出过程中的数据库读失败，由应用层按 500 与
            可读 message 应答。
    """
    bundle = build_export(ctx.connection, clock=ctx.clock)
    return ApiResponse(
        status=200,
        payload=None,
        binary=BinaryBody(
            content_type="application/zip", data=bundle.body, filename=bundle.filename
        ),
    )


def build_export(connection: sqlite3.Connection, *, clock: Clock) -> ExportBundle:
    """组装导出 zip：五个 JSON 条目，键名与对应端点应答一致。"""
    created_at = clock.utc_now().astimezone(UTC)
    values, plugins = collect_export_data(connection)
    entries: dict[str, dict[str, object]] = {
        "settings.json": {"created_at": format_at(created_at), "values": values},
        "plugins.json": {"created_at": format_at(created_at), "plugins": plugins},
        "job-templates.json": {
            "created_at": format_at(created_at),
            "job_templates": catalog_store.template_rows(connection),
        },
        "schedules.json": {
            "created_at": format_at(created_at),
            "schedules": catalog_store.schedule_rows(connection),
        },
        "runs.json": {
            "created_at": format_at(created_at),
            "runs": run_store.run_summary_rows(connection),
        },
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in entries.items():
            archive.writestr(name, _dump(payload))
    stamp = created_at.strftime(TIMESTAMP_FORMAT)
    return ExportBundle(
        filename=f"lap-backstop-export-{stamp}.zip",
        body=buffer.getvalue(),
    )


def _dump(payload: Mapping[str, object]) -> str:
    """条目 JSON 编码；键排序与端点应答同一口径。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


__all__ = ["ExportBundle", "build_export", "get_export"]
