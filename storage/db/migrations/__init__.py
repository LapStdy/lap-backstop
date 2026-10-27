"""迁移清单：按版本升序，编号四位、只前滚。"""

from __future__ import annotations

from importlib import import_module

from storage.db.migrations.runner import Migration, load_up


def _load(version: int, name: str) -> Migration:
    module = import_module(f"storage.db.migrations.{version:04d}_{name}")
    return Migration(version=version, name=name, up=load_up(module))


MIGRATIONS: tuple[Migration, ...] = (_load(1, "initial"), _load(2, "plugin_config"))
