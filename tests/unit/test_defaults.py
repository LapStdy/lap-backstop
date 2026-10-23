"""默认值文件与加载器：唯一真源可读、缺键报错。"""

from __future__ import annotations

import pytest

from storage.defaults import DEFAULTS_PATH, default, load_defaults


def test_defaults_file_is_present_and_parsed() -> None:
    assert DEFAULTS_PATH.is_file()
    data = load_defaults()
    assert data["worker"]["max_observations"] == 10
    assert data["recovery"]["max_attempts"] == 3


def test_default_returns_section_value() -> None:
    assert default("leases", "renew_interval_seconds") == 60


def test_unknown_key_reports_section_and_key() -> None:
    with pytest.raises(KeyError) as excinfo:
        default("worker", "does_not_exist")
    assert "worker.does_not_exist" in str(excinfo.value)
