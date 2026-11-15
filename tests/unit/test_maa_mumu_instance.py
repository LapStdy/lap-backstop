"""MuMuManager info 解析：字段取自 2026-11-15 本机实测（MuMu 6.5.4.0）。

实测样本里没有 ``player_state`` 与 ``main_wnd``，实例判据是
``is_process_started`` 与 ``is_android_started``。
"""

from __future__ import annotations

from pathlib import Path

from plugins.maa.mumu_instance import MumuInstance, parse_instance_info, read_instance_info

_INSTANCE_OFF = """{
  "android_version": "15.0",
  "created_timestamp": 1784311503401163,
  "disk_size_bytes": 28700919866,
  "error_code": 0,
  "hyperv_enabled": true,
  "index": "0",
  "info_source": "rpc",
  "is_android_started": false,
  "is_main": false,
  "is_process_started": false,
  "name": "明日方舟"
}"""


def test_parse_real_offline_sample() -> None:
    """实测样本（实例未启动）：字段原样解析，两个布尔都取 False。"""
    instance = parse_instance_info(_INSTANCE_OFF)
    assert instance == MumuInstance(
        index=0,
        name="明日方舟",
        android_version="15.0",
        is_process_started=False,
        is_android_started=False,
    )


def test_parse_started_sample() -> None:
    """实例进程与 Android 都起来了：两个布尔都取 True。"""
    text = _INSTANCE_OFF.replace('"is_android_started": false', '"is_android_started": true')
    text = text.replace('"is_process_started": false', '"is_process_started": true')
    instance = parse_instance_info(text)
    assert instance is not None
    assert instance.is_process_started and instance.is_android_started


def test_parse_returns_none_on_error_code() -> None:
    """error_code 非 0（实例不存在等）不成结论，交调用方退回窗口判据。"""
    assert parse_instance_info(_INSTANCE_OFF.replace('"error_code": 0', '"error_code": 3')) is None


def test_parse_returns_none_when_field_missing() -> None:
    """字段随版本改，缺实例判据字段时返回 None，不按缺省值猜。"""
    text = _INSTANCE_OFF.replace('  "is_android_started": false,\n', "")
    assert parse_instance_info(text) is None


def test_parse_returns_none_on_broken_json() -> None:
    """输出被额外日志污染或截断时返回 None。"""
    assert parse_instance_info("not json") is None
    assert parse_instance_info(_INSTANCE_OFF[:40]) is None


def test_parse_returns_none_when_index_not_numeric() -> None:
    """index 不是非负整数字面量时不成结论。"""
    assert parse_instance_info(_INSTANCE_OFF.replace('"index": "0"', '"index": "x"')) is None


def test_read_instance_info_returns_none_without_manager(tmp_path: Path) -> None:
    """MuMuManager 不存在时返回 None，不抛异常给探测路径。"""
    assert read_instance_info(tmp_path / "MuMuManager.exe", 0) is None
