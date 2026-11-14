"""worker 生命周期场景的共享夹具：env 在支撑模块定义，经此对目录内测试可见。"""

from __future__ import annotations

from tests.scenarios.worker_lifecycle_support import env  # noqa: F401 —— pytest 按参数名解析夹具
