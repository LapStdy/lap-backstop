"""内核包：调度、作业、环境、资源、恢复、事件与 API。

依赖方向由 ADR 0001 固定：core 只能依赖 storage 与 contracts，不导入具体插件。
"""
