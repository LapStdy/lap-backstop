# 技术债台账

[返回 README](README.md)

记录所有绕行、欠测试、欠重构、兼容层与手工步骤。规则与评审流程见 [13 · 变更与演进](docs/13-变更与演进.md)。

## 规则

- 编号 `D-001` 递增，不复用、不跳号。
- 每笔债必须有归还期限：具体日期或版本号；写不出期限的当场决定做或不做，不入账。
- 同时未还的债务不超过 20 笔；已满时先还债，再加新功能。
- 每月复审一次；逾期项在阶段体检记录里单列，并说明顺延理由与新期限。
- 已还的条目删除，还债内容进提交说明与 CHANGELOG。
- 绕过门禁的紧急修复必须记一笔，写清绕过了哪几条检查；补齐后删除。
- 事故复盘产生的债同样入账，并在复盘文档里引用编号。

## 字段

| 字段 | 要求 |
|---|---|
| 编号 | `D-001` 形式，递增 |
| 日期 | 记账当天，`YYYY-MM-DD` |
| 债务 | 一句话说清欠了什么，带文件路径或模块名 |
| 原因 | 为什么当时欠下（排期、依赖未就绪、故障止血） |
| 影响 | 不还会怎样：哪条路径会坏、谁会踩到 |
| 归还期限 | 日期或版本号 |
| 状态 | 未还 / 部分还 / 已还 |

## 台账

| 编号 | 日期 | 债务 | 原因 | 影响 | 归还期限 | 状态 |
|---|---|---|---|---|---|---|
| D-001 | 2026-10-24 | 执行器在核心进程同步跑完整运行（`core/worker/executor.py`），长运行占住 supervisor 的 tick | 本轮隔离先落在插件调用层（ADR 0010），阶段编排留在核心 | API/SSE 接入后，一次长运行期间接口会卡住 | P3 客户端与远程前 | 未还 |
| D-002 | 2026-10-27 | `core/supervisor.py` 540 行，连续两次超 docs/11 模块上限 500 | 环境动作端点在既有 509 行基础上加 31 行；拆分属独立重构，不混入本功能卡 | 改动热点继续膨胀，tick 主循环可读性与可测性下降 | 下一张内核重构卡 | 已还（2026-10-27 归还：supervisor.py 216 行；新增 supervisor_disk 103 行、supervisor_environments 167 行、supervisor_leases 126 行、supervisor_recovery 129 行） |
| D-003 | 2026-10-27 | `tests/unit/test_hostops_doctor.py` 的 `test_check_lock_and_mutex` 与 `test_run_doctor_prints_each_check_and_summary` 依赖本机当刻状态（全局命名互斥量与心跳、计划任务等检查项） | 断言绑定开发机稳定态；故障注入用例会持有 60 秒全局互斥量 | 并行跑 gate 或机器状态变化时误报红，需人工复跑甄别 | 下一张测试整备卡 | 未还 |
| D-004 | 2026-10-28 | 进程被强杀或断电时不执行 `NotifyService.close`，`notifications` 表里已入账的 `pending` 行没有启动清扫（`core/notify/service.py`） | 停机收尾只覆盖正常退出路径 | 账目历史出现永久中间态，设置页把 `pending` 当投递历史时显示为一直在投递 | 下一张通知卡 | 未还 |
| D-005 | 2026-10-28 | 渠道实现违反 `Notifier` 契约抛出异常时，投递工作线程终止，该条通知的 `pending` 行无终态（`core/notify/delivery.py`） | 契约约定渠道以 `DeliveryResult` 表达失败，投递循环未做异常兜底 | 一处渠道实现缺陷会使整个通知线程停摆，之后的通知全部不再投递 | 下一张通知卡 | 未还 |
| D-006 | 2026-10-28 | 环境状态机的两个写者各持连接：tick 的保温关闭走内核连接，手动动作走请求线程连接，`core/environments/service.py` 的读与写不在同一事务（71、102、217、239 行），动作实例锁只串行动作自身 | 请求线程不能复用内核连接（sqlite3 线程检查），改自开连接是当期最小修法 | 并发「保温关闭 + 手动 prepare」可能撞状态机或丢更新，出现状态与 `last_probe_at` 不一致 | 下一张内核并发卡 | 未还 |
| D-007 | 2026-10-28 | tick 抛出的非 IO 异常在 `core/hostops/serve.py` 里没有结构化日志：`_run_core` 只捕 `OSError`/`sqlite3.Error`/`MigrationError`，其它异常经 `finally` 收尾后带 traceback 退出，不写 `serve.run_failed` | 错误契约未定义「程序错误」在 serve 顶层的记账口径 | 核心因程序错误退出时日志台账缺一条可检索的失败事件，排障只能靠 stderr | 下一张宿主卡 | 未还 |
| D-008 | 2026-10-28 | 租约续租只在 tick 里发生（`core/supervisor_leases.py:61-74`），而 tick 同步跑执行器（`core/worker/executor.py`，见 D-001），长运行期间不会续租 | 租约管理挂在 tick 节律上，没有独立续租线程 | TTL 默认 10 分钟，超过 10 分钟的运行租约会被自己回收，与 docs/09「每 60 秒续租、不依赖 worker 存活」冲突 | 与 D-001 同一张执行器异步化卡 | 未还 |
| D-009 | 2026-10-28 | 插件加载不校验 manifest：worker 只 import `plugin.py` 并检查有没有 `PLUGIN`（`core/plugins/worker.py`），不校验 `api_version`、`requires.contract`、`capabilities` 与配置 schema | docs/05 的加载校验尚未接进 worker 启动路径 | 版本不兼容或契约不满足的插件会被当成可用加载，故障推迟到运行期以 `E_PLUGIN` 出现 | MAA 插件接入前 | 未还 |
| D-010 | 2026-10-28 | 清理失败与清理异常路径会二次 cleanup：`core/worker/executor.py:321` 已调用 provider 清理，随后 `_fail`/`_cancel_run` 再走 `_safe_cleanup`（496-501、505-513、553-561） | 取消路径已保证只清一次，其余路径未收口 | provider 的 cleanup 不是纯操作时（关进程、断 adb）会被重复执行，可能误杀复用的环境 | 下一张执行器卡 | 未还 |
