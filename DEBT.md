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
| D-002 | 2026-10-27 | `core/supervisor.py` 540 行，连续两次超 docs/11 模块上限 500 | 环境动作端点在既有 509 行基础上加 31 行；拆分属独立重构，不混入本功能卡 | 改动热点继续膨胀，tick 主循环可读性与可测性下降 | 下一张内核重构卡 | 已还（2026-10-27 归还：supervisor.py 216 行；新增 supervisor_disk 103 行、supervisor_environments 167 行、supervisor_leases 126 行、supervisor_recovery 129 行） |
| D-005 | 2026-10-28 | 渠道实现违反 `Notifier` 契约抛出异常时，投递工作线程终止，该条通知的 `pending` 行无终态（`core/notify/delivery.py`） | 契约约定渠道以 `DeliveryResult` 表达失败，投递循环未做异常兜底 | 一处渠道实现缺陷会使整个通知线程停摆，之后的通知全部不再投递 | 下一张通知卡 | 已还（2026-10-31 归还：渠道抛异常折成一次失败尝试并照常退避重试；工作线程再兜一层，任何意外都落失败终态，线程继续服务后续通知） |
| D-007 | 2026-10-28 | tick 抛出的非 IO 异常在 `core/hostops/serve.py` 里没有结构化日志：`_run_core` 只捕 `OSError`/`sqlite3.Error`/`MigrationError`，其它异常经 `finally` 收尾后带 traceback 退出，不写 `serve.run_failed` | 错误契约未定义「程序错误」在 serve 顶层的记账口径 | 核心因程序错误退出时日志台账缺一条可检索的失败事件，排障只能靠 stderr | 下一张宿主卡 | 已还（2026-10-31 归还：`_run_core` 增非 IO 异常分支，写 `serve.run_failed` 带 `error` 与 `error_type` 后再上抛，堆栈仍留在 stderr） |
| D-013 | 2026-10-30 | docs/11 的行数与参数上限被突破：`core/worker/executor.py` 748 行、`WorkerExecutor` 类 633 行、`execute` 61 行、`_run_script` 62 行；`core/hostops/serve.py` 669 行、`_run_core` 88 行；`core/supervisor.py` 的 `Supervisor` 类 376 行、`__init__` 55 行；`Supervisor.__init__`、`WorkerExecutor.__init__`、`ServeLoop.__init__`、`_start_api`、`_bind_api` 参数 5–8 个 | 停机收尾与执行器异步化在既有超限上继续加注入点；拆分与参数收敛属独立重构，不混入功能卡 | 执行器与 serve 是改动热点，类与函数继续变长会让评审与单测难以覆盖；参数表变宽后调用点易错 | 下一张执行器 / serve 拆分卡 | 未还（结构上限机械检查已入闸：这些项在 `tests/unit/test_structure_limits.py` 的允许清单里按 D-013 记账） |
| D-018 | 2026-11-02 | docs/11「结构与上限」的既有超限打包 11 项：7 个构造类函数参数 5–7 个（`core/api/app.py::ApiApp.__init__` 7、`core/notify/service.py::NotifyService.__init__` 7、`core/supervisor_environments.py::EnvironmentSupervision.__init__` 6、`core/hostops/environment_actions.py::EnvironmentActionService.__init__` 5、`core/hostops/retention.py::RetentionRunner.__init__` 5、`core/plugins/worker.py::_Reader.__init__` 5、`core/supervisor_leases.py::LeaseSupervision.__init__` 5）、`core/events/evidence.py::EvidenceService.record` 5 参、`core/hostops/upgrade_state.py::record_failed` 5 参、`storage/repositories/resources.py::ResourceRepository.ensure` 5 参、`storage/db/backup.py` 模块 513 行 | 卡61 把结构上限做成机械检查时按现状入允许清单，先让检查入闸；参数收敛与模块拆分属独立重构，不混入质量卡 | 构造与记录类函数参数表偏宽，调用点易错；`storage/db/backup.py` 继续变长后难评审 | 下一张参数收敛卡 / storage 重构卡 | 未还（允许清单见 `tests/unit/test_structure_limits.py`，匹配按文件+名字，清单只缩不涨） |
| D-019 | 2026-11-02 | 两条时序敏感用例会把门禁间歇染红：`tests/integration/test_serve_shell.py::test_shutdown_writes_single_interrupt_at_phase_boundary` 断言未来运行恒为 `queued`（异步派发下偶发已到 `waiting`）；`tests/integration/test_api_export.py::test_export_twice_returns_valid_zips` 两次导出 zip 逐字节比对（导出时刻跨秒即不等） | 断言写成确定状态与字节相等，单例复跑通过、全量高负载下偶发红 | 全量门禁间歇红，掩盖真实缺陷，假红判据要人工复核 | 下一张测试稳定性卡 | 未还（2026-11-02 值守夜两条各出现一次：单例复跑 3/3 通过、整包复跑七条全绿） |
