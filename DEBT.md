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
| D-013 | 2026-10-30 | docs/11 的行数与参数上限被突破：`core/worker/executor.py` 748 行、`WorkerExecutor` 类 633 行、`execute` 61 行、`_run_script` 62 行；`core/hostops/serve.py` 669 行、`_run_core` 88 行；`core/supervisor.py` 的 `Supervisor` 类 376 行、`__init__` 55 行；`Supervisor.__init__`、`WorkerExecutor.__init__`、`ServeLoop.__init__`、`_start_api`、`_bind_api` 参数 5–8 个 | 停机收尾与执行器异步化在既有超限上继续加注入点；拆分与参数收敛属独立重构，不混入功能卡 | 执行器与 serve 是改动热点，类与函数继续变长会让评审与单测难以覆盖；参数表变宽后调用点易错 | 下一张执行器 / serve 拆分卡 | 部分还（2026-11-15 归还 serve 五项：`core/hostops/serve.py` 模块行数、`_run_core` 行数、`ServeLoop.__init__`/`_start_api`/`_bind_api` 参数由卡76-SV 达标并合并为 `6b851f6`；归还 supervisor 三项：`Supervisor` 类行数、`__init__` 行数与参数数由卡78-SP 达标并合并为 `2ac6dda`；允许清单条目按卡面保留，待卡82 终局收缩统一删除；executor 项见卡75） |
| D-018 | 2026-11-02 | docs/11「结构与上限」的既有超限打包 11 项：7 个构造类函数参数 5–7 个（`core/api/app.py::ApiApp.__init__` 7、`core/notify/service.py::NotifyService.__init__` 7、`core/supervisor_environments.py::EnvironmentSupervision.__init__` 6、`core/hostops/environment_actions.py::EnvironmentActionService.__init__` 5、`core/hostops/retention.py::RetentionRunner.__init__` 5、`core/plugins/worker.py::_Reader.__init__` 5、`core/supervisor_leases.py::LeaseSupervision.__init__` 5）、`core/events/evidence.py::EvidenceService.record` 5 参、`core/hostops/upgrade_state.py::record_failed` 5 参、`storage/repositories/resources.py::ResourceRepository.ensure` 5 参、`storage/db/backup.py` 模块 513 行 | 卡61 把结构上限做成机械检查时按现状入允许清单，先让检查入闸；参数收敛与模块拆分属独立重构，不混入质量卡 | 构造与记录类函数参数表偏宽，调用点易错；`storage/db/backup.py` 继续变长后难评审 | 下一张参数收敛卡 / storage 重构卡 | 已还（2026-11-08 归还：`storage/db/backup.py` 按「快照生成与校验 / 导出与修剪」拆开，拆为 `backup.py` 313 行与 `backup_retention.py` 136 行，清单条目随 c2ee3f4 删除。2026-11-14 归还：十个构造与记录函数参数全部收敛到 ≤4，同源参数合成 frozen dataclass，行为不变；`_D018_ENTRIES` 清空） |
| D-019 | 2026-11-13 | 结构上限的机械检查只扫 `core/`、`storage/`、`contracts/`；`tests/` 38 处与 `scripts/` 3 处超限未入检查，按 docs/11 的规模上限尚未机械判定 | 检查随卡61 入闸时按生产三包定范围；测试与工具脚本的既有超限不在同一轮清 | 测试文件与工具脚本继续变长后只能靠评审发现，未入检查的部分不会挡住新增超限 | 下一张测试拆分卡 | 已还（2026-11-14 归还：扫描范围扩到 `tests/` 与 `scripts/`，五处根全扫；41 处既有超限按文件加名字逐条入 `tests/unit/test_structure_limits.py` 的 `_D019_ENTRIES`，实测值取本分支扫描输出，检查器自身超限同记一条；逐项拆分由 TASKS.md「工程治理」的收缩条目承接） |
| D-020 | 2026-11-14 | `python -m pytest tests/integration -q` 整目录跑时 `tests/integration/test_end_to_end_run.py::test_due_schedule_runs_to_completion_and_results_are_queryable_over_http` 稳定失败（SSE `recv` 超时，`test_end_to_end_run.py:127`），同一用例单跑通过；干净 `main = f7d5975` 的临时 worktree 上同样复现 | 套件内互扰机制未定：serve 家族、heartbeat 与 SSE 客户端在同一 pytest 进程里的残留状态（端口、互斥量、后台线程、子进程）未被隔离；卡69 的进程级作用域隔离仍不能消除 | 全量门禁的 pytest 轴无法稳定全绿，任何分支都拿不到合并证据：2026-11-14 夜里多张卡的门禁唯一失败就是这一条 | 卡71-SE（套件隔离修复，进行中） | 已还（2026-11-14 归还：卡71-SE `93928cd`，合并 `9cfcbc3`。根因＝调度 2 GiB 磁盘守卫读的是宿主 C 盘真实余量（本机 1.8–2.1 GiB 贴线），e2e 未注入 `disk_free`，整跑时前缀用例先写临时库把余量压到线下，建运行被静默拒绝、事件流沉默 30 秒致探针超时；卡69 的端口与互斥量隔离无效，因为共享的是磁盘不是进程状态。修法＝e2e 注入 `STABLE_DISK_FREE`（100 GiB，WARN 线之上），口径写进 docs/12 集成测试一节） |
| D-021 | 2026-11-14 | 宿主 C 盘 100 GB 只剩 1.76–2.1 GiB（1.8%），贴着 2 GiB 建运行守护线：serve 起来后每个计划的建运行都被静默拒绝（每轮 `supervisor.schedule_disk_guarded` 只记 DEBUG，仅首次一条 `host.disk_full_guard` 边沿事件），平台在这台机器上实际不调度 | 数据根可落在任意卷，但打包形态原本靠 exe 位置推断；系统盘余量被系统、临时目录与工具链挤占，清理属环境动作 | 本机做真机或集成验证时「计划不触发」容易被误判成代码缺陷；门禁与用例随宿主余量漂移 | 宿主清理 / 部署搬家 | 部分还（2026-11-14 归还机制：`resolve_data_root` 支持 `LAP_BACKSTOP_DATA_ROOT` 覆盖、只收绝对路径，计划任务两种形态都把 `--data-root` 写进动作，docs/08「路径布局」写明数据根必须在非系统盘，本机定 `D:\lap-backstop\data`。未还：C: 余量本身与 pytest 临时根仍在系统盘，需清出 ≥ 20 GiB 或把 `TEMP`/`TMP` 指到 D: 后再做真机验证） |
