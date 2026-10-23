# 任务清单

只记还没做的事。做完的进提交说明或交接账本。

## P0：契约与内核

- [ ] 契约单一真源校验：docs/05 与 contracts/ 一份定义，02/03/09 只引用。
- [ ] pre-commit 钩子跑 `scripts/gate.py`，钩子失败不允许提交。
- [ ] core/events：事件快照与回放。
- [ ] worker 子进程与 RPC：插件隔离、调用超时强杀、崩溃重启。
- [ ] 数据库备份与恢复：快照、导出、迁移前快照。
- [ ] 场景测试：崩溃、租约、取消、证据缺失。
- [ ] 看门狗、单实例、doctor 自检、磁盘水位。

验收：fake 插件在虚拟时钟下跑通 30 天调度；崩溃恢复后状态一致；场景测试全绿。

## P0 修复：质量审查发现（2026-10-24）

- [ ] 结论校验：`VerdictService` 改用 `EvidenceRepository.known_ids`；核对模板 `required_evidence` 与 `checks[].evidence_ids`；`policy_id` 回填模板 id
  验收：缺证据、缺证据种类、引用不存在证据各一条回归测试。
- [ ] 监督接线：启动扫描非终态；调度每秒 tick；逾期扫描；保温到期关闭；租约 60 秒续租、过期转 recovering；`keep_warm_minutes` 由 schedule 传入执行器；请求取消接入执行器（graceful→force）
  验收：租约过期、保温到期、执行中取消各一条场景测试。
- [ ] 口径裁决（先问，再改）：重复迁移 E_CONFLICT 与 docs/02:131 幂等二选一；`run.state.changed` 载荷 from/to 与 from_state/to_state 二选一；提交 400 行是否含测试
- [ ] 规范卫生：`JsonLineLogger` 补 at/event 字段；默认值集中 `config/defaults.toml`；`Capability`/`PhaseName` 用 StrEnum；`core/worker`、`core/clock`、`core/logging` 补 ADR；`_optional_str`/`placeholders` 收敛；`gate.py` 加超时；fake 插件公开方法补 docstring；后续提交 ≤400 行
  验收：六条门禁全绿，ADR 落盘。
- [ ] 测试改进：状态机用例由 docs/02 迁移表生成，避免测试与文档各抄一份（docs/12:38）

## P1：MAA 插件与方舟环境

- [ ] 冷启动采样：开机到模拟器可用、到游戏主界面，至少 10 次，记录 P50/P95。
- [ ] 环境 provider：MuMu 启动/复用、游戏拉起、登录、主界面判定。
- [ ] 就绪状态机：进程/窗口/adb/模板/稳定帧；未知画面记录。
- [ ] 预热与保温：preheat_minutes、keep_warm_minutes、空闲关闭。
- [ ] 门控：环境 ready 才放行 MAA；开始唤醒失败急停。
- [ ] 脚本 adapter：MAA GUI 或 Python 绑定；日志/回调采集。
- [ ] verifier：期望任务集、完成集、理智数值、结束画面异常。
- [ ] recovery：重连、重启游戏、重启模拟器、停手通知。
- [ ] 证据与事件：截图、日志片段、任务集、Verdict。
- [ ] 7 天无人值守浸泡，记录失败分类与修复。

验收：连续 7 天按调度完成，失败可恢复或通知；每天人工干预 0 次。

## P2：MAAEnd 插件与终末地环境

- [ ] 确认 MAAEnd 的启动方式、日志、完成信号、配置结构。
- [ ] 环境 provider：终末地客户端/模拟器。
- [ ] 脚本 adapter：启动、观察、停止、恢复。
- [ ] verifier：任务集或等价完成证据。
- [ ] 用第二个插件回归内核契约；修正抽象后再冻结 API v1。

验收：同一套内核、同一套调度/资源/事件，不改内核即可托管 MAAEnd。

## P3：客户端与远程

- [ ] Web 最小页：总览、运行详情、叫停、环境状态。
- [ ] schema 驱动表单：调度、作业模板、插件配置。
- [ ] Tailscale + 单 token；只监听本机与 tailnet。
- [ ] 通知渠道：ServerChan、钉钉、ntfy；投递重试。
- [ ] APK 三屏：总览、运行详情、设置；共享 API。

验收：手机在外可看状态、叫停、改时间；断线重连可用。

## P4：扩展

- [ ] AI Advisor：未知画面分诊、失败归因、公告解读、识别辅助、自然语言编排。
- [ ] 多机 Runner：远程执行、设备身份、密钥管理。
- [ ] 工作流插件：用户可编排的步骤流。
- [ ] 第二款游戏环境接入。
- [ ] 跳过当天：一次发生点跳过不产生运行，记事件，界面提供入口。

## 技术验证

- [ ] MAA 失败分类：开始唤醒失败、掉线、任务出错、更新/维护，各占多少。
- [ ] MAAEnd 的日志与完成信号样本。
- [ ] 自动登录、计划任务唤醒、BIOS 来电自启的可行性。
- [ ] MuMu 原生截图 DLL 的稳定性与失效条件。
- [ ] 磁盘与日志增长速度，30 天保留是否可行。
- [ ] Tailscale 在手机上的断线/耗电表现。
