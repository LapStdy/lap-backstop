# ADR 0010 · worker 子进程与能力调用 RPC

- 状态：accepted
- 日期：2026-10-24

## 背景

插件代码会挂起、崩溃、写坏标准输出，也会被结束；内核不能因此丢状态或停止服务。[05](05-插件与适配器.md) 要求插件不进入内核进程、内核与 worker 只交换 JSON 消息；[ADR 0007](0007-worker执行器与阶段边界.md) 把一次运行拆成四阶段并约定换通道时不改阶段方法与 `RunScope`。执行器与状态机已经在核心进程跑通，缺的是把插件调用移出内核进程。

## 决策

1. 进程布局：阶段编排留在 `core/worker/executor.py`；插件在 `core/plugins/worker.py` 启动的子进程里运行，按插件拉起、按需常驻，内核进程不导入插件模块。加载只发生在 worker，入口是 `plugins/<id>/plugin.py` 导出的 `PLUGIN`。
2. 协议：stdin/stdout 上行分隔 UTF-8 JSON，一行一条消息。核心→worker 用 `call`、`cancel`、`callback_result`、`shutdown`；worker→核心用 `ready`、`result`、`callback`、`notify`、`fault`。协议独占 stdout，插件打印被重定向到 stderr。
3. 上下文：`RunContext` 的可序列化字段（`run_id`、`attempt`、`template_id`、`plugin_id`、`environment_id`、`config`、`work_dir`、`evidence_dir`、`deadline_at`、`secrets`）随 `call` 下发；`cancel`、`clock`、`log`、`emit`、`record_evidence` 在 worker 侧重建。`emit` 与 `record_evidence` 是同步回调，内核在等待调用结果时处理并把事件、证据写入数据库；插件拿不到数据库句柄。
4. 校验：内核不信任 worker 返回的 JSON。解码按 `contracts` 的类型逐字段校验，非法载荷、未知枚举、naive 时刻都按 `WorkerPayloadError` 失败，不进入领域对象。
5. 超时与强杀：每次调用的上限来自 `config/defaults.toml` 的 `worker.call_timeout_seconds`，等待与强杀用真实单调时间；到点先结束进程再按 `E_DEADLINE` 收尾。虚拟时钟不跨进程（[08](08-宿主与运维.md)），调度与业务截止仍由核心按注入的 `Clock` 判定。
6. 取消：核心在等待调用结果时轮询 `ctx.cancel`；置位后发 `cancel`，等 `worker.cancel_grace_seconds`，仍未返回则强杀进程；运行按 `cancelled` 收尾。
7. 崩溃与降级：worker 退出或写坏协议 → `WorkerCrashError`，执行器按 `E_PLUGIN_CRASH` 进入恢复；同一插件连续 `worker.max_restarts` 次失败后 channel 标记 `degraded`，后续调用直接失败，直到内核重启。
8. 覆盖范围：environment、script、verifier 三种能力走 RPC；config_projection、notifier、advisor 复用同一协议，在接入对应路由时补调用表与编码。

## 后果

- 插件崩溃、挂起、写 stdout 不再威胁内核；阶段、事件与恢复仍由执行器控制。
- 每次运行多一次进程启动；常驻池与并发 worker 在接入多运行并发时补。
- 执行器仍在核心进程同步跑一次运行，长运行会占住 supervisor 的 tick；把整次运行移入 worker 进程属于后续演进。
- degraded 标记由 channel 暴露，调度侧跳过该插件在接入多插件路由时生效。
- 进程等待是真实时间，虚拟时钟只覆盖核心逻辑；子进程相关测试用短的真实超时。

## 替代方案

| 方案 | 不采纳的原因 |
|---|---|
| 插件留在内核进程内调用 | 挂起或崩溃会拖垮核心，插件写 stdout 也会污染内核输出。 |
| worker 独占数据库连接、整次运行在 worker 内执行 | 阶段与事件写入要和调度、租约同源；两个写者会破坏单一写入者约定（[11](11-代码规约.md)）。 |
| pickle、gRPC 等二进制协议 | 增加依赖与版本耦合；行分隔 JSON 便于抓包、回放与逐字段校验。 |
| 每个阶段新起进程 | 脚本句柄（如脚本 pid）要跨阶段存活，进程按运行存活才成立。 |
