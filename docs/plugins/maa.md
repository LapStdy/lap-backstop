# MAA 插件

[返回总览](../00-总览与索引.md)

本文件回答：MAA 插件声明什么能力、如何准备方舟环境、如何驱动与观察 MAA、如何验证结果、如何读写配置、失败如何恢复。

## manifest

```json
{
  "id": "maa",
  "name": "MAA · 明日方舟",
  "version": "1.0.0",
  "api_version": 1,
  "description": "驱动 MAA 完成明日方舟日常并验证结果",
  "capabilities": ["environment", "script", "verifier", "config_projection"],
  "config_schema": "config.schema.json",
  "requires": {"contract": ">=1 <2", "platform": "windows", "python": ">=3.12"}
}
```

| 能力 | 实现内容 |
|---|---|
| `environment` | MuMu 12 + 明日方舟的探测、准备、复位、保温与清理 |
| `script` | MAA 的启动、观察、停止与恢复 |
| `verifier` | 脚本证据与画面证据的合成，三种验证口径 |
| `config_projection` | `gui.new.json` 的差异比较、写入、备份与回滚 |

## 环境

| 项 | 路径 / 参数 |
|---|---|
| 模拟器 | `H:/MuMuPlayer`，实例 VM0，Android 15 |
| adb | `127.0.0.1:16384` |
| 分辨率 | 1280×720，Windows 显示缩放 100% |
| MAA 程序 | `H:/MAA/MAA.exe` |
| MAA 配置 | `H:/MAA/config/gui.new.json` |
| GUI 日志 | `H:/MAA/debug/gui.log` |
| asst 日志 | `H:/MAA/debug/asst.log` |
| MAA 资源 | `H:/MAA/resource` |
| Python 绑定 | `H:/MAA/Python/asst` |

探测 `probe` 同时确认三件事：模拟器窗口存在、adb 可连接且可截图、游戏在前台。任一不满足时返回对应状态：未启动为 `off`，启动中为 `booting`，adb 失联或游戏未就绪为 `degraded`，分辨率或缩放不符为 `failed`。

准备 `prepare` 的步骤与判据：

| 顺序 | 步骤 | 判据 | 超时 |
|---|---|---|---|
| 1 | 复用或启动 MuMu | 窗口存在且 `adb connect` 成功 | 180 秒 |
| 2 | 校验分辨率与缩放 | 1280×720 且缩放 100% | 10 秒 |
| 3 | 拉起游戏 | 游戏进程存在 | 60 秒 |
| 4 | 等登录界面 | 登录界面或主界面模板命中 | 300 秒 |
| 5 | 点登录按钮 | 固定安全坐标点击后画面变化 | 30 秒 |
| 6 | 等主界面 | 任一主题模板命中 | 300 秒 |
| 7 | 启动 MAA 并加载资源（`binding`） | `load` 与 `connect` 成功 | 180 秒 |

- 步骤 4 命中主界面时跳过步骤 5。
- 步骤 7 在预热窗口内执行；没有预热窗口时，等待计入准备阶段上限。
- `gui` 模式不做预启动；MAA 启动与资源加载在 `launch` 时发生，计入执行阶段。
- `reset` 先 `adb disconnect` 再 `adb connect`，关闭游戏，重走步骤 3 到 6；复位后环境状态回到 `booting`。
- `warm` 在 `keep_warm_minutes` 内保持 MuMu 与游戏就绪；`binding` 模式同时保持 asst 实例常驻，省掉一次资源加载。
- `cleanup(keep_warm)`：保温未过期时不关闭游戏与模拟器；`keep_warm=false` 时关闭。
- 登录按钮只用固定安全坐标，点击前做区域钳制。

MAA 启动前剔除 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`，否则上报请求会走死代理。MAA 以工作目录 `H:/MAA` 启动，不带命令行参数。

## 脚本

| 模式 | 启动方式 | 完成信号 | 适用场景 |
|---|---|---|---|
| `gui` | 运行 `MAA.exe`，`RunDirectly=true` 时自动开跑 | `gui.log` 的 `任务已全部完成！` | 与现有配置、日志、PostActions 行为一致 |
| `binding` | 官方 Python 绑定：`load` / `connect` / `append_task` / `start` / `stop` / `running` / `get_image` | 绑定回调的 JSON 与 `running` 状态 | 需要更细的节点信号与主动截图 |

`launch` 只触发任务队列：`binding` 模式复用常驻 asst 实例并执行 `append_task` 与 `start`；`gui` 模式每次启动新的 MAA 进程，`RunDirectly=true` 时资源加载完成后自动开跑。需要把冷启动移出关键路径时用 `binding` 模式。

`observe` 只读，不改变脚本状态。每次观察返回进程状态、日志游标与最近信号：

| 日志 | 信号 | 含义 |
|---|---|---|
| `gui.log` | `正在连接模拟器……` | 连接开始 |
| `gui.log` | `开始任务: X` / `完成任务: X` / `任务出错: X` | 单个任务进度 |
| `gui.log` | `任务已全部完成！` | 整条队列完成的唯一脚本侧依据 |
| `gui.log` | `游戏掉线，停止任务` | 游戏连接中断 |
| `gui.log` | `Post actions: …` | 收尾动作 |
| `asst.log` | `AllTasksCompleted` | 单条任务链结束，不能当作整队列完成 |
| `asst.log` | `TaskChainStart` / `TaskChainCompleted` / `TaskChainError` | 任务链粒度信号 |
| `asst.log` | `SubTaskExtraInfo` 的 `SanityBeforeStage` | 理智采样：`current_sanity` 与 `max_sanity` |

运行期间每 `sample_interval_s` 秒抽一帧，作为卡住判定与画面证据；默认 30 秒。

`stop` 先请求停止，10 秒内确认进程退出；超时由内核终止进程。`recover` 接收内核动作 `restart_script`、`reset_environment`、`abort`；`restart_script` 之下再分重新附着常驻实例、重启脚本、重启游戏三档，`abort` 之下停止脚本并通知。

## 验证

验证策略属于 MAA 插件，由 JobTemplate 的 `VerificationPolicy` 保存完整结构，`params.mode` 取 `script`、`screen`、`dual`，默认 `dual`。策略在作业开始时冻结，运行中不改。

| 证据来源 | 具体信号 | `Evidence.kind` |
|---|---|---|
| 脚本 | 完成行、错误行、掉线行、进程退出状态、任务链信号 | `log` / `exit` / `probe` |
| 画面 | 结束截图、主界面模板命中、异常画面模板命中 | `screenshot` |
| 理智 | 运行前后的 `current_sanity` 采样 | `probe` |

| 政策 | `completed` | `failed` | `unknown` |
|---|---|---|---|
| `script` | 出现完成行，无错误行，进程正常退出 | 出现任务出错或掉线行 | 完成行缺失、日志被截断 |
| `screen` | 结束画面为主界面，无异常画面 | 命中错误框、掉线、维护或更新画面 | 截图缺失、画面无法归类 |
| `dual` | 脚本与画面都指向完成 | 任一来源明确失败 | 任一来源缺失或两者矛盾 |

画面核对使用结束截图，判据是：主界面任一主题模板命中，且没有错误框、掉线、维护、更新或未知画面。截图不可读或模板连续失配时记证据缺失，`screen` 与 `dual` 记 `unknown`，不降级为完成。

理智核对用于刷理智任务：运行后 `current_sanity` 低于运行前记为通过；未采到样本记证据缺失；数值未下降时记 `unknown`，交人工核对。

## 配置投影

`gui.new.json` 中与本插件相关的部分：

| 字段 | 用途 |
|---|---|
| `Configurations` | 多套配置；`Current` 指定当前使用的一套 |
| `TaskQueue` | 任务列表，顺序即执行顺序；任务用 `$type` 区分类型 |
| `Gui.PostActions` | 完成后动作，位标志写成逗号分隔字符串 |

任务项只需要 `$type`、`Name`、`IsEnable`，其余字段由 MAA 按默认值补全。列表字段（`StagePlan`、`RoomList`）必须写 JSON 数组，类型写错会导致整份配置解析失败。

写入规则：

- MAA 运行时会用内存配置覆盖文件，写入前必须先退出 MAA；检测到 MAA 在跑时拒绝写入并返回 `E_CONFIG_LOCKED`（`detail=maa.config_locked`）。
- 每次写入前备份为 `gui.new.json.<时间>.bak`，保留最近 `backup_keep` 份。
- 写入后读回校验；不一致则回滚到备份。
- `Gui.PostActions` 由平台接管，固定写 `ExitSelf`；发现外部写成 `ExitArknights` 或 `ExitEmulator` 时按程序值重写并记冲突。
- 普通字段的差异默认以脚本侧为主，导入平台；配置项 `config_projection.strategy` 可取 `script_first` 或 `platform_first`。

`diff` 返回逐字段差异，`apply` 返回备份路径与写入后的配置哈希，`rollback` 按修订号取回备份。日常副本与测试副本不能同时运行，它们连同一个模拟器与同一个账号。

## 参数 schema

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `maa_dir` | string | `H:/MAA` | MAA 目录 |
| `mode` | enum | `gui` | `gui` 或 `binding` |
| `mumu_dir` | string | `H:/MuMuPlayer` | MuMu 安装目录 |
| `adb_address` | string | `127.0.0.1:16384` | 模拟器 adb 地址 |
| `profile` | string | `Current` 指向的配置名 | 使用的 MAA 配置 |
| `tasks` | array | 日常队列 | 每项含 `type`、`name`、`enabled`、`params` |
| `launch_timeout_s` | integer | 180 | 启动到资源加载完成的上限 |
| `wake_timeout_s` | integer | 90 | 开始唤醒到主界面的上限 |
| `startup_timeout_s` | integer | 300 | 游戏启动到登录界面的上限 |
| `sample_interval_s` | integer | 30 | 运行期抽帧间隔 |
| `keep_warm_minutes` | integer | 30 | 运行结束后保持环境就绪的时长 |
| `config_projection.enabled` | boolean | true | 是否在开跑前对账并写入 |
| `config_projection.backup_keep` | integer | 20 | 备份保留份数 |

三档验证口径写在 JobTemplate 的 `VerificationPolicy.params.mode`，不在插件配置里。

## 错误映射

| 观测 | 错误码与 `detail` | 默认恢复 |
|---|---|---|
| `gui.log` 出现 `任务出错: X` | `E_SCRIPT_TASK_FAILED` / `maa.task_failed` | 重启 MAA，整条队列重跑一次 |
| `gui.log` 出现 `游戏掉线，停止任务` | `E_ENV_DISCONNECTED` / `maa.disconnected` | 复位环境后重试一次 |
| 开始唤醒超时（实测 66 秒） | `E_ENV_WAKE_TIMEOUT` / `maa.wake_timeout` | 重启游戏后重试；连续 2 次停手 |
| MAA 启动到资源加载超时 | `E_SCRIPT_RESOURCE_TIMEOUT` / `maa.resource_timeout` | 停手并通知，不重复冷启动 |
| MAA 进程退出且无完成行 | `E_SCRIPT_EXITED` / `maa.exited` | 重新启动并重跑一次 |
| `adb connect` 失败 | `E_ENV_DISCONNECTED` / `maa.adb_unreachable` | 复位 adb 后重试 |
| 写入配置时 MAA 在运行 | `E_CONFIG_LOCKED` / `maa.config_locked` | 等待 30 秒，仍在运行则停止 MAA 后重试 |
| PostActions 被外部改成退出游戏或退出模拟器 | `E_CONFIG_CONFLICT` / `maa.post_actions_conflict` | 重写为 `ExitSelf`，记冲突，继续运行 |
| 结束画面无法归类 | `E_VERIFY_UNKNOWN_SCREEN` / `maa.unknown_screen` | 先请 Advisor 分诊；不可用时复位后重试一次 |
| 截图或日志证据缺失 | `E_VERIFY_NO_EVIDENCE` / `maa.no_evidence` | 记 `unknown`，不重试 |
| 分辨率或显示缩放不符 | `E_ENV_RESOLUTION` / `maa.resolution_mismatch` | 停手并通知，不自动改系统设置 |

## 恢复动作

| 内核动作 | MAA 内部动作 | 触发 | 行为 | 上限 |
|---|---|---|---|---|
| `restart_script` | 重启脚本 | 任务出错、进程消失 | 重启 MAA 并重跑队列 | 1 次/attempt |
| `restart_script` | 重启游戏 | 唤醒超时、界面卡死 | 关闭并拉起游戏，等到主界面 | 2 次/attempt |
| `restart_script` | 重新附着 | MAA 仍在运行、绑定会话丢失 | 重新附着常驻实例，不重启 | 不限 |
| `reset_environment` | 复位环境 | 掉线、adb 失联 | 断开并重连 adb，按需重启模拟器 | 1 次/attempt |
| `abort` | 停手并通知 | 预算用尽、分辨率不符、连续掉线 | 停止脚本，发通知并附截图与原因 | 终态 |

内核枚举只有 `retry`、`reset_environment`、`restart_script`、`abort` 四种动作；重启游戏、重新附着、停手并通知是插件在 `restart_script` 与 `abort` 之下的分解，不进入内核枚举。恢复动作由内核按恢复策略路由，插件只执行动作并返回结果；动作本身不产生 `Verdict`。

## 实测数据

单次冷启动时间线（2026-10-20）：

| 时刻 | 事件 | 间隔 |
|---|---|---|
| 03:05:52 | MAA 启动 | — |
| 03:07:04 | 资源加载完成 | 72 秒 |
| 03:07:14 | 连接模拟器 | 10 秒 |
| 03:07:20 | 开始唤醒 | 6 秒 |
| 03:08:26 | 开始唤醒出错 | 66 秒 |
| 03:10:09 | 9 个任务全部失败 | 整轮 2 分 49 秒 |

操作耗时：

| 动作 | 耗时 |
|---|---|
| 模拟器与游戏冷启动 | 约 55 秒 |
| `adb connect` | 32 毫秒 |
| adb 点击 | 55 毫秒 |
| adb 截图（1280×720 PNG） | 约 600 毫秒 |
| MuMu 原生截图 | 10 到 150 毫秒 |
| ROI 模板匹配 | 约 7 毫秒 |
| 全屏单模板匹配 | 100 到 190 毫秒 |
| 全屏五模板单帧 | 775 毫秒，超出 200 毫秒预算 |

29 天统计：开跑 60 次，完成 44 次，掉线 8 次，3 轮任务出错。断线是高频故障，掉线恢复与预热是插件的两条主路径。

## 边界

- 不修改 MAA 程序文件与游戏客户端，只读写 `config/` 与 `debug/`。
- 不写入 `ExitArknights` 或 `ExitEmulator`；游戏与模拟器由环境清理关闭。
- 不改系统音量、显示缩放与电源设置；这些由宿主或环境插件处理。
- 两处 MAA 副本不能同时运行，避免争用同一个模拟器与账号。