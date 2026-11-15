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
| `environment` | MuMu 6.0 + 明日方舟的探测、准备、复位、保温与清理 |
| `script` | MAA 的启动、观察、停止与恢复 |
| `verifier` | 本次运行 `gui.log` 段的脚本证据判据，结论 `completed` / `failed` / `unknown` |
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

探测 `probe` 经只读端口确认五件事：模拟器实例进程在且 Android 启动完成、模拟器窗口存在、
adb 可连接且可截图、游戏在前台、游戏横屏时截图是 1280×720。实例状态取自
`MuMuManager info -v 0` 的 `is_process_started` 与 `is_android_started`，读不到时退回窗口
判据。任一不满足时返回对应状态：实例或窗口不在为 `off`，实例在启动为 `booting`，
adb 失联或游戏未就绪为 `degraded`，分辨率或缩放不符为 `failed`。

准备 `prepare` 的步骤与判据：

| 顺序 | 步骤 | 判据 | 超时 |
|---|---|---|---|
| 1 | 复用或启动 MuMu | 实例未启动时 `control launch`；adb 设备在线（先 `kill-server` 再 `connect`） | 180 秒 |
| 2 | 确认可截图 | 截到一帧 | 单次截图 |
| 3 | 拉起游戏 | 游戏包在前台 | 60 秒 |
| 4 | 登录流程 | 状态机按 [07](../arknights/07-登录流程状态机.md)：准备画面点一次 `START`、登录界面点 `开始唤醒`、连接中与加载屏等待、覆盖层处置，判到主界面才继续 | 每阶段 30–120 秒 |
| 5 | 主界面门控 | 交接档：总分 ≥ 8、外观命中 ≥ 2、连续 2 帧（规则见 [03](../arknights/03-主界面判定与导航.md)，实现见 [04](../arknights/04-主界面探针实现.md)） | 2 秒 |
| 6 | 启动 MAA 并加载资源（`binding`） | `load` 与 `connect` 成功 | 180 秒 |

- 步骤 1–5 由环境 provider 的 `prepare` 执行（步骤 4 可用配置 `login_flow` 关闭，步骤 5 可用 `page_gate` 关闭）；步骤 6 属脚本能力。

登录流程（步骤 4）的判读与动作：

- 判读先跑主界面判定（`HomeJudge`），达标即 `HOME`；否则按三张真机帧模板判阶段：准备画面 `start_button`（阈值 0.80）、登录界面 `wake_button`（0.85）、加载屏 `loading_logo`（0.85）；都不中按未知画面等待。
- 动作只有三处固定安全坐标：准备画面 `START`、登录界面 `开始唤醒`、覆盖层里的公告关闭与限时活动签到卡。身份不明的覆盖层、退出确认、维护与掉线一律停手，不猜坐标。
- 状态只前进不后退；回退到更早画面、阶段超时、截帧失败都按 `E_ENV_NOT_READY` 收口，失败阶段与原因写进结果说明。
- 分辨率在 `probe` 里核对：仅当游戏在前台时比较截图的宽高与 `screen_size`。
- 主界面门控的截帧通道由 `frame_source` 选：`mumu`（默认）走 `external_renderer_ipc.dll` 的 `nemu_capture_display`，帧是 RGBA8888 且行序自下而上，端口翻成自上而下 BGR 后再判定，每次取帧连接一次、取完断开；`adb` 走 `exec-out screencap`。真机同刻对比（2026-11-15）：两通道都是 1280×720、像素均值差 0.212；原生 3.3–6.0 毫秒，adb 236–822 毫秒并出现过 5.2 秒离群（超过门控 2 秒预算）。原生取帧有超时上限（默认 5000 毫秒，构造时可覆盖）：DLL 卡死最迟约「上限 + 200 毫秒」按截帧失败收口，照常断开连接，下一次取帧不受影响；超时事件尚未接入生产写入口（见 `DEBT.md` 的 `D-026`）。取不到帧时门控按 `E_ENV_UNREACHABLE` 失败，不自动退回另一通道。

主界面门控（步骤 4）的口径：

- 帧来源是帧端口 `FrameSource`，默认 adb 截帧实现（`adb -s <地址> exec-out screencap -p`）；判定器与连续帧确认复用 03、04 的同一套锚点与门槛，只判定不点击、不按键、不启停任何进程。
- 每次判定写两条证据：截帧 PNG（`screenshot`，原始帧字节，文件名含帧哈希前 12 位）与判定明细 JSON（`probe`，逐锚点得分、偏移、两路命中、通道、总分、档位、结论、判定耗时与帧哈希）。
- 连续 2 帧未达交接档：prepare 失败并返回 `E_ENV_NOT_READY`，另写一条 `run.signal`（`kind=maa.gate.unknown`，载荷含总分、外观命中数与帧哈希）；截帧失败或帧不是 PNG 返回 `E_ENV_UNREACHABLE`；帧宽高不是 1280×720 返回 `E_ENV_RESOLUTION`，不缩放继续识别。
- `page_gate=false` 时不截帧、不写证据、不写信号，prepare 在步骤 3 完成后直接放行，行为与未接门控一致。
- 前三步失败时不做门控，保持原有错误码与语义。
- 主界面门控（步骤 5）一次约 1.5 秒：adb 截帧约 600 毫秒 ×2、帧间等待 0.3 秒、判定 3.4–4.1 毫秒，预算 2 秒。
- 步骤 6 在预热窗口内执行；没有预热窗口时，等待计入准备阶段上限。
- `gui` 模式不做预启动；MAA 启动与资源加载在 `launch` 时发生，计入执行阶段。
- `reset` 先 `adb disconnect` 再 `adb connect`，结束游戏进程后重新拉起并等它到前台；环境状态回到 `booting` 由内核按探测迁移。
- `warm` 确认游戏在前台，不在就拉起并等；`minutes` 为 0 是复用确认（保温窗口内再次运行前的现场检查），大于 0 时是本次保温时长，两者都记进结果说明。
- `cleanup(keep_warm)`：`keep_warm=true` 保留游戏与模拟器；`false` 时先结束游戏进程再关闭模拟器实例。

MAA 启动前剔除 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`，否则上报请求会走死代理。MAA 以工作目录 `H:/MAA` 启动，不带命令行参数。

## 脚本

| 模式 | 启动方式 | 完成信号 | 适用场景 |
|---|---|---|---|
| `gui` | 运行 `MAA.exe`，`RunDirectly=true` 时自动开跑 | `gui.log` 的 `任务已全部完成！` | 与现有配置、日志、PostActions 行为一致 |
| `binding` | 官方 Python 绑定：`load` / `connect` / `append_task` / `start` / `stop` / `running` / `get_image` | 绑定回调的 JSON 与 `running` 状态 | 需要更细的节点信号与主动截图 |

`launch` 在 `gui` 模式下启动新的 `MAA.exe` 进程（工作目录为 MAA 目录、剔除代理变量），记录 pid 与两份日志的起始偏移；`RunDirectly=true` 时 MAA 在资源加载完成后自动开跑。`binding` 模式复用常驻 asst 实例并执行 `append_task` 与 `start`，把冷启动移出关键路径。

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

`stop` 先向 MAA 的窗口发 `WM_CLOSE` 请它自己收尾，10 秒宽限期内没退出才结束整棵进程树；没有窗口时直接结束进程树。`recover` 接收内核动作 `restart_script`、`reset_environment`、`abort`；`restart_script` 之下再分重新附着常驻实例、重启脚本、重启游戏三档，`abort` 之下停止脚本并通知。

## 验证

判定只用脚本证据：脚本启动时把本次运行的日志起点写进工作目录的会话标记，验证阶段按标记取 `gui.log` 的本次运行段。

| 结论 | 判据 |
|---|---|
| `completed` | 段内出现「任务已全部完成！」，没有任务出错、掉线与错误汇总行，且插件配置的期望任务集全部出现「完成任务: X」；配置没有期望任务集时只按完成行与失败行判定 |
| `failed` | 段内出现「任务出错: X」「游戏掉线，停止任务」或「任务已结束，以下任务出现错误:」任一 |
| `unknown` | 段内没有终止信号；或完成行在而期望任务未全部完成；或会话标记缺失、日志滚动、日志文件不可读 |

期望任务集是插件配置 `tasks` 里 `enabled` 不为 `false` 的任务名集合，逐个与段内
「完成任务: X」点名核对。核对结果写在检查项 `maa-task-set`（全齐 `ok`、有缺失
`failed` 并列出缺失任务名、没有期望集 `unknown`），脚本日志判定写在检查项
`maa-script-log`。失败优先于完成：MAA 在单任务出错后仍会打印「任务已全部完成！」，
历史日志里出现过这种组合（先 9 次任务出错再打印完成行），所以完成行只在没有失败行
时才算数；缺失任务不判 `failed`，只把结论压成 `unknown`。

证据：本次运行的 `gui.log` 段写进证据目录的 `maa-run-guilog.log`，登记为 `log`
证据；段超过 1 MiB 只保留末尾。会话标记之后本轮 `asst.log` 段内的理智采样
（`SanityBeforeStage` 的 `current_sanity`）汇总写 `maa-sanity.json`（采样数、首个
与末个 `current_sanity`、`delta=末-首`），登记为 `probe` 证据；没有采样或日志不可
读时不写，理智数值不参与结论。没有可判定日志段时写 `maa-verify-unknown.txt`
说明原因，结论记 `unknown`。

没有证据就不判完成：`unknown` 交人工核对，重试与停手由内核恢复策略决定，插件不重跑、不改环境。画面证据（结束截图、异常画面模板）不参与判定，待真机采帧后另卡接入。

## 配置投影

`gui.new.json` 中与本插件相关的部分：

| 字段 | 用途 |
|---|---|
| `Configurations` | 多套配置；`Current` 指定当前使用的一套 |
| `TaskQueue` | 任务列表，顺序即执行顺序；任务用 `$type` 区分类型 |
| `Gui.PostActions` | 完成后动作，位标志写成逗号分隔字符串 |

任务项只需要 `$type`、`Name`、`IsEnable`，其余字段由 MAA 按默认值补全。列表字段（`StagePlan`、`RoomList`）必须写 JSON 数组，类型写错会导致整份配置解析失败。

开跑前执行器先调用 `desired` 按插件配置构造整份目标：`profile` 定 `Current` 与配置段名（留空按 MAA 首次运行的配置名 `Default`），`tasks` 依序映射成段内 `TaskQueue`（`type`→`$type`、`name`→`Name`、`enabled`→`IsEnable`，`params` 并入任务项，同名键以参数为准）。整份目标不含平台接管值；文件里与这份目标无关的键随写入不再保留。

写入规则：

- MAA 运行时会用内存配置覆盖文件，写入前必须先退出 MAA；检测到 MAA 在跑时拒绝写入并返回 `E_CONFIG_LOCKED`（原因码 `maa.config_locked`）。
- `apply` 的期望配置是整份目标；两处平台接管值在写入前写死：`Gui.PostActions` 固定 `ExitSelf`，`Gui.ExternalNotification.Delivery.Enable` 置 false。
- 每次写入前把现行文件备份为 `gui.new.json.<YYYYmmdd-HHMMSS>.bak`（同一秒内追加序号），保留最近 `backup_keep` 份。
- 写入走同目录临时文件原子替换，保持 UTF-8 无 BOM、缩进 2、LF 换行；写完读回校验，不一致则把备份写回并按 `E_CONFIG_INVALID` 失败。
- 现行文件的 `Gui.PostActions` 含 `ExitArknights` 或 `ExitEmulator` 时按平台值重写，`meta.conflicts` 记 `E_CONFIG_CONFLICT`（原因码 `maa.post_actions_conflict`），不阻断本次写入。
- 合并策略由 `config_projection.strategy` 决定：`script_first`（默认）以现行文件为底，只覆盖 `Current`、当前配置段的 `TaskQueue` 与两处接管值，其余字段原样保留；`platform_first` 按整份目标覆盖，现行文件里目标未覆盖的键不留。

`desired` 只构造目标、不读写文件。`diff` 返回逐字段差异；`apply` 返回修订号、备份路径与写入后的配置哈希（`meta.revision`、`meta.backup`、`meta.sha256`、`meta.conflicts`），现行文件已与期望一致时不写盘（`meta.changed=false`）；`rollback` 按修订号取回备份并读回校验。`apply` 失败（含通道异常）时执行器按 `meta.revision` 回滚一次并写 `config.rolled_back`，运行交恢复策略收口，脚本不启动。日常副本与测试副本不能同时运行，它们连同一个模拟器与同一个账号。

## 参数 schema

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `maa_dir` | string | `H:/MAA` | MAA 目录 |
| `mode` | enum | `gui` | `gui` 或 `binding` |
| `mumu_dir` | string | `H:/MuMuPlayer` | MuMu 安装目录 |
| `adb_address` | string | `127.0.0.1:16384` | 模拟器 adb 地址 |
| `emulator_window_title` | string | `MuMu` | 模拟器窗口标题子串，判定窗口是否存在 |
| `game_package` | string | `com.hypergryph.arknights` | 游戏包名，判定游戏是否在前台 |
| `screen_size` | string | `1280x720` | 游戏横屏时的截图分辨率，写法「宽x高」 |
| `frame_source` | enum | `mumu` | 截帧通道：`mumu` 走原生渲染通道，`adb` 走 `exec-out screencap` |
| `mumu_instance_index` | integer | 0 | 模拟器实例序号 |
| `mumu_boot_timeout_s` | integer | 180 | 模拟器启动到 adb 可连的上限 |
| `game_launch_timeout_s` | integer | 60 | 游戏启动到前台的时限 |
| `page_gate` | boolean | `true` | 主界面门控：prepare 拉起游戏后截帧判定主界面，连续两帧确认才放行 |
| `login_flow` | boolean | `true` | 登录流程：prepare 在游戏到前台后跑登录状态机，到主界面才继续 |
| `profile` | string | `Default` | 使用的 MAA 配置名，即整份目标的 `Current` |
| `tasks` | array | 日常队列 | 每项含 `type`、`name`、`enabled`、`params` |
| `launch_timeout_s` | integer | 180 | 启动到资源加载完成的上限 |
| `wake_timeout_s` | integer | 90 | 开始唤醒到主界面的上限 |
| `startup_timeout_s` | integer | 300 | 游戏启动到登录界面的上限 |
| `sample_interval_s` | integer | 30 | 运行期抽帧间隔 |
| `keep_warm_minutes` | integer | 30 | 运行结束后保持环境就绪的时长 |
| `config_projection.enabled` | boolean | true | 是否在开跑前对账并写入 |
| `config_projection.backup_keep` | integer | 20 | 备份保留份数 |
| `config_projection.strategy` | enum | `script_first` | `script_first` 保留脚本侧普通字段；`platform_first` 整份目标覆盖 |

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
| 写入配置时 MAA 在运行 | `E_CONFIG_LOCKED`，原因码 `maa.config_locked` | 等待 30 秒，仍在运行则停止 MAA 后重试 |
| PostActions 被外部改成退出游戏或退出模拟器 | 重写为 `ExitSelf`；`meta.conflicts` 记 `E_CONFIG_CONFLICT`（原因码 `maa.post_actions_conflict`） | 记冲突，继续运行 |
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
| 主界面锚点判定 | 单帧 3.2 到 9.2 毫秒、中位 3.4（42 帧，含位置通道） |
| ROI 模板匹配 | 约 7 毫秒 |
| 全屏单模板匹配 | 100 到 190 毫秒 |
| 全屏五模板单帧 | 775 毫秒，超出 200 毫秒预算 |

29 天统计：开跑 60 次，完成 44 次，掉线 8 次，3 轮任务出错。断线是高频故障，掉线恢复与预热是插件的两条主路径。

## 边界

- 不修改 MAA 程序文件与游戏客户端，只读写 `config/` 与 `debug/`。
- 不写入 `ExitArknights` 或 `ExitEmulator`；游戏与模拟器由环境清理关闭。
- 不改系统音量、显示缩放与电源设置；这些由宿主或环境插件处理。
- 两处 MAA 副本不能同时运行，避免争用同一个模拟器与账号。