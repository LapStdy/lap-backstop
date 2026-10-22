# 02 · MAA 操作与配置

[返回总览](../00-总览与索引.md) · [返回方舟日常](00-方舟日常.md)

本文件回答：MAA 装在哪、怎么启动与连接、日志里哪些行能当信号、`gui.new.json` 怎么读写、哪些字段由平台接管、测试副本怎么用、Python 绑定能做什么、哪些东西不许碰。

## 路径与版本

| 项 | 路径或取值 |
|---|---|
| MAA 程序 | `H:/MAA/MAA.exe` |
| MAA 版本 | 6.20.1 |
| 主配置 | `H:/MAA/config/gui.new.json` |
| 另一份配置 | `H:/MAA/config/gui.json` 与其 `*.bak` |
| GUI 日志 | `H:/MAA/debug/gui.log` |
| asst 日志 | `H:/MAA/debug/asst.log` |
| 资源 | `H:/MAA/resource` |
| 资源版本 | `H:/MAA/resource/version.json` |
| 更新器 | `H:/MAA/MAA.Updater.exe` |
| Python 绑定 | `H:/MAA/Python/asst` |
| 测试副本 | `D:/MAA-Test` |
| adb | `H:/MuMuPlayer/nx_main/adb.exe` |
| 模拟器管理 | `H:/MuMuPlayer/nx_main/MuMuManager.exe` |

`gui.json` 与 `gui.new.json` 同时存在；平台只读写 `gui.new.json`，不碰 `gui.json`。

## 启动

```powershell
$env:HTTP_PROXY = ''; $env:HTTPS_PROXY = ''; $env:ALL_PROXY = ''
Start-Process -FilePath 'H:\MAA\MAA.exe' -WorkingDirectory 'H:\MAA'
```

- 不带命令行参数，工作目录必须是 `H:/MAA`，否则相对路径解析错位。
- 启动前清空三个代理变量；代理不通时 MAA 的上报与更新请求会卡住。
- 启动日志依次出现 `MaaAssistantArknights GUI started`、`Version v6.20.1`、`User Dir H:\MAA\`。
- 启动到资源加载完成约 72 秒；其中 `LoadResource` 一段实测 37.4 秒，其余是配置加载与初始化。
- `RunDirectly=true` 时，启动后约 30 到 70 秒自动进入任务链；`false` 时停在主界面等待开始。
- 启动等待上限 180 秒；超时记 `E_SCRIPT_RESOURCE_TIMEOUT`，不再等。

## 连接

| 项 | 取值 |
|---|---|
| 连接配置 | `MuMuEmulator12` |
| adb 路径 | `H:/MuMuPlayer/nx_main/adb.exe` |
| 地址 | `127.0.0.1:16384` |
| 触摸通道 | `MumuExtras` |
| 模拟器目录 | `H:/MuMuPlayer` |
| 实例序号 | 0 |
| adb 自动重启 | 开 |
| 退出时结束 adb | 开 |

- 连接耗时实测约 5 秒；日志出现 `正在连接模拟器……`、`完整 MuMu 增强已就位`、`最快截图耗时: 7ms (MumuExtras)`。
- asst 日志记录 `MuMu extras input is ready[m_width=1280] [m_height=720]`，确认原生触控已就绪。
- 触摸走原生通道，asst 日志的 `mumu click: (x, y)` 与 `mumu swipe` 是 MAA 发出的输入事件。
- 截图走原生渲染，`ConnectionInfo` 的 `ScreencapCost` 给出平均值、最大值、最小值，实测 4 到 7 毫秒。
- 平台不重复连接 MAA 的 adb；MAA 运行时平台只读日志与截图，不发送输入。

## 日志信号

`gui.log` 是平台运行层的主信号源；`asst.log` 补充任务链与数值采样。

| 日志 | 信号 | 用途 |
|---|---|---|
| `gui.log` | `MaaAssistantArknights GUI started` / `Version vX` | 启动与版本 |
| `gui.log` | `正在连接模拟器……` | 连接开始 |
| `gui.log` | `最快截图耗时: N ms (MumuExtras)` | 截图通道探测 |
| `gui.log` | `开始任务: X` / `完成任务: X` / `任务出错: X` | 单任务进度 |
| `gui.log` | `任务已全部完成！` | 整条队列完成的唯一脚本侧依据 |
| `gui.log` | `任务已结束，以下任务出现错误:` 加任务名列表 | 整轮失败汇总 |
| `gui.log` | `游戏掉线，停止任务` | 游戏连接中断 |
| `gui.log` | `正在停止……` / `已停止` | 停止过程 |
| `gui.log` | `Post actions: …` | 收尾动作 |
| `asst.log` | `AllTasksCompleted {finished_tasks,taskchain,taskid}` | 任务链结束；出错时也会触发，不能当完成 |
| `asst.log` | `TaskChainStart` / `TaskChainCompleted` / `TaskChainError` | 任务链粒度 |
| `asst.log` | `SubTaskExtraInfo` 的 `SanityBeforeStage.current_sanity` | 理智采样 |
| `asst.log` | `ConnectionInfo` 的 `ScreencapCost` | 截图耗时 |
| `asst.log` | `mumu click: (x, y)` / `mumu swipe` | 原生触控事件 |

- 日志按字节偏移增量读取，只读新增部分；`asst.log` 可达数十 MB，不能整文件载入。
- 日志字符串随界面语言变化；`Gui.Localization` 固定 `zh-cn`。语言被改走时按配置错误停手，不猜英文变体。
- 每轮记录启动时间、连接时间、每个任务开始与结束时间、完成或错误汇总行。

## 配置结构

`gui.new.json` 是 System.Text.Json 格式，缩进 2，UTF-8 无 BOM。

| 层级 | 键 | 内容 |
|---|---|---|
| 根 | `Configurations` | 多套配置，键是配置名 |
| 根 | `Current` | 当前配置名 |
| 根 | `Gui` | 应用级界面设置：语言、托盘、窗口位置 |
| 根 | `Timers` | MAA 定时启动设置 |
| 根 | `Update` | MAA 自身更新设置 |
| 根 | `ConfigVersion` | 配置版本号，当前 1 |
| 配置 | `TaskQueue` | 任务列表，顺序即执行顺序 |
| 配置 | `Gui.PostActions` | 完成后动作 |
| 配置 | `Gui.StartUpSettings` | 启动模拟器、直接开跑、等待秒数 |
| 配置 | `Gui.ConnectSettings` | adb 路径、地址、触摸通道 |
| 配置 | `Gui.ExternalNotification` | 外部通知渠道与发送条件 |
| 配置 | `Gui.RuntimeSettings` | 运行期开关：启动游戏、防睡眠、时长上限 |

`PostActions` 在 `Configurations.<配置名>.Gui` 下，不在根 `Gui` 下；写错层级不会生效。

## 配置读写

写入规则：

1. 先退出 MAA。MAA 退出时把内存配置写回 `gui.new.json`，运行中写入会被覆盖。
2. 用 `tasklist /FI "IMAGENAME eq MAA.exe"` 检测；在跑时拒绝写入并返回 `E_CONFIG_LOCKED`，等 30 秒后仍运行则停止 MAA 再试。
3. 每次写入前备份为 `gui.new.json.<YYYYmmdd-HHMMSS>.bak`，保留最近 20 份。
4. 写临时文件 `.json.tmp`，再原子替换；保持 UTF-8 无 BOM、缩进 2、LF 换行。
5. 写入后读回，比对字段与文件哈希；不一致回滚到备份并记 `E_CONFIG_INVALID`。
6. 列表字段 `StagePlan`、`RoomList`、`StageList` 必须写 JSON 数组；类型写错会让整份配置解析失败。
7. 任务用 `$type` 决定反序列化类型；未知 `$type` 会导致整份配置加载失败，写入前校验类型白名单。
8. 同一时刻只写一套配置；`Current` 指向哪套，MAA 就运行哪套。

`scripts/maa_config.py` 是无依赖的读写工具，支持 `profiles`、`use`、`new`、`tasks`、`add`、`rm`、`move`、`set`、`show`、`post-actions`、`run-directly`、`title-prefix`。它做运行检测、备份与原子替换；读回校验由调用方执行。

## 完成后动作

`PostActions` 是位标志，序列化为逗号分隔字符串；空字符串等于 `None`。

| 值 | 位 | 含义 |
|---|---|---|
| `None` | 0 | 无动作 |
| `ExitArknights` | 1 | 退出游戏 |
| `BackToAndroidHome` | 2 | 回到模拟器桌面 |
| `ExitEmulator` | 4 | 退出模拟器 |
| `ExitSelf` | 8 | 退出 MAA |
| `IfNoOtherMaa` | 16 | 没有其他 MAA 时执行 |
| `Hibernate` | 32 | 休眠 |
| `Shutdown` | 64 | 关机 |
| `Sleep` | 128 | 睡眠 |
| `SkipOnError` | 256 | 任务出错时跳过后续动作 |
| `LockScreen` | 512 | 锁屏 |

平台接管规则：

- 写 `ExitSelf`：MAA 在成功、失败、掉线三种结果下都退出，进程退出状态可作证据。
- 不写 `ExitArknights`、`ExitEmulator`、`Hibernate`、`Shutdown`、`Sleep`、`LockScreen`；游戏与模拟器由 `cleanup` 关闭，保温期间保留。
- 发现外部写入 `ExitArknights` 或 `ExitEmulator` 时重写为平台值，记 `E_CONFIG_CONFLICT`，继续运行。
- 该字段不适用「以脚本为主」的默认策略。

## Timers

`Timers` 在根层级，含 `ForceScheduledStart`、`ShowWindowBeforeForceScheduledStart`、`CustomConfig`、`NotifyBeforeScheduledStart`、`ScheduledStartNotificationMinutes`、`ScheduledWakeUp` 与 8 个定时槽 `List[0..7]`，每槽有 `Id`、`IsEnabled`、`Config`、`Hour`、`Minute`。

- 平台是唯一调度者；MAA 定时槽全部 `IsEnabled=false`，`ScheduledWakeUp=false`。
- MAA 定时启动与平台调度同时启用会造成重复执行，且 MAA 侧没有幂等键与证据链。
- 用户手动启用时，平台检测到 `Timers` 变化写冲突事件，不把该次启动当平台运行。

## 外部通知

`Gui.ExternalNotification` 含 `Configs` 与 `Delivery`；`Configs` 当前是 `ServerChan` 渠道，`SendKey` 是密钥。`Delivery` 的开关包括 `Enable`、`SendWhenComplete`、`SendWhenError`、`SendWhenStalled`、`SendBeforeScheduledStart`、`SendAfterLogCount`、`SendWhenContentMatches` 及白名单、黑名单、条数上限、时间窗。

- 平台是唯一通知源；写入 `Delivery.Enable=false`，避免同一结果两条通知。
- `SendKey` 与 `MirrorChyanCdk` 都是密钥，不写日志、不进事件、不进证据。
- 通知内容与渠道由平台负责；MAA 侧只保留日志。

## MAA 自身更新

`Update` 在根层级，关键字段：`Name`、`VersionType`、`UpdateSource`（当前 `MirrorChyan`）、`CheckOnStartup`、`CheckOnSchedule`、`AutoDownloadUpdatePackage`、`AutoInstallUpdatePackage`、`DoNotShowUpdate`、`AllowNightlyUpdates`、`Proxy`。

- MAA 启动时检查自身与资源更新；检测到新版本会下载，是否自动安装由 `AutoInstallUpdatePackage` 控制。
- 安装由 `MAA.Updater.exe` 执行：等父进程退出、拿更新互斥、按计划文件替换程序与资源、按需重启。
- 平台不修改 MAA 程序文件；MAA 更新是独立作业，不塞进日常作业的 `execute`。
- 作业模板记录 MAA 版本与资源版本；版本变化写事件，日常作业与更新作业分开判定。

## 测试副本

| 项 | 取值 |
|---|---|
| 位置 | `D:/MAA-Test` |
| 窗口标题前缀 | `测试·` |
| 配置名 | `Default`、`项目测试` |
| `PostActions` | `None` |
| `RunDirectly` | `false` |
| 用途 | 配置工具与启动流程的回归测试 |

测试副本与日常副本连同一个模拟器、同一个账号，不能同时运行。测试前把 `Current` 切到 `项目测试`，测试后切回。

## Python 绑定

`H:/MAA/Python/asst` 是官方绑定，`Asst` 类的主要方法：

| 方法 | 用途 |
|---|---|
| `load(path)` | 加载资源 |
| `connect(adb_path, address, config)` | 连接模拟器 |
| `append_task(type_name, params)` | 追加任务，返回 `task_id` |
| `set_task_params(task_id, params)` | 修改任务参数 |
| `start()` / `stop()` / `running()` | 开始、停止、查询运行状态 |
| `get_image(size)` | 取一帧原始图像 |
| `set_instance_option(option, value)` | 设置实例选项 |
| `get_version()` | 取版本 |

- 回调是 JSON 事件流，按 `what` 区分 `TaskChainStart`、`TaskChainCompleted`、`TaskChainError`、`SubTaskExtraInfo`、`AllTasksCompleted` 等。
- 绑定模式不读 `gui.new.json`，任务队列由调用方通过 `append_task` 构造。
- 同一个 MAA 实例不能同时被 GUI 与绑定驱动；同一环境同一时刻只允许一个脚本驱动者。

## 边界

- 不修改 MAA 程序文件与 `resource/`；更新只由 `MAA.Updater.exe` 执行。
- 不修改游戏客户端文件，不注入、不破解。
- 不用 MAA 的 `Timers` 代替平台调度。
- 不改系统音量、显示缩放、电源设置；这些属于环境或宿主。
- MAA 版本与资源版本变化后先跑冒烟作业，再恢复日常调度。
