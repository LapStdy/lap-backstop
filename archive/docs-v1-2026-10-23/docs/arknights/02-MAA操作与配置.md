# MAA 操作与配置

[返回方舟日常](00-方舟日常.md) · [返回总览](../00-总览与索引.md)

## 定位

MAA 是当前唯一被托管的脚本。本文写它的位置、启动方式、连接参数、日志信号、任务队列格式与配置读写方式。核心程序只经适配器接触 MAA，不修改它的程序文件。

## 位置与文件

| 项 | 路径 |
|---|---|
| 程序 | `H:\MAA\MAA.exe` |
| 配置 | `H:\MAA\config\gui.new.json` |
| asst 日志 | `H:\MAA\debug\asst.log` |
| GUI 日志 | `H:\MAA\debug\gui.log` |
| 资源 | `H:\MAA\resource` |

## 启动

- 直接运行 `MAA.exe`，不带参数，工作目录为 MAA 目录。
- 启动前剔除 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`，否则 MAA 的上报请求会走死代理。
- `RunDirectly=true` 时，启动后约 30 到 70 秒自动开始任务链，不需要点界面。
- 任务结束后的动作由 `PostActions` 决定，见下文。

## 连接

- 连接配置 `MuMuEmulator12`：adb `H:\MuMuPlayer\nx_main\adb.exe`，地址 `127.0.0.1:16384`。
- 触摸走 MuMu 原生通道 `MumuExtras`，日志记作 `mumu click` / `mumu swipe`。
- 截图优先走 MuMu 原生渲染接口 `H:\MuMuPlayer\nx_device\15.0\shell\sdk\external_renderer_ipc.dll`，实测 10 到 150 毫秒；adb 截图约 600 毫秒。
- 原生接口导出 11 个函数：`nemu_connect`、`nemu_disconnect`、`nemu_get_display_id`、`nemu_capture_display`、`nemu_input_event_touch_down`、`nemu_input_event_touch_up`、`nemu_input_event_finger_touch_down`、`nemu_input_event_finger_touch_up`、`nemu_input_event_key_down`、`nemu_input_event_key_up`、`nemu_input_text`。调用签名未公开，接入前需另行逆向。

## 日志信号

| 信号 | 位置 | 用途 |
|---|---|---|
| `正在连接模拟器……` | gui.log | 连接开始 |
| `最快截图耗时: N ms` | gui.log | 截图通道探测结果 |
| `开始任务: X` | gui.log | 任务链开始 |
| `完成任务: X` | gui.log | 单个任务完成 |
| `任务出错: X` | gui.log | 单个任务失败 |
| `任务已全部完成！` | gui.log | 整条队列完成；完成判定的 MAA 侧唯一依据 |
| `游戏掉线，停止任务` | gui.log | 游戏连接中断；兜底入口 |
| `Post actions: …` | gui.log | 收尾动作 |
| `AllTasksCompleted` | asst.log | 单条任务链结束，不能作为整队列完成的依据 |
| `Current Sanity: a , Max Sanity: b` | asst.log | 理智采样 |

## 任务队列

配置在 `gui.new.json` 的 `Configurations` 下保存多套，`Current` 指定当前使用的一套，切换配置只改 `Current`。

每套配置的 `TaskQueue` 是任务列表，顺序即执行顺序；任务用 `$type` 区分类型：

```text
StartUpTask  CloseDownTask  FightTask  OperProgressTask  AwardTask  MallTask
InfrastTask  RecruitTask  RoguelikeTask  CopilotTask  SSSCopilotTask
SingleStepTask  DepotTask  OperBoxTask  UserDataUpdateTask  ReclamationTask
DepotMaintainTask  SwitchThemeTask  CustomTask
```

通用字段：`Name`、`IsEnable`、`TaskType`。其余字段由任务类型决定，例如 `FightTask` 的 `StagePlan`、`TimesLimit`、`EnableTimesLimit`，`InfrastTask` 的 `RoomList`、`Filename`。

手写任务只需要 `$type`、`Name`、`IsEnable`，其余字段由 MAA 用自己的默认值补全。

## 完成后动作

`Gui.PostActions` 是位标志，写成逗号分隔的字符串：

| 值 | 含义 |
|---|---|
| `None` | 无动作 |
| `ExitArknights` | 退出游戏 |
| `BackToAndroidHome` | 回到模拟器桌面 |
| `ExitEmulator` | 退出模拟器 |
| `ExitSelf` | 退出 MAA |
| `IfNoOtherMaa` | 无其他 MAA 时执行 |
| `Hibernate` | 休眠 |
| `Shutdown` | 关机 |
| `Sleep` | 睡眠 |
| `SkipOnError` | 出错时跳过后续动作 |
| `LockScreen` | 锁屏 |

MAA 的日常副本保存为 `ExitEmulator, ExitSelf`；项目测试副本为 `None`。

程序接管这一字段：

- 程序把 MAA 的完成后动作写成 `ExitSelf`，游戏与模拟器由收尾段关闭。
- 程序不写入 `ExitEmulator` 或 `ExitArknights`；发现外部改成这两个值，按程序值重写并记冲突。
- 这一字段不适用「以脚本为主」的默认策略。
- 收尾顺序：MAA 结束 → 后置动作核对画面 → 判定 → 收尾段关闭游戏与模拟器。

## 配置读写

`scripts/maa_config.py` 直接读写 `gui.new.json`：

```powershell
python scripts/maa_config.py --maa-dir D:\MAA-Test profiles
python scripts/maa_config.py --maa-dir D:\MAA-Test use 项目测试
python scripts/maa_config.py --maa-dir D:\MAA-Test tasks
python scripts/maa_config.py --maa-dir D:\MAA-Test add FightTask --name 测试理智 --index 1
python scripts/maa_config.py --maa-dir D:\MAA-Test set 3 StagePlan '["1-7"]'
python scripts/maa_config.py --maa-dir D:\MAA-Test set 3 IsEnable false
python scripts/maa_config.py --maa-dir D:\MAA-Test rm 3
python scripts/maa_config.py --maa-dir D:\MAA-Test post-actions None
```

写入规则：

- MAA 运行时会用内存里的配置覆盖文件，写入前必须先退出 MAA；脚本检测到 MAA 在跑会拒绝写入。
- 每次写入前自动备份为 `gui.new.json.<时间>.bak`。
- 写入后读回校验；完成后动作是程序接管字段，不写入退出游戏或退出模拟器。
- 列表字段（`StagePlan`、`RoomList`）必须传 JSON 数组，类型写错会导致整份配置解析失败。

## 测试副本

| 项 | 值 |
|---|---|
| 位置 | `D:\MAA-Test` |
| 窗口标题前缀 | `测试·` |
| 完成后动作 | `None` |
| 自动开跑 | `RunDirectly=false` |
| 配置 | 独立于日常副本，含 `Default` 与 `项目测试` 两套 |

两份副本不能同时运行：它们连同一个模拟器、同一个账号。

## 边界

- 不修改 MAA 程序文件，只读写 `config/` 与 `debug/` 下的日志。
- 不修改游戏客户端。
- 游戏掉线（`游戏掉线，停止任务`）不在 MAA 层兜底，由管家程序负责重连、重启游戏或停手通知。
