# MAAEnd 插件

[返回总览](../00-总览与索引.md)

本文件回答：MAAEnd 插件必须提供哪些能力、接口契约是什么、哪些事实尚未实测。

## 定位

MAAEnd 是《终末地》的自动化脚本，基于 MaaFramework 构建。插件把 MaaFramework 的任务描述、回调与日志包在能力接口后面；平台不解析 MaaFramework 的 pipeline 结构，只保存路径、哈希与运行结果。

| 项 | 内容 |
|---|---|
| 插件 id | `maaend` |
| 脚本 | MAAEnd |
| 环境 | 终末地客户端及其承载环境（模拟器或桌面客户端） |
| 框架 | MaaFramework，提供控制器、资源与任务执行 |
| 平台职责 | 调度、环境状态、资源租约、事件、证据、恢复预算 |
| 插件职责 | 启动客户端、驱动任务、读回调与日志、取画面、判定与自身配置 |

MAAEnd 与 MAA 可同时安装；作业模板按 `plugin_id` 路由，两个插件各读自己的配置。

## manifest

```json
{
  "id": "maaend",
  "name": "MAAEnd · 终末地",
  "version": "1.0.0",
  "api_version": 1,
  "description": "驱动 MAAEnd 完成终末地日常并验证结果",
  "capabilities": ["environment", "script", "verifier"],
  "config_schema": "config.schema.json",
  "requires": {"contract": ">=1 <2", "platform": "windows", "python": ">=3.12"}
}
```

## 环境

环境状态使用平台状态机：`off`、`booting`、`ready`、`busy`、`degraded`、`failed`。插件必须让每个状态可探测，不能只靠进程存在推断就绪。

| 方法 | 契约 |
|---|---|
| `probe` | 返回状态、观察时间与依据；必须同时检查承载环境、控制器连接与客户端画面 |
| `prepare` | 复用或启动承载环境，拉起客户端，等到可操作界面；每步有超时与判据 |
| `reset` | 重建控制器连接，按需重启客户端；结束后状态回到 `booting` |
| `warm` | 在给定时长内保持承载环境与客户端就绪 |
| `cleanup(keep_warm)` | 保温时只释放控制权；`keep_warm=false` 时关闭客户端与承载环境；重复调用安全 |

就绪判据必须是可观测事实：控制器可连接、客户端在前台、目标界面模板命中。三项缺一不可。分辨率、缩放与窗口位置在 `prepare` 中校验，不一致返回 `E_ENV_RESOLUTION`（`detail=maaend.resolution_mismatch`），不自动修改系统设置。

## 脚本

`ScriptAdapter` 的四个方法必须覆盖 MaaFramework 的完整生命周期：

| 方法 | 契约 |
|---|---|
| `launch` | 幂等；启动 MaaFramework 任务并返回句柄；重复调用复用已有实例 |
| `observe` | 只读；返回 `running`/`exited`/`hung`、最近节点或回调信号、进度与时间戳 |
| `stop` | 先请求停止，再等待退出；超时后终止；重复调用安全 |
| `recover` | 只执行内核动作 `restart_script`、`reset_environment`、`abort` 及其分解：重新附着、重启脚本、重启客户端、复位环境 |

完成信号必须显式，并满足以下要求：

1. 有任务级完成信号，来自 MaaFramework 的任务或回调通知，而不是进程存活。
2. 有日志终止行作为交叉验证；日志路径与匹配模式写进配置。
3. 节点级进度可用于卡住判定；拿不到节点信号时，用画面抽帧与日志游标代替。
4. 完成信号、失败信号与取消信号三者可区分；仅有「结束」而无结果时记 `unknown`。
5. 回调与日志都带时间戳；插件按游标读取，不重复消费。

## 验证

验证策略由 JobTemplate 的 `VerificationPolicy` 保存完整结构，`params.mode` 取 `script`、`screen`、`dual`，默认 `dual`，作业开始时冻结。

| 政策 | `completed` | `failed` | `unknown` |
|---|---|---|---|
| `script` | 任务完成信号出现且无失败信号 | 任务失败或控制器断开 | 完成信号缺失或语义不明 |
| `screen` | 结束画面为目标界面，无异常画面 | 命中错误、掉线、维护或更新画面 | 截图缺失或画面无法归类 |
| `dual` | 脚本与画面都指向完成 | 任一来源明确失败 | 任一来源缺失或两者矛盾 |

证据类型与平台一致：`probe`（任务与回调信号）、`log`、`exit` 来自脚本侧，`screenshot` 来自画面侧。每条证据带来源、时间、哈希与可读摘要。

画面模板来自实测采集；模板未覆盖的画面记 `unknown`，不用猜测结果补齐。脚本侧没有明确完成信号时，`dual` 不允许判为 `completed`。

## 配置

| 字段 | 类型 | 说明 |
|---|---|---|
| `client_path` | string | 终末地客户端可执行文件 |
| `environment.kind` | enum | 承载环境类型，取值由实测确定 |
| `environment.address` | string | 模拟器 adb 地址或桌面窗口标识 |
| `controller` | enum | 控制器类型，取值由实测确定 |
| `resource_dir` | string | MaaFramework 资源目录 |
| `pipeline_dir` | string | 任务 pipeline 目录；平台只存路径与哈希 |
| `task_entry` | string | 要执行的任务入口 |
| `timeouts` | object | 准备、启动、执行、验证四段超时 |
| `keep_warm_minutes` | integer | 运行结束后保持就绪的时长 |

三档验证口径写在 JobTemplate 的 `VerificationPolicy.params.mode`，不在插件配置里。

运行配置全部来自插件 schema，写入平台的配置修订；不读取 `gui.new.json`。MaaFramework 的 pipeline 文件随插件包发布，平台只保存路径与哈希，不解析、不写入。配置未通过 schema 校验时返回 `E_CONFIG`，作业被拦截。

## 错误映射

| 观测 | 错误码与 `detail` | 默认恢复 |
|---|---|---|
| 控制器连接失败 | `E_ENV_DISCONNECTED` / `maaend.controller_unreachable` | 重建控制器连接后重试 |
| 客户端启动失败或超时 | `E_ENV_WAKE_TIMEOUT` / `maaend.client_boot_timeout` | 重启客户端一次 |
| 任务器启动失败 | `E_SCRIPT` / `maaend.launch_failed` | 重启脚本一次 |
| 任务执行失败 | `E_SCRIPT_TASK_FAILED` / `maaend.task_failed` | 重启脚本并重跑一次 |
| 脚本进程退出且无完成信号 | `E_SCRIPT_EXITED` / `maaend.exited` | 重新启动并重跑一次 |
| 画面无法归类 | `E_VERIFY_UNKNOWN_SCREEN` / `maaend.unknown_screen` | 记录 `unknown`，交人工或 Advisor 分诊 |
| 截图或回调证据缺失 | `E_VERIFY_NO_EVIDENCE` / `maaend.no_evidence` | 记 `unknown`，不重试 |
| 配置未通过 schema | `E_CONFIG` / `maaend.config_invalid` | 拦截作业，等用户修正 |

恢复预算与动作上限由内核恢复策略决定，插件不扩充内核动作枚举，只执行动作并返回结果。

## 接入前需实测的项

以下各项进入 [TASKS.md](../../TASKS.md) 的「技术验证」段；实测前不设默认值，不写死坐标与等待时间，不把猜测写进契约。

| 项 | 要回答的问题 |
|---|---|
| 集成方式 | 进程内调用、CLI 还是 Python 绑定；版本与依赖如何固定 |
| 控制器 | 用模拟器 adb 还是桌面窗口控制器；连接参数与重连语义 |
| 完成信号 | 任务完成、失败、取消分别由哪个回调或日志行表达 |
| 节点信号 | 是否提供节点级进度；回调频率与丢失后的补偿方式 |
| 日志 | 文件位置、格式、轮转与游标读取方式 |
| 截图 | 截图通道、分辨率、单帧耗时与失败条件 |
| 客户端启动 | 启动命令、登录界面判据、进入主界面的判据 |
| 画面模板 | 目标界面、错误、掉线、维护与更新画面的模板与阈值 |
| 断线与更新 | 掉线、维护、版本更新的可观测信号与恢复路径 |

## 边界

- 不修改终末地客户端与 MaaFramework 程序文件，只读写配置、日志与资源目录。
- 不在没有完成信号时宣布完成；信号语义不明记 `unknown`。
- 不写死未经实测的坐标、等待时间与模板阈值。
- 不与其他插件争用同一客户端或同一账号；互斥由资源租约保证。