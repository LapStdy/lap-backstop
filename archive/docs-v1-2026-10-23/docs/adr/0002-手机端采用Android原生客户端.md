# ADR 0002 · 手机端采用 Android 原生客户端

- 状态：已采纳
- 日期：2026-10-19

## 背景

手机端入口是 Android 安装包，电脑端保留 WebView2 窗口。两条约束同时成立：

- 手机端要能直连电脑上的核心进程，看状态、叫停、处理冲突、看历史。
- 界面要接近 dsh-mobile 的观感：深海背景、液态玻璃、状态点、运行中停止方块、短促动效。

候选路线包括 Kotlin + Compose、Flutter、React Native/Expo、Capacitor/WebView、KMP/Compose Multiplatform。对比见 [research/04](../research/04-安卓入口与技术栈对比.md)。

## 决策

1. **手机端是原生 Android 客户端，Kotlin + Jetpack Compose，单模块起步。**
2. **不首期引入 KMP。** 业务状态与判定都在电脑侧，手机是瘦客户端；等出现第二个端且协议/状态机复杂到无法维护两份实现时再抽 shared。
3. **不引入 Flutter、React Native、Capacitor 作为首期实现。** 前两者在 Android-only 下换不来跨端收益，还要补平台通道；Capacitor 的 WebView 无法还原玻璃与流体背景。
4. **通信是 HTTP + SSE。** 状态与事件走 SSE，命令走 HTTP POST；不另造 WebSocket 业务协议。
5. **配对是一次性二维码 + 长期设备令牌。** 令牌存 Android Keystore，服务端只存 SHA-256 摘要；设备可撤销。
6. **出门优先 Tailscale。** 不默认开公网 Tunnel；公网入口与 S-104 冲突，需单独决策并改边界。
7. **玻璃库优先 `io.github.kyant0:backdrop` 1.0.6。** 与 dsh-mobile 的 Compose 封装同源，复刻成本最低；长期跨端可换 Haze 2.x。
8. **分发是签名 APK + GitHub Releases。** 不依赖应用商店；应用内更新作为二期。

## 后果

- 手机端放弃现有 React 页面复用；电脑端 WebView2 与 React 页面继续使用。
- 新增一套 Kotlin/Gradle 工具链与 Android 构建链，单人维护面变大。
- 可以定向借用 dsh-mobile 的 MIT Compose 组件（主题、玻璃、背景、触感、自适应布局），缩短达到同等观感的时间。
- 后端 API 需要补配对、设备表、`/api/health` 能力位与 SSE 游标；现有电脑端页面不受影响。
- 手机端不共享页面代码，但共享同一套 HTTP API 与 SSE 事件语义。

## 替代方案

| 方案 | 不采纳的原因 |
|---|---|
| Flutter | Android-only 下跨端优势用不上；玻璃/流体与系统集成仍要平台通道；不能复用 dsh-mobile 的 Compose 组件 |
| React Native/Expo | 可复用 TS 逻辑，但不能复用 React 页面；玻璃/流体要 Skia 或原生模块，Expo 版本与原生模块绑定是隐性成本 |
| Capacitor/WebView | 出包最快，但 UI 是 WebView；玻璃只能近似，旧 WebView 缺 `backdrop-filter` |
| KMP | 单端阶段只增加构建与调试面；手机端是瘦客户端，没有必须共享的复杂状态机 |
| Compose Multiplatform | 未来 iOS/桌面共享 UI 的合理选项；当前 Android-only，先按原生 Android 起步 |

## 后续

M0 的页面范围、数据映射、动效与验收写在 [07-Android客户端](../07-Android客户端.md)。实现前不建工程、不写代码。
