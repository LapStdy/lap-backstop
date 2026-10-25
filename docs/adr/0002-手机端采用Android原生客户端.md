# ADR 0002 · 手机端采用 Android 原生客户端

[返回总览](../00-总览与索引.md)

- 状态：superseded（由 [ADR 0011](0011-手机端改为同一页面的安装壳.md) 取代）
- 日期：2026-10-23
- 关联：[07-Android客户端](../07-Android客户端.md)、[06-图形界面](../06-图形界面.md)、[ADR 0001](0001-分层与目录结构.md)

## 背景

平台需要手机端，理由有两条：

- 可用性：运行在书房电脑上，人在外面要知道「今天成没成、现在卡在哪、能不能停」。
- 作品线：手机端是能拿出手的交付物，观感与手感要接近成熟产品，而不是一个能用的网页。

约束同时成立：单人维护；后端是 Python；电脑端已有 WebView2 窗口；业务真相与判定全在电脑侧；出门只能经 Tailscale 访问。

候选路线：Kotlin + Jetpack Compose、Flutter、React Native/Expo、Capacitor/WebView、KMP/Compose Multiplatform、纯 Web PWA。对比结论见第「替代方案」节。

## 决策

1. 手机端是原生 Android 客户端：Kotlin + Jetpack Compose，单模块起步。
2. 定位是瘦客户端与作品线：三屏（总览、运行、设置），只经 /api/v1 与 SSE 读写，不承载内核、不排程、不判定。
3. 不首期引入 KMP 与 Compose Multiplatform：单端阶段只增加构建与调试面，没有必须共享的复杂状态机。
4. 通信是 HTTP + SSE：状态与事件走 SSE，命令走 POST，不自造 WebSocket 业务协议。
5. 认证是固定 Bearer token：token 写在平台配置里，桌面端设置页展示当前值，手机扫码或手输导入后只放在请求头。配对码、设备台账、Keystore 硬件保护都不是一期必需。
6. 网络是局域网直连加 Tailscale；不做公网入口、端口映射与中转服务。
7. 分发是签名 APK 加 GitHub Releases，手动覆盖安装，保留本地设置。
8. 视觉与动效定向借用 dsh-mobile 的 MIT Compose 组件与设计 token：深海背景、玻璃卡、状态点、短促转场。

## 后果

- 新增 Kotlin 与 Gradle 工具链，单人维护面变大；构建链版本升级要单独跟进。
- 不能复用桌面 WebView2 里的页面代码；两端各自实现，靠 API 契约与事件语义对齐。
- 内核必须保持 /api/v1 与事件字段的向后兼容；APK 不随内核发版，契约不兼容时服务端返回明确错误码。
- 手机端能力有意少于桌面：模板、计划表达式、插件配置的完整编辑留在桌面，手机只切启用状态与叫停。
- 需要一套真机验收（K-01–K-08），覆盖连接、首屏、断线、叫停、通知、Tailscale。
- 安全面收敛：没有配对码与设备台账，撤销访问靠更换 token 并重启内核，泄漏处置只有换 token 一条路。

## 替代方案

| 方案 | 不采纳的原因 |
|---|---|
| Flutter | Android-only 下跨端优势用不上；玻璃与流体背景要自绘或插件，系统集成仍要平台通道 |
| React Native/Expo | 可复用 TS 逻辑，但桌面页面是 WebView2，不能复用；玻璃效果要 Skia 或原生模块，Expo 版本与原生模块绑定 |
| Capacitor/WebView | 出包最快，但界面就是网页；玻璃只能近似，旧 WebView 缺少 backdrop-filter，观感达不到作品线要求 |
| 纯 Web PWA | 无需安装，但没有前台服务与本地通知，锁屏提醒与后台连接不可靠 |
| KMP / Compose Multiplatform | 单端阶段收益为负；等出现第二个端且状态机复杂到需要一份实现时再抽 shared |

## 相关

页面范围、数据映射、通知与验收写在 [07-Android客户端](../07-Android客户端.md)；桌面窗口与手机浏览器写在 [06-图形界面](../06-图形界面.md)。
