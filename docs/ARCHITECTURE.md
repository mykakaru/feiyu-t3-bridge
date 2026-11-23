# Feiyu T3 Bridge 架构与协议详解

本文档详细说明双 Bot 企业微信 ↔ 本机 T3 Code 桥接系统的模块划分、任务调度状态机、安全边界、配额接力与四层高可用自愈机制。

---

## 一、核心设计原则

1. **统一 T3 Code 入口与隔离工作区**
   - 所有经桥接触发的任务统一落在专用的 T3 Project（例如 `FeiyuTasks`，配置项 `t3_project_id` + `tasks_root`），与个人日常手工项目完全隔离，归档分组也互不干扰。
   - 每个事项分配独立工作目录与 `manifest.json`，明确隔离**主人直发正文**（具授权效力）与**引用消息、附件、他人消息**（仅作不可信上下文资料）。
2. **双 Bot 职责物理隔离**
   - **私聊远控 Bot（`private_bot`）**：`private_bot_receiver.cjs` + `private_bot_control.py`。使用独立 DPAPI 凭据与 WebSocket 长连接，专门承载主人私聊问询、语音/图片/文件双向传输、以及本地任意 T3 Agent 通过 `agent_relay.py` 发起的跨线程主动提问（`A-XXXX`）。
   - **群管理 Bot（`feiyu`）**：`wecom_receiver.cjs` + `bridge_control.py`。专门承载群内静态说明卡、群友 `@` 私聊秒推（`F-xxxxxx`）、草稿审核投递（`D-xxxx`）与统一向群发消息。
3. **安全边界：最小权限与 Fail-Closed**
   - 身份鉴权仅核对 `identity_mapping.json` 中经人工首轮私聊确认的**稳定 WebSocket `userid`**，绝不信任显示昵称或引用正文中的自称。
   - 媒体直链与单文件 AES 密钥在入队前即通过 Windows 当前用户 DPAPI 密封落盘（`sealed_media.cjs` / `sealed_media.py`），绝不写入队列 JSON、日志或命令行参数。
   - 媒体下载（`scoped_media.cjs`）强制校验精确 HTTPS 域名白名单、公网 DNS 解析校验（拒绝私网/回环地址）、禁重定向、100 MiB 上限与 60 秒超时。
   - 出站投递（`outbox.cjs`）采用“先持久化 attempt、再发请求”的防重机制；仅收到 `errcode=0` 标记 `server_accepted`，不确定超时的发送绝不盲目自动重试。

---

## 二、轻 / 重双池动态分级调度

调度引擎由 `bridge_daemon.py`、`business_queue.py` 与 `t3_dispatch.py` 协同驱动：

1. **统一轻槽入口（默认 `light_slots=8`）**
   - 所有新事项先进入 `queued`，由轻量入口模型（`entry_model`）执行清晰度审计与复杂度判定。
   - 纯问答、资料检索、轻文档整理或简单工具排查留在轻槽快速完结；需要高阶模型但仍属轻任务时，可返回 `ESCALATE` + `workload_tier=light` 沿用轻槽。
2. **重槽接力（默认 `regular_slots=4`）**
   - 涉及主工程代码/资产修改、多 Agent 协同、批量处理或复杂视频流程时，返回 `ESCALATE` + `workload_tier=heavy`（省略分级默认按重任务处理）。
   - 入口线程按配置归档，桥接持久化交接摘要与目标模型，立即释放轻槽并转入 `heavy_queued`，由 4 个独立重槽按创建时间领取并启动高阶主线程。
3. **显式指定模型直达**
   - 指令中明确指定模型与推理档位时（如 `【模型：gpt-6.2-astra max】`）：
     - 若目标档位与 T3 默认档位一致，直接启动目标模型线程；
     - 若为非默认档位，通过精确直通转发器调用 `delegate_task` 锁定精确 `options`，绝不静默降级；模型或档位不可用时直接阻断并向主人确认。
4. **排队免责、续办优先与进度回报**
   - `queue_expiry_exempt=true` 时，尚未拿到运行租约的排队任务不计入未启动超时。
   - 轻/重池各自优先领取本池的续办任务（追问答复、10 分钟窗口内 `【追加】`），同类按创建时间 FIFO。
   - 长任务排队或运行期间，按 `progress_report_interval_seconds`（默认 1800 秒）推送进度回执；静默、追问和终态自动跳过，错过的间隔不补发。
5. **10 分钟续办窗口与 0 模型自动归档**
   - 任务完成后保留 `auto_archive_after_seconds=600` 秒续办窗口，期间可直接在 T3 桌面续聊或在企微 `【追加】`/引用收据回接原线程。
   - 空闲超时后由守护进程通过短期最小权限会话调用 `thread.archive` 自动归档（不触碰其他项目、置顶、运行中或等待追问线程）。
   - 所有私聊办结回执末尾均附带关键处理 Agent 的 `会话ID`，即使线程已归档也可随时用 `t3_thread_read` / `t3 thread show` 溯源。

---

## 三、双层模糊/矛盾指令门禁（`ambiguity_gate.py` + `clarifications.py`）

1. **第一层（确定性本地前置门禁）**：在启动任何模型前，Python 直接拦截明显自相矛盾的指令（如同时要求“只读不改”与“直接修改提交”、同时要求“仅面板静默”与“发到群里”）、多模型冲突、目标外发群歧义、近期多份候选文件歧义。
2. **第二层（结构化反猜想拦截）**：要求 T3 代理在执行写操作或对外发送前通过 `FEIYU_JSON` 协议汇报清晰度检查；即便模型误报 `COMPLETED`，只要 `unconfirmed_critical_assumptions` 非空，桥接仍会强制拦截转为 `ASK`。
3. **群任务追问私聊隔离**：在群内由主人发起的任务一旦触发追问（`clarify:*`）、确认回执或超时提醒，统一改发到主人与大肥鱼的私聊窗口（附带来源群、任务编号与 Agent ID），避免在业务群内刷屏；主人私聊答复后，最终办结结果仍干净发回原群。

---

## 四、本地 Agent 主动跨线程发问（`agent_relay.py`）

任何正在运行的本地 T3 Agent 如需向主人确认关键决策，可直接调用：

```powershell
python bridge/private_bot_control.py ask --thread-id <T3_THREAD_ID> --text-file prompt.txt --title "简短主题"
```

- 系统自动为同一 `threadId` 分配或复用短会话码 `A-XXXX`，在消息末尾追加固定回接尾标（超长截断时仍保留），并写入 `business.sqlite3`（`agent_outbound_asks`）。
- 主人在私聊中直接**引用**该消息回复，或在正文中带 `A-XXXX` / `【回复 A-XXXX】` / `threadId: <id>`，桥接仅校验本人稳定 `userid`，**0 模型调用**直接通过 `t3 thread send` 将回复与被引用摘要回传至原 T3 线程继续执行。

---

## 五、全量 T3 静默配额接力（`t3_quota_relay.py`）

- 调度器每 30 秒巡检活跃项目，识别因 Provider 配额耗尽而失败的轮次。
- 自动依据可用模型池选择接棒模型与推理档位，继承原项目工作区、运行权限与交互模式，携带交接检查点（`checkpoint_path`）启动接棒线程。
- 私聊的 `A-XXXX` 映射与肥鱼事项的最终回执自动指向接棒线程。
- 内置 `quota_relay_max_hops`（默认 2 棒）、`quota_relay_daily_cap`（默认每日 24 次）与 `quota_relay_recency_seconds`（默认 1800 秒）防止无限循环接力。

---

## 六、大视频保原画策略（`private_files.py`）

- 默认交付视频保留原始清晰度（`allow_lossy_video_compress=False`），不把超限原片强行压糊。
- 超过企业微信 Bot 单文件上限（19.5 MB）时，支持两种分级策略：
  1. 保留本地原片路径，交由受控外部渠道投递至配置的目标验收群（环境变量 `FEIYU_LARGE_VIDEO_GROUP`）；
  2. 按完整展示段落预拆分为单段 `<= 19 MB` 的片段经 `private_bot` 交付，不拆断同一连续镜头。

---

## 七、四层高可用自愈与平滑重载

| 层级 | 组件 | 职责 |
| :--- | :--- | :--- |
| **Layer 1** | `bridge_daemon.py` | 单实例业务与调度守护进程；滚动日志 `bridge-daemon.log`；反向巡检双 Bot 接收器与 Watchdog |
| **Layer 2** | `watchdog.py` | 全栈独立健康守护；每秒查心跳、每 5 秒查 `queue/` 滞留 >60s 与回执卡死；假活 >45s 校验进程身份后重启 |
| **Layer 3** | `WecomBridge.cs` | 原生 Windows WinForms 系统托盘与状态面板；持续异常 25s 自动退避恢复；接收 `recovery.request` 实现脱离 Windows Job Object 的安全委托拉起 |
| **Layer 4** | `health_guard.py` | 配合 Windows 计划任务每 3 分钟静默巡检并兜底拉起所有缺失/僵死组件（含托盘自身） |
| **升级通道** | `t3_self_heal.py` | 任意核心组件连续重启 `>= 2` 次或异常持续 `>= 180s` 时，绕过 `bridge_daemon` 直启 T3 诊断线程（10 分钟全局冷却、30 分钟同签名冷却、每日 12 次熔断） |

### 无损重载守护进程步骤

1. 读取运行状态，优先选择无活跃重槽 worker 的窗口。
2. 将当前守护 PID 写入 `daemon-reload.request` 的 `pid` 字段，让旧守护优雅退出（正在运行的 `t3_dispatch.py job` 由其独立子进程完成）。
3. 确认旧 PID 退出后，由托盘或 `bridge_daemon.py start` 拉起单个新守护，并核查 `local-dispatch-status.json` 的心跳与槽位配置。
