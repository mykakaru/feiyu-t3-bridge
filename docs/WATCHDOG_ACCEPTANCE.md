# 守护进程与四层高可用自愈规范

本文档记录肥鱼桥的高可用守护架构、故障判定阈值与验收边界。

## 一、四层守护与自愈职责

1. **Layer 1 · `bridge_daemon.py`（调度与反向巡检）**
   - 单实例 Windows 命名互斥锁保护，防止多开抢队列。
   - 驱动 `business_queue`、`t3_dispatch`、`chat_feedback` 与 `t3_quota_relay`，日志自动滚动至 `bridge-daemon.log`。
   - 反向巡检 `watchdog.py`、`bridge_control.py` 与 `private_bot_control.py` 健康状态。

2. **Layer 2 · `watchdog.py`（全栈独立看门狗）**
   - 独立隐藏进程 + 命名互斥锁，不依赖模型轮询。
   - **心跳监控**：每秒检查大肥鱼接收器、克劳德接收器、`bridge_daemon` 与 `t3_dispatch` 心跳。
   - **端到端滞留监控**：每 5 秒检查 `queue/` 新消息滞留 `> 60s` 或 `chat_receipts` 卡死 `> 60s`。
   - **假活与重启策略**：心跳假活 `> 45s` 或断连/滞留 `> 60s` 时，校验进程身份后重启；重启退避按 `10s → 20s → 40s ... 300s` 指数递增。
   - **Fail-Closed 保护**：鉴权失败、被另一客户端顶号或人为执行 `stop` 时不抢占重连。

3. **Layer 3 · `WecomBridge.cs`（原生托盘面板 & 脱离 Job Object 宿主）**
   - 在独立交互会话中运行，实时展示双 Bot、T3 调度引擎、槽位与守护心跳。
   - 持续异常 25 秒自动退避恢复；承接来自受限 Windows Job Object 内 Agent 写入的 `recovery.request` 信号，通过命名事件委托安全拉起守护进程，避免命令行退出时被 Job Object 误杀。
   - 历史故障与恢复记录归档在 `watchdog-incidents/`，并在面板「历史时间线」中合并展示，不向业务群刷屏。

4. **Layer 4 · `health_guard.py`（OS 级计划任务兜底）**
   - 配合 Windows 计划任务每 3 分钟静默巡检一次，兜底拉起所有缺失或僵死的组件（包括托盘程序自身）。

5. **升级通道 · `t3_self_heal.py`（自动诊断线程）**
   - 当任意核心组件连续重启 `>= 2` 次仍未恢复，或单次异常持续 `>= 180s`（鉴权/顶号受阻 `>= 60s`）时，绕过 `bridge_daemon` 直接调用 `t3.cmd thread start`（`wait=False`）拉起 T3 自动诊断线程。
   - 内置防护：10 分钟全局冷却、30 分钟同签名冷却、活跃自愈线程互斥与每日 12 次硬熔断。

## 二、已知边界声明

- **断连不等于服务端离线重放**：`@wecom/aibot-node-sdk` 的断线重连不承诺回放断网/关机期间的离线消息；已落盘的 `queue/` 记录在进程重启后可保留并继续处理。
- **不盲目重试不确定发送**：`outbox.cjs` 在发送前先持久化 attempt；超时或未知异常标记 `failed_or_unknown`，需核验后由人工/新任务决定是否重发，避免重复刷屏。
- **整机断电或整机断网**：本机看门狗无法在无网络环境下通过同一企微通道发出远端告警。

## 三、常用运维命令

```powershell
python bridge/watchdog.py status
python bridge/watchdog.py start
python bridge/watchdog.py stop
python bridge/health_guard.py status
```
