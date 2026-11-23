# 本机复现指南（面向开发者 & Coding Agent）

这份文档专门写给**你自己换机重装**或**把仓库发给其他人的 Coding Agent 在其本地 Windows 环境 1:1 复现肥鱼桥**使用。

---

## 一、前置环境要求

| 依赖 | 最低版本 | 说明 |
| :--- | :--- | :--- |
| **操作系统** | Windows 10 / 11 (x64) | 凭据加密与媒体密封使用 Windows Current-User DPAPI (`CryptProtectData`) |
| **Python** | 3.11+（推荐 3.13 + `uv`） | 驱动调度、门禁、看门狗、配额接力与全部单测 |
| **Node.js** | 20+ | 运行企业微信官方长连接接收器 `wecom_receiver.cjs` / `private_bot_receiver.cjs` |
| **企业微信官方 SDK** | `@wecom/aibot-node-sdk@1.0.6` | 通过 npm 安装至运行目录 |
| **T3 Code** | 0.1.11+ | 需本机已安装并保证 `t3.cmd` CLI 可用、至少一个 Provider 已登录 |
| **.NET Framework `csc.exe`** | Windows 自带 4.x | 可选，用于将 `bridge/WecomBridge.cs` 编译为原生托盘程序 `WecomBridge.exe` |

---

## 二、目录初始化与依赖安装

假设你的本地运行目录为 `C:\Users\<YOU>\WecomBridge`（**建议运行目录与 Git 源码仓库分开，避免运行态数据库/凭据混入 Git**）：

```powershell
# 1. 复制核心源码与依赖清单到运行目录
New-Item -ItemType Directory -Force "$env:USERPROFILE\WecomBridge"
Copy-Item -Path ".\bridge\*" -Destination "$env:USERPROFILE\WecomBridge\" -Force
Copy-Item -Path ".\config\package.example.json" -Destination "$env:USERPROFILE\WecomBridge\package.json"

# 2. 安装企业微信官方 AI Bot SDK
Set-Location "$env:USERPROFILE\WecomBridge"
npm install
```

---

## 三、准备配置文件（从模板复制并填入你自己的值）

将 `config/*.example.json` 复制到运行目录并去掉 `.example` 后缀：

1. **`t3-bridge-config.json`**（复制自 `config/t3-bridge-config.example.json`）
   - 在 T3 Code 中创建一个专门隔离用的 Project（例如 `FeiyuTasks`，指向一个独立空目录如 `C:\Users\<YOU>\Documents\WecomTasks`）。
   - 将该项目的 `projectId` 填入 `t3_project_id`。
   - 将 `t3_home`、`t3_cli`、`tasks_root` 改为你本机的实际路径。
   - 根据你本机已登录的 Provider，调整 `entry_model`（轻量入口模型）和 `escalation_model`（重槽接力模型）。
2. **`identity_mapping.json`**（复制自 `config/identity_mapping.example.json`）
   - 启动接收器后，用本人企业微信给 Bot 发一条私聊测试消息，从 `queue/` 落盘记录中读取你的稳定 `sender`（即 WebSocket `userid`）。
   - 将该精确 ID 填入 `owner_ws_userids` 数组。**未配置前系统默认 Fail-Closed，不会执行任何指令。**
3. **`routing_settings.json`**（复制自 `config/routing_settings.example.json`）
   - 配置默认个人/群只读模型偏好。

---

## 四、配置双 Bot 企微长连接凭据（DPAPI 加密，不落明文）

在企业微信管理后台创建 1 个或 2 个支持长连接模式的智能机器人，分别取得 `BotID` 与 `Secret`：

- **大肥鱼（群管理 Bot）**：使用 `bridge_control.py` 对应的 DPAPI 凭据文件（`receiver-credentials.dpapi`，UTF-8 JSON `{"bot_id": "...", "secret": "..."}` 经 Windows `CryptProtectData` 加密后写入，严禁明文保存）。
- **Claude（单人私聊远控 Bot）**：使用 `private_bot_control.py` 对应的独立 DPAPI 凭据文件（`private-bot-credentials.dpapi`，同样格式经 DPAPI 加密）。

> 提示：可用 `bridge/bridge_control.py` 中的 `protect(payload_bytes, decrypt=False)` 生成当前用户绑定的 `.dpapi` 文件。

---

## 五、可选环境变量清单

源码已将所有本机路径与可定制项改为环境变量优先、合理默认兜底：

| 环境变量 | 用途 | 默认值 |
| :--- | :--- | :--- |
| `WECOM_SDK_PACKAGE_JSON` | 指定 `@wecom/aibot-node-sdk` 所在的 `package.json` 路径 | 运行目录下的 `package.json` |
| `FEIYU_NODE_EXE` | 指定 `node.exe` 绝对路径 | 系统 `PATH` 中的 `node` |
| `FEIYU_PYTHON_EXE` | 供 `sealed_media.cjs` 调用的 Python 路径 | `python` |
| `FEIYU_OWNER_WS_USERID` | `scoped_media.cjs` 绑定本人私聊文件下载校验的稳定 ID | 空（Fail-Closed） |
| `FEIYU_EXACT_MEDIA_HOST` | 企业微信媒体下载精确 COS 域名白名单 | 示例占位域名（按实测回包 host 配置） |
| `FEIYU_DEFAULT_GROUP_CHATID` | `group_replies.py` 默认单群上下文 `chatid` | `wrk_example_default_group` |
| `FEIYU_LARGE_VIDEO_GROUP` | 超 19.5 MB 大视频原画兜底目标群名称 | `【视频验收群】` |
| `WECOM_CLI_SCRIPT` | 可选：本地 `wecom-cli` 封装脚本路径（用于查询群名列表） | `~/.codex/skills/wecom-unified/scripts/run_cli.py` |
| `FEIYU_P4_WORKSPACE` / `FEIYU_P4_PROJECT_SUBPATH` / `FEIYU_P4_CLIENT` / `FEIYU_P4_USER` | 可选：仅当启用 `peer_pending.py` 做 Perforce 工程待审变更时配置 | 示例占位值 |

---

## 六、启动、编译托盘与配置开机守护

```powershell
# 1. 启动大肥鱼与克劳德双接收器
python bridge_control.py start
python private_bot_control.py start

# 2. 启动单实例调度守护与独立看门狗
python bridge_daemon.py start
python watchdog.py start

# 3. 检查全栈状态
python bridge_control.py status
python private_bot_control.py status
python watchdog.py status
```

### 编译原生 WinForms 托盘程序（可选）

```powershell
$csc = "C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
& $csc /target:winexe /out:WecomBridge.exe /r:System.Windows.Forms.dll /r:System.Drawing.dll WecomBridge.cs
```

### 注册 Windows 3 分钟兜底计划任务（可选）

在任务计划程序中创建当前用户登录触发、每 3 分钟静默运行一次的任务，执行：
`pythonw.exe "C:\Users\<YOU>\WecomBridge\health_guard.py" ensure`

---

## 七、离线单元测试与验收

在仓库根目录执行：

```powershell
# Python 单元测试
python -m unittest discover -s tests -p "test_*.py"

# Node.js CJS 核心收发与媒体密封单测
node tests/test.cjs
node tests/test_outbox.cjs
node tests/test_outbox_diagnostics.cjs
node tests/test_status_writer.cjs

# 开源推送前的零泄露审计门禁
python scripts/sanitize_audit.py
```

### 给 AI Agent 的快速复现指令模板

如果你想直接让 AI 帮你装，把仓库链接和下面这段话发给它即可：

> 请阅读这个仓库的 `README.md`、`docs/ARCHITECTURE.md` 和 `docs/REPRODUCE_WITH_AI.md`。先帮我把 `bridge/` 复制到我的独立本地运行目录并安装 `@wecom/aibot-node-sdk@1.0.6`，然后按步骤向我收集我的 T3 专用 Project ID、企业微信 Bot 凭据（用 Windows DPAPI 加密落盘，不要存明文）和我本人的稳定 `userid`，启动双 Bot + 守护进程并跑完 `unittest` 验收。
