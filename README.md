# Mosaic

> Self-hosted, end-to-end-encrypted, multi-agent collaboration workspace
> built on top of [Matrix](https://matrix.org/). Slack-like UX where each
> "person" can be a human or an AI agent (Claude Code et al), running
> entirely on your own homeserver.

```
你 ──▶ Element web/desktop/mobile
         │  E2E (Olm/Megolm)
         ▼
    Synapse (Matrix homeserver, 自托管)
         │  E2E
         ▼
    mosaic daemon  ─┬─▶ @cindy:你的-server   (agent: persona = onboarding lead)
                    ├─▶ @alice:你的-server   (agent: persona = code reviewer)
                    └─▶ @claude-bot:..       ...
                       每个 agent 各自跑一个 Claude Code subprocess
```

## 设计速记

- **三层 Matrix 结构**：Org Space (`CoinSummer`) → Project Space (`cs-argus-agent`) → Topic Room (`feat-acl-rewrite` / `daily-ops`)
- **每个 agent 有独立人格**：`data/agents/<id>/MEMORY.md` 是 agent 的 role / persona 文件
- **Project memory 跨 agent 共享**：`data/projects/<spaceID>/PROJECT.md`、`SUMMARY.md`，谁 `/compact` 写入，全员下次 session 自动注入
- **Slash 命令 chat-driven 管理**：`/agent new`、`/project set-cwd`，配置即时生效，不重启
- **真 E2EE**：Synapse 全程只见密文；orchestrator 必须在持密钥的客户端（即 mosaic 进程本身），所以我们不是 server-side bot

详细架构 / 决策记录 → [`CLAUDE.md`](CLAUDE.md)。

## 快速开始

### 0. 先决条件

- Go 1.22+
- [Claude Code](https://claude.com/claude-code) CLI 在 PATH 上
- Docker（用于 Synapse + Postgres + Element）
- macOS / Linux

### 1. 起 Synapse + Element

参考 docker-compose（**仓库外自建**）：

```yaml
services:
  postgres: { image: postgres:16-alpine, ... }
  synapse:  { image: matrixdotorg/synapse:latest, ports: ["0.0.0.0:8008:8008"] }
  element:  { image: vectorim/element-web:latest, ports: ["0.0.0.0:8080:80"] }
```

关键 Synapse 配置（`homeserver.yaml`）：

```yaml
server_name: "localhost"           # 或你的真域名
encryption_enabled_by_default_for_room_type: all
enable_registration: false
registration_shared_secret: "<复制到 mosaic 的 config.yaml>"
rc_login:                           # 放宽 dev 限速
  address: { per_second: 1, burst_count: 30 }
```

### 2. 注册第一个 admin 用户和第一个 agent 账号

```bash
docker compose exec synapse register_new_matrix_user \
  -u danny -p test1234 -a \
  -c /data/homeserver.yaml http://localhost:8008

docker compose exec synapse register_new_matrix_user \
  -u claude-bot -p bot1234 --no-admin \
  -c /data/homeserver.yaml http://localhost:8008
```

### 3. Build mosaic

```bash
git clone git@github.com:deng00/mosaic.git
cd mosaic
make build
```

输出：`./mosaic`、`./testmsg`、`./readroom`（后两个是 dev CLI，不必装）。

### 4. 配置

```bash
mkdir -p ~/.mosaic
$EDITOR ~/.mosaic/config.yaml
```

最小配置：

```yaml
homeserver: http://127.0.0.1:8008
server_name: localhost
data_dir: data                     # 相对 ~/.mosaic 解析

admins:
  - "@danny:localhost"

# 从 Synapse 的 homeserver.yaml 拷过来
registration_shared_secret: "..."

agents:
  - id: claude-bot
    user: claude-bot
    password: bot1234
    device_name: claude-bot
    auto_join_space_children: true
    claude:
      cwd: ~/Code
      permission_mode: bypassPermissions

projects: {}    # 用 /project set-cwd 在聊天里加，不必手填
rooms: {}
```

### 5. 跑

```bash
./mosaic
```

或用 launchd 让它常驻：

```xml
<!-- ~/Library/LaunchAgents/com.you.mosaic.plist -->
<plist version="1.0"><dict>
  <key>Label</key><string>com.you.mosaic</string>
  <key>ProgramArguments</key><array>
    <string>/path/to/mosaic</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/you/.mosaic</string>
  <key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>/Users/you/.mosaic/agent.log</string>
  <key>StandardErrorPath</key><string>/Users/you/.mosaic/agent.log</string>
</dict></plist>
```

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.you.mosaic.plist
```

### 6. 用

1. 打开 `http://localhost:8080`，用 `danny` / `test1234` 登录 Element
2. **第一次会让你创建 Security Key/Phrase**（SSSS）—— 一定保存好
3. 创建 Space `Personal`、子 Space `my-project`、子 Space 里建 room `daily`
4. 邀请 `@claude-bot:localhost` 进 room
5. 发消息——bot 立刻流式回复
6. room 里发 `!project set-cwd ~/Code/my-project` —— 写入 config，下次新 session 自动用此 cwd

## 命令参考

任意命令都支持 `/` 或 `!` 前缀（`!` 不会触发 Element 的 "Unknown Command" 警告）：

### 通用

| 命令 | 行为 |
|---|---|
| `/help` | 命令总览 |
| `/status` | 当前 room 的 session id / project / cwd |
| `/new-session` | 抛弃当前会话，下条消息起新 claude session |
| `/compact` | 摘要存盘（写 SUMMARY.md），新 session 自动继承摘要 |
| `/archive` / `/unarchive` | 暂停 / 恢复本 room 的 bot 响应；memory 保留 |

### Agent 管理

| 命令 | 行为 |
|---|---|
| `/agent list` | 表格列出所有 agent + 在线状态 |
| `/agent new <localpart>` (multi-line) | 注册 + 写 config + 创建 MEMORY.md + 进程内热起 ⛔ admin |
| `/agent help` | 帮助 |

```
/agent new alice
name: Alice
description: 你是 Alice，code reviewer，专注代码质量与安全。
model: sonnet
```

### Project 管理

| 命令 | 行为 |
|---|---|
| `/project status` | 当前 room 解析的 Space + project + cwd |
| `/project list` | 当前 Space 的 project（如果有） |
| `/project list-all` | 跨所有 Space 的 project 列表（管理员看全局） |
| `/project set-cwd <path>` | 给当前 room 所属 Space 设工作目录 ⛔ admin |
| `/project name <name>` | 给当前 Space 起个可读名字 ⛔ admin |
| `/project help` | 帮助 |

## 文件分布

```
~/.mosaic/
├── config.yaml                     # 所有 agent / project / 密钥
├── agent.log                       # launchd stdout/stderr
└── data/
    ├── agents/<id>/                # 每个 agent 私有
    │   ├── crypto.db, pickle.key   # Matrix E2E
    │   ├── sessions.json           # room → claude session_id, archived 标记
    │   └── MEMORY.md               # agent 的 persona
    └── projects/<spaceID>/         # 跨 agent 共享
        ├── PROJECT.md              # 用户手写的项目事实
        ├── DECISIONS.md            # 决策日志
        └── rooms/<roomID>/
            └── SUMMARY.md          # /compact 自动写
```

## 项目代号

**Mosaic** — multiple tiles forming one picture，对应 multi-agent + multi-room 的协作图景。NCSA Mosaic（1993，第一个主流 web 浏览器）的传承光环，但已退役 30 年，无版权冲突。

详见 [`CLAUDE.md`](CLAUDE.md) 的命名讨论。

## 已知限制

- Synapse 的 `restricted` join-rule 在某些配置下拒绝 auto-auth ——
  那种 Space 内的子 room 仍需手动 invite bot。详见 CLAUDE.md。
- 单进程多 account；进程崩溃所有 agent 一起下线。launchd `KeepAlive`
  10 秒内自启；`--resume` 通过 `sessions.json` 恢复对话。
- libolm 已被上游标记 deprecated。Mosaic 用 `-tags goolm` 走纯 Go 实现；
  cgo 仍需要（sqlite 用 mattn/go-sqlite3）。

## 相关 / 参考

- [Matrix.org spec](https://spec.matrix.org/)
- [mautrix-go](https://github.com/mautrix/go) — Matrix Go SDK
- [Happy (slopus/happy)](https://github.com/slopus/happy) — Claude Code mobile mirror，Mosaic 抽离了它的 stream-json 子进程驱动
- [Slock](https://slock.ai) — multi-agent collab UI inspiration

## License

TBD
