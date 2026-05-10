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

## 给加入别人 server 的同事用

如果有人已经搭好 Synapse + Mosaic（比如你团队里有 admin 跑了一台 server），你**不必**再装一遍 Synapse 也不必跑 admin 的那个 Mosaic 进程。两条路：

### 路径 A：只用 Element 聊，**不跑 mosaic 二进制**（推荐 9 成情况）

最简单——admin 已经在他那台机器上跑着 mosaic，agents 已经在线。你做的事：

1. 找 admin 要：
   - **homeserver URL**（例：`http://192.168.10.201:8008`）
   - **你的 Matrix 账号**（admin 用 `register_new_matrix_user` 给你建一个：`@yourname:server`）
   - **初始密码**（首次登录后自己改）
2. 装 Element（[web](https://app.element.io) 或下 desktop / iOS / Android）
3. Sign in → "Edit" homeserver → 填 admin 给你的 URL → 登录
4. 第一次登录会让你**创建 Security Key/Phrase**（SSSS）—— **务必记下**，否则换设备解不出 E2E 历史
5. 让 admin 把你邀请进 Project Space 和 topic room
6. 在 room 里 @bot 或直接发消息——bot 立刻流式回复

完事。这条路你完全不需要这个 repo 的代码。

### 路径 B：跑自己的 mosaic 实例，挂自己的 agents

适合你想**有自己的 Cindy / Alice / 私人 agent，跑在你自己电脑上访问你自己的本地代码**——而不是借 admin 的那批共享 agent。

每人一个 mosaic 进程是支持的：所有 mosaic 都连到**同一个** Synapse，agents 互不重名即可（你的 agents 用 `@alice-yourname:server`，admin 的 agents 用 `@alice:server`，不冲突）。每个 agent 的 claude 子进程都跑在**那台 mosaic 进程的机器上**——所以你的 agents 操作的是**你的本地文件**，admin 的 agents 操作的是 admin 那边的文件。

#### 准备工作（一次性）

1. **从 admin 那拿到**：
   - homeserver URL
   - 你自己的 Matrix admin 账号（用来发 `/agent new` 等管理命令；不需要 Synapse server admin 权限，只需在 mosaic 的 admins 白名单里）
   - 如果想自己用 `/agent new` 创建 agent：需要 **Synapse `registration_shared_secret`**（admin 从他的 `homeserver.yaml` 拷给你；这个 secret 让你能注册新 Matrix 用户而无需 Synapse admin token）
   - 否则就让 admin 帮你预先 `register_new_matrix_user` 创建好 agent 账号，你只在自己 config.yaml 里填用户名密码即可
2. **本机要装的**：
   - Go 1.22+（仅编译时需要——admin 也可以编译好二进制直接发给你）
   - [Claude Code CLI](https://claude.com/claude-code)（你的 agent 跑的就是它）
   - macOS / Linux

#### 步骤

```bash
# 拿 binary——要么 git clone 自己 build，要么 admin 给你他编译好的
git clone git@github.com:deng00/mosaic.git
cd mosaic
make build               # 输出 ./mosaic

# 配置文件
mkdir -p ~/.mosaic
$EDITOR ~/.mosaic/config.yaml
```

`~/.mosaic/config.yaml` 最小内容（替换尖括号占位符）：

```yaml
homeserver: http://<admin-server-IP>:8008      # admin 给你的 URL
server_name: <admin 的 server_name>            # 通常是 admin 的域名/hostname
data_dir: data

admins:
  - "@<你的 Matrix user>:<server_name>"        # 用你自己的账号

# 仅当你想用 /agent new 自己注册 agent 时填
registration_shared_secret: "<admin 给的>"

agents:
  # 选项 1：admin 已帮你预注册好 agent → 直接填用户名密码
  - id: alice-mine
    user: alice-mine                            # 你的 agent 的 localpart
    password: "<admin 给的初始密码或你自己 reset 后的>"
    device_name: alice-mine
    auto_join_space_children: true
    claude:
      cwd: ~/Code                                # 你本机的代码目录
      permission_mode: bypassPermissions

  # 选项 2：还没注册的 agent → 跑 /agent new 自动创建（需要 shared_secret）

projects: {}    # 用 /project set-cwd 在聊天里加
rooms: {}
```

跑：

```bash
./mosaic
# 或用 launchd / systemd 让它常驻——参考"快速开始"那一节
```

启动后：
1. 你 Matrix 账号开 Element 跟 admin 同样登录
2. 邀请你刚配的 `@alice-mine:server` 进任意 room（admin 控制的或你自己建的 Space 里）
3. 跟它说话——claude 子进程在**你这台机器**上起，操作**你这台机器**的 `~/Code` 目录
4. admin 那边的 mosaic 进程**完全不参与**——你们俩的进程互不影响，只是用同一个 Matrix 总线

#### 与 admin 的责任边界

| 这件事 | 谁来做 |
|---|---|
| 跑 Synapse + Postgres + Element web | admin |
| 给你建 Matrix 账号 | admin（`register_new_matrix_user`） |
| 给你 admin 自己 mosaic 跑的那些 agents | admin |
| 你自己的 agents（在你电脑上的） | **你自己**跑 mosaic 维护 |
| Synapse 的 `registration_shared_secret` | admin 决定要不要给你；不给的话，agents 都得 admin 帮你 register |

注意点：
- **每个 mosaic 进程对应一台机器** —— claude 子进程的 cwd 是**那台机器**的本地路径。admin 的 mosaic 看不到你的 `~/Code`，反之亦然。
- agent 的 Matrix 账号是 **server 全局的**（同一个 homeserver），但 agent 的 claude 进程是**本地的**。这意味着如果你和 admin 都跑同名 agent (`@alice:server`)，会冲突——选独特的 localpart。
- mosaic 的 `data/agents/<id>/MEMORY.md` 是**你这台机器上**的，跟 admin 的 mosaic 那台机器上的同名 agent 数据互不可见。
- 加密密钥（`crypto.db` + `pickle.key`）也是本地的——你的 agent 在你机器上的 device 是个独立设备，admin 那边看不到你的 agent 历史消息（除非 admin 也是该 room 成员）。

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
