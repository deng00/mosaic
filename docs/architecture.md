# Mosaic — 架构设计

记录 Mosaic 的关键设计取舍**为什么**长这样。读完后你应该能判断：某个改动是否符合既定约束、某个新需求适不适合塞进这套架构。

## 一句话定位

Mosaic 是一个**自托管、E2E 加密、多 agent 协作工作台**：把多个 AI agent 包成 Matrix bot 用户，跑在你自己的 Synapse 上，让你像用 Slack 一样跟它们工作——但服务器是你的、消息是端到端加密的。

## 整体形状

```
你的笔记本                   服务器（可同一台）
┌──────────────┐            ┌──────────────────┐
│ Element      │ ───────►   │ Synapse          │
│ (Web/Mobile) │  E2EE      │ (Matrix server)  │
└──────────────┘            └──────────────────┘
                                     ▲
                                     │ E2EE 客户端连接
                                     │
                            ┌──────────────────┐
                            │ Mosaic daemon    │ ← 这是本项目
                            │ (Go, 单进程)     │
                            │  ├ Cindy session │ → 子进程: claude / codex / ...
                            │  ├ Alice session │ → 子进程: claude / codex / ...
                            │  └ Bob session   │ → 子进程: claude / codex / ...
                            └──────────────────┘
```

Mosaic 是一个 **Matrix 客户端守护进程**——技术地位跟 Element 一样，只是没有界面，而是驱动 AI subprocess。每个 agent 是一个独立的 Matrix 账号，有自己的密钥、自己的 device。

## 为什么走 Matrix 而不是 Slack / RC / Mattermost

硬需求：**E2EE + 自托管**两个都要。

| 选项 | E2EE | 自托管 | 结论 |
|---|---|---|---|
| Matrix（Synapse + Element） | Olm/Megolm，审计成熟 | ✅ | **选了** |
| Rocket.Chat | 后加的、很弱 | ✅ | 排除 |
| Mattermost | 无 | ✅ | 排除 |
| Slack | 无 | ❌ | 不可能 |

E2EE 有个连带要求：**orchestrator 不能跑在服务端**——服务端只看到密文。所以 Mosaic 必须是客户端进程，跟着密钥一起跑在你信任的机器上。这跟 [happy](https://github.com/slopus/happy)、[slock](https://github.com/slock-ai) 走的是同一条路，原因相同。

## 三层 Matrix 层级

```
Org Space         例: "CoinSummer", "Personal"
└── Project Space 例: "cs-argus-agent"
    └── Topic Room 例: "test", "feat-acl-rewrite"
```

- **Org Space**：组织/个人维度，纯组织作用，Mosaic 不读取
- **Project Space**：项目维度。**Mosaic 在这一层挂共享配置**——cwd、model、memory（PROJECT.md / DECISIONS.md / SUMMARY.md）。同一 Project Space 下所有 room 继承这些
- **Topic Room**：一个对话线程 / 子任务。永久存在，做完了 `/archive`

agent 只解析房间的**直接父 Space** 作为项目，不向上爬整个链。这是个开放设计点（见末尾）。

### 自动初始化新项目

当 bot 自动加入一个 `m.space.child`，且目标本身也是一个 Space（即"父 Space 下新建的子 Space = 新项目"）时，bridge 会触发 `EnsureProject`（按 Space ID 幂等插入），并由第一个赢得 race 的 agent 在新 Space 里建出默认 rooms：`git` / `deploy` / `bugs` / `feature` / `test`（见 `pkg/agent/agent.go` 的 `defaultProjectRooms`）。直接邀请到顶层 Space 不会触发——只有 auto-join-from-parent 这条路。

## 数据模型

```
~/.mosaic/                                  ← XDG 风格 home
├── config.yaml                             ← agent/project/room 配置 + secret
├── agent.log                               ← launchd stdout/stderr
└── data/
    ├── agents/<agent-id>/                  ← per-agent 状态（PRIVATE）
    │   ├── crypto.db, pickle.key           Matrix E2E (olm/megolm + cross-signing)
    │   ├── sessions.json                   {sessions: room→sid, archived: room→bool}
    │   └── MEMORY.md                       人设 / 风格 / 角色
    └── projects/<spaceID>/                 ← cross-agent SHARED
        ├── PROJECT.md                      项目事实（架构、依赖）
        ├── DECISIONS.md                    决策日志
        └── rooms/<roomID>/SUMMARY.md       `/compact` 输出（跨 agent 共享，
                                            Cindy 做的 /compact, Alice 看得见）
```

`agents/` vs `projects/` 的拆分是刻意的：

- **身份 (identity) 是每个 agent 私有的**
- **项目记忆 (memory) 是 agent 之间共享的**——多 agent 协作时上下文一致

`SUMMARY.md` 是 Mosaic 唯一**自己写**的文件；其他都用户/外部维护。

## 单进程 × 多账号

每个 agent 是一个 `mautrix.Client` 实例，各自带 `crypto.db`、`pickle.key`、`MEMORY.md`、Matrix `device_id`。它们都在同一个 Go 进程里，共享 `*FileConfig` 和 `AgentRuntime`。

**理由**：

- **Cross-signing** 状态是 per-account 的；单进程能干净地持有 N 个独立 crypto store
- **项目级 memory 共享**——单次读取所有 agent 共用
- **`/agent new` 热加**：通过 shared secret 在 Synapse 注册新 bot 用户，追加 config，spawn goroutine，**无需重启**

**接受的代价**：进程崩了所有 agent 一起挂。launchd 的 `KeepAlive=true` 10s 内自动重起；对话通过 `--resume <sid>` 从 `sessions.json` 续上。

## 并发模型：per-room 串行 inbox

```
handleMessage → enqueue (channel, buffer 32) → dispatchLoop → runTurn
```

同一个房间的消息**必须串行处理**。两个并行 `runTurn` 会一起 drain 同一个 `proc.Events()` channel，输出会错乱。Dispatch loop 是 per-room 的，所以**不同房间天然并行**（各自有独立的 claude 子进程）。

## 多 agent 协作：mention 路由

单 agent 房间：**broadcast 模式**——所有消息都响应。

多 agent 房间：**按 @-mention 路由**——只回复显式 @ 自己的消息（认 m.mentions pill 也认手敲 `@localpart`）。peer agent 之间的消息只认协议级 m.mentions（手敲会无限套娃）。没 @ 任何 agent 的消息一律静默——避免多 bot 抢答。

实现上：bridge 在 handleMessage 入口 `shouldIgnoreForRouting` 决定 skip/reply；自动写 `m.mentions` 到 outbound 消息，让 peer router 信任。

## 消息渲染：每个 content block 一条 Matrix 消息

模型流式产出的每个 content block 独立成消息：

- **text block** → 流式 `m.room.message`，200ms 节流 edit
- **tool_use block** → 静态一行（`pkg/agent/format.go` 里 per-tool 美化）
- **tool_result** → 整条静默（成功不打扰，错误也不显示——agent 下一轮自己解释）
- **image** → `m.image`（房间加密时走 `EncryptedFileInfo`）

> 早期试过"一个 turn 一个大气泡持续 edit"——结果"已修改"跑到顶部，timeline 乱套。改成 per-block-per-message 自然按时间顺序排列，跟 Element UI 也最契合。

Markdown → HTML 走 goldmark（GFM 扩展），填到 `formatted_body`，Element 渲染表格 / 代码块 / 列表都正确。

## `<ask_user>` 协议 → Matrix poll

Agent 想让用户多选时，输出一段约定 envelope：

```
<ask_user>
今晚吃什么？
- 火锅
- 日料
- 烧烤
</ask_user>
```

bridge 解析后**不发原文**，发一个 `org.matrix.msc3381.poll.start` 事件——Element 原生渲染成点击投票卡片。用户点选项 → `m.poll.response` 回来 → bridge 关闭 poll（`m.poll.end`）→ 把选项作为新 user turn 注入。

**为什么用 envelope 而不是 Claude SDK 的 `AskUserQuestion` tool**：那个 tool 是 SDK 内置的，headless 模式下 SDK 直接 fail，我们没法 fulfill。envelope 法对 runtime 完全透明。

**兼容**：如果 model 还是调了 `AskUserQuestion` tool，bridge 拦截渲染同样的 poll UI（损失一回合在失败的 tool_result 上，但 UX 不坏）。

## Slash command

两个前缀都识：`/foo` 和 `!foo`。Element web 看到不认识的 `/` 命令会弹"Unknown Command"警告，所以 `!` 是无噪声替代。Matrix 协议**没有**服务端注册 slash command 的机制。

```
通用：    /help  /status  /new-session  /compact  /archive  /unarchive
Agent：   /agent help|list|new|allow|revoke|members
Project： /project help|status|list|list-all|cwd|name|new
```

`/agent new` 和 `/project cwd|name` **需要 admin**（config 里 `admins:` 列表）。

`/agent new` 接受多行 body：

```
/agent new alice
name: Alice
description: 你是 Alice，code reviewer。
model: sonnet
```

它通过 Synapse `shared_secret_registration` 注册新 Matrix 用户（不需要 admin token，用 `registration_shared_secret` 即可），生成随机密码，追加 config，建好 MEMORY.md 模板，进程内 spawn agent——全程无重启。

## 显示折叠：`server_name` 后缀

Matrix ID 全局唯一：`!abc:server`、`@user:server`。`:server` 在协议层是强制的（federation 路由要）。但对**本 homeserver** 的 ID 来说，每条消息都带它是噪声。Mosaic 折叠：

```
config: server_name: localhost
显示:  !HubAKxod...:localhost  →  !HubAKxod...
显示:  @cindy:localhost         →  @cindy
显示:  @bob:matrix.org           →  @bob:matrix.org   (federation, 保留)
```

`server_name` 未设时从 homeserver URL 推；但 `127.0.0.1` 之类的 URL host ≠ Synapse 的 `server_name`（往往是 `localhost`），需要在 config 显式设。

## 上下文 / memory 生命周期

每次 fresh session，三层堆叠塞进 `--append-system-prompt`：

1. `data/agents/<id>/MEMORY.md` —— agent 身份（人设）
2. `data/projects/<spaceID>/PROJECT.md` + `DECISIONS.md` —— 项目共享事实
3. `data/projects/<spaceID>/rooms/<roomID>/SUMMARY.md` —— 上一次 `/compact` 的总结

当一个 room 的对话变长，用户跑 `/compact`：

1. Mosaic 注入一条合成 user message（`compactPrompt` "把上面对话总结成 markdown..."）
2. claude 流式输出 markdown 总结（用户也能看到）
3. Mosaic 抓最终 assistant text，写到 SUMMARY.md（tmp + rename 原子）
4. 结束 session（清内存 `roomSession`，丢 sessions.json 里的 resume id）
5. 下一条 user message → fresh claude session，SUMMARY.md 作为"早先对话总结"重新注入

净效果：**room 上下文有界**。长存房间任意次 `/compact` 后仍然行为正常。

## 失败模式与恢复

| 失败 | 检测 | 恢复 |
|---|---|---|
| Claude 子进程在 turn 之间死了 | `proc.Send` returns "file already closed" | evictSession + getOrCreate (用 `--resume <sid>` 重 spawn) + 重试一次 |
| Claude stdin/stdout EOF 中途 | `Events()` channel closes | evictSession + 提示用户重发；对话记忆通过 sessions.json 保留 |
| Claude `result.subtype = error_*` | result event | 翻译成人话（限流 / max turns / max tokens），**不自动重试**（防 token 烧光） |
| Synapse 限流登录 | agent 启动时 `M_LIMIT_EXCEEDED` | `rc_login` 在 homeserver.yaml 调宽 |
| Restricted-room auto-join 被拒 | `m.space.child` autojoin 时 `M_FORBIDDEN` | 已知 Synapse bug；fallback 手动 invite |
| agent pickle key 丢了 | crypto.db 读不出 | 灾难性——E2E 历史完蛋；首次 cross-signing bootstrap 时打印的 SSSS recovery key 能救 |

## 典型重要约定

- **config 里的路径**可以用 `~` / `~/`。`expandHome()` 在 `resolve()` 末尾跑，所以传给 claude 的总是绝对路径。**不把展开后的路径写回 config**（每次重启重 resolve，方便跨机器）
- **不要往 `data/projects/<spaceID>/PROJECT.md` 写**——用户管理的。只有 `SUMMARY.md` 是 agent-managed
- **goolm，不是 libolm**。build 时 `-tags goolm`。libolm 上游已弃，goolm 是 mautrix-go 的纯 Go 移植
- **config.yaml 写入要原子**：marshal 完整 FileConfig，写 `path.tmp`，rename。用户可能正用编辑器开着它
- **agent 行为改动的运行顺序**：改代码 → `make build` → `launchctl kickstart -k gui/$(id -u)/com.danny0.mosaic` → tail `~/.mosaic/agent.log` → 在 Element 里戳

## 开放设计点

- **Org 级 cwd 继承**：目前只看 room 的直接父 Space。Org Space → Project Space 嵌套继承没实现。`resolve()` 加个父链 walk 就能做
- **多机部署 ("COMPUTER")**：单机为主。要多机的话，daemon 得 (a) 通过 sync layer 联邦（多 mosaic 实例共享 config），或者 (b) 远程 shell 驱动
- **Restricted-room auto-join bug**：Synapse 1.152 即使 bot 是 Space 成员也拒绝授权。疑 Synapse bug。workaround 是手动 invite，值得上报上游
- **per-agent runtime 切换**：`runtime` 字段已在 `BotConfig` / `CreateRequest` 占位（stub）；切 Claude Code → OpenCode → Codex → 自写 SDK runner 的开关。还没接通
- **环境变量注入**：`BotConfig.Claude.Env` 已有，但 UI 编辑（slock 风格）没做

## 技术栈

- **Go 1.22+**，单进程多账号
- **[mautrix-go](https://maunium.net/go/mautrix)** Matrix 客户端 + **`goolm`** 纯 Go olm 加密
- **[Claude Code](https://claude.com/claude-code)** / **[OpenAI Codex CLI](https://github.com/openai/codex)** 作为可插拔 runtime
- **goldmark** Markdown → HTML（GFM 扩展）
- **launchd** / systemd 监管，崩了自动起

要看代码入口：

- `main.go` — 配置加载 + 多 agent goroutine 起飞
- `pkg/agent/agent.go` — bridge 核心，handleMessage / dispatchLoop / consume
- `pkg/runtime/` — 通用 Process/Event 接口 + claude/codex driver
- `pkg/matrix/client.go` — 对 mautrix 的 thin wrapper（SendText / SendPollStart / SendImage / Reactions / ...）
- `pkg/claude/streamjson/` — stream-json NDJSON I/O 原语

## 一行总结

**E2E 加密的 Matrix 之上，单进程多账号 Go 守护进程，把 Claude Code 子进程驱动成跨房间协作的 AI 同事。**
