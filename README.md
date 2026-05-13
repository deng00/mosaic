# Mosaic

> Self-hosted, end-to-end-encrypted, multi-agent collaboration workspace
> built on [Matrix](https://matrix.org/). Slack-like UX where each "person"
> can be a human or an AI agent (Claude Code et al), running entirely on
> your own homeserver.

```
你 ──▶ Element web/desktop/mobile
         │  E2E (Olm/Megolm)
         ▼
    Synapse (Matrix homeserver, 自托管)
         │  E2E
         ▼
    mosaic daemon  ─┬─▶ @cindy:server   (agent: onboarding lead)
                    ├─▶ @alice:server   (agent: code reviewer)
                    └─▶ @claude-bot:..  ...
                       每个 agent 各自跑一个 Claude Code subprocess
```

## 特性

- **多 agent 一房间协作**：`@cindy` 让 Cindy 出来，`@alice` 让 Alice review。单 agent 房间自动 broadcast
- **三层 Matrix 层级**：Org Space → Project Space → Topic Room。project 级共享 cwd / memory
- **流式输出 + 工具调用渲染**：每个 content block 独立 Matrix 消息，timeline 顺序自然
- **多选交互走 Matrix 原生 poll**：Element 直接渲染投票卡片
- **多模态**：图片双向（用户发图给 agent，agent 生成的图自动上传）
- **Slash 命令热配置**：`/agent new`、`/project cwd` 等，配置即时生效，不重启
- **会话恢复 + `/compact`**：subprocess 挂了自动 `--resume`；长对话压缩成 SUMMARY.md 跨 session 持续

## 文档

- [`docs/synapse-setup.md`](docs/synapse-setup.md) — Synapse + Postgres docker 部署，喂给 Mosaic 的字段对照表
- [`docs/architecture.md`](docs/architecture.md) — 设计取舍：为什么 Matrix、为什么单进程多账号、E2EE 约束、并发模型……
- [`CLAUDE.md`](CLAUDE.md) — 给 AI 助手看的内部约定（也可以人读）

## 快速开始

详细步骤见 [`docs/synapse-setup.md`](docs/synapse-setup.md)。最小流程：

```bash
# 1. 起 Synapse + Postgres（参考 docs/synapse-setup.md）
# 2. 创建你的管理员账号
docker compose exec synapse register_new_matrix_user \
  -u danny -p '<pw>' -a -c /data/homeserver.yaml http://localhost:8008

# 3. build mosaic
git clone git@github.com:deng00/mosaic.git
cd mosaic && make install

# 4. 写 ~/.mosaic/config.yaml（最小骨架）
cat > ~/.mosaic/config.yaml <<'EOF'
homeserver: http://127.0.0.1:8008
server_name: localhost
registration_shared_secret: "<从 homeserver.yaml 拷过来>"
admins: ["@danny:localhost"]
agents:
  - id: cindy
    user: cindy
    password: cindy-bot-pw
    display_name: Cindy
    cwd: ~/Code
    model: claude-opus-4-7
    env: { CLAUDE_CODE_OAUTH_TOKEN: sk-ant-... }
EOF

# 5. 跑
mosaic    # 或挂到 launchd / systemd

# 6. Element 登录 → 创建 Space → 邀请 @cindy:localhost → 发消息
```

## 访问控制

Matrix room invite **就是**访问门：进得了房间就能 @ agent 聊天。

`admins` 列表只控制谁能跑**管理类**命令（`/agent new`、`/project cwd`、`/export`），不影响普通对话。

## 命令速查

任何命令都支持 `/` 或 `!` 前缀（`!` 不会触发 Element 的 "Unknown command" 警告）。

| 通用 | 说明 |
|---|---|
| `/help` `/status` | 命令总览 / 当前 room 状态 |
| `/new-session` | 抛弃当前会话，下条消息起新 claude session |
| `/compact` | 摘要存盘（SUMMARY.md），新 session 自动继承 |
| `/archive` `/unarchive` | 暂停 / 恢复本 room 的 bot 响应 |

| Agent 管理 | 说明 |
|---|---|
| `/agent list` | 所有 agent + 在线状态 |
| `/agent new <localpart>` | 注册 + 写 config + 进程内热起 ⛔ admin |

| Project 管理 (⛔ admin) | 说明 |
|---|---|
| `/project status` | 当前 room 解析出的 cwd / project |
| `/project cwd <path>` | 给当前 Space 设工作目录（不存在自动创建） |
| `/project name <name>` | 给当前 Space 起人类可读名字 |
| `/project new <name>` | 在当前 Org Space 下建一个子 Project Space |

## 项目代号

**Mosaic** — multiple tiles forming one picture，对应 multi-agent + multi-room 的协作图景。NCSA Mosaic（1993，第一个主流 web 浏览器）退役 30 年，无版权冲突。

## 相关

- [Matrix.org spec](https://spec.matrix.org/)
- [mautrix-go](https://github.com/mautrix/go) — Matrix Go SDK
- [Happy (slopus/happy)](https://github.com/slopus/happy) — Claude Code mobile mirror，Mosaic 借鉴了它的 stream-json 子进程驱动
- [Slock](https://slock.ai) — multi-agent collab UI inspiration

## License

TBD
