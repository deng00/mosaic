# Synapse 自托管最小化部署（Docker）→ 接入 Mosaic

把 Synapse + Postgres 两个容器跑起来，然后把对应字段填进 `~/.mosaic/config.yaml`。
适合本机开发或单机部署；要公网 / federation 自己加 reverse proxy + 证书。

## 1. 目录结构

```
~/synapse/
├── docker-compose.yml
├── data/              ← 持久化（homeserver.yaml、密钥、媒体）
└── pgdata/            ← Postgres 数据
```

## 2. docker-compose.yml

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: synapse-pg
    restart: unless-stopped
    environment:
      POSTGRES_USER: synapse
      POSTGRES_PASSWORD: changeme-pg-pw
      POSTGRES_DB: synapse
      POSTGRES_INITDB_ARGS: "--encoding=UTF-8 --lc-collate=C --lc-ctype=C"
    volumes:
      - ./pgdata:/var/lib/postgresql/data

  synapse:
    image: matrixdotorg/synapse:latest
    container_name: synapse
    restart: unless-stopped
    depends_on: [postgres]
    environment:
      SYNAPSE_SERVER_NAME: localhost        # ⚠ 改成你想要的 server_name
      SYNAPSE_REPORT_STATS: "no"
    volumes:
      - ./data:/data
    ports:
      - "8008:8008"                          # HTTP（客户端用这个）
```

> **server_name 关键点**：一旦定下来就不能改（federation 的事件 ID 都带它）。
> 本机玩用 `localhost`；想公网访问改成你的域名（要 reverse proxy + 证书）。

## 3. 生成初始 homeserver.yaml

```bash
docker compose run --rm synapse generate
```

会在 `data/homeserver.yaml` 写一份默认配置。

## 4. 改 `data/homeserver.yaml`

```yaml
server_name: "localhost"
public_baseurl: "http://localhost:8008"
report_stats: false
registration_shared_secret: "随便一长串_base64_或_uuid_要保密"  # ⭐ Mosaic 要用
enable_registration: false                                      # 走 shared_secret 创建账号
suppress_key_server_warning: true

database:
  name: psycopg2
  args:
    user: synapse
    password: changeme-pg-pw
    database: synapse
    host: postgres
    cp_min: 5
    cp_max: 10

# 房间默认开 E2EE（Mosaic 必须）
encryption_enabled_by_default_for_room_type: all

# agent 重启会频繁 login —— 放宽限流，避免 M_LIMIT_EXCEEDED
rc_login:
  address:
    per_second: 1000
    burst_count: 1000
  account:
    per_second: 1000
    burst_count: 1000
  failed_attempts:
    per_second: 1000
    burst_count: 1000

# 大消息（多模态 base64 图）够用
max_upload_size: "100M"
```

启动：

```bash
docker compose up -d
docker compose logs -f synapse   # 看跑起来
```

## 5. 创建你自己（管理员）账号

```bash
docker compose exec synapse register_new_matrix_user \
  -c /data/homeserver.yaml \
  -u danny -p '你的密码' -a \
  http://localhost:8008
```

`-a` = admin。这个账号你在 Element 登录。

## 6. 配 Element 登录

打开 https://app.element.io（或自己跑 element-web）。
homeserver 手动选 `http://localhost:8008`，登 `@danny:localhost`。

## 7. 喂给 Mosaic 的信息 ⭐

`~/.mosaic/config.yaml`：

```yaml
homeserver: http://127.0.0.1:8008        # ① docker 暴露的端口
server_name: localhost                   # ② 跟 homeserver.yaml 一致
data_dir: ./data

admins:
  - "@danny:localhost"                   # ③ 你刚创建的管理员

# ④ 从 homeserver.yaml 复制过来 —— /agent new 需要这个来注册新 bot 账号
registration_shared_secret: "随便一长串_base64_或_uuid_要保密"

agents:
  - id: cindy
    user: cindy                           # ⑤ Mosaic 启动时会自动用 shared_secret
    password: cindy-bot-pw                #    注册这个 bot 账号（首次启动）
    display_name: Cindy
    cwd: ~/Code
    model: claude-opus-4-7
    runtime: claude
    env:
      CLAUDE_CODE_OAUTH_TOKEN: sk-ant-...
```

### 关键字段对应表

| Mosaic 配置 | 来自 Synapse |
|---|---|
| `homeserver` | `docker-compose.yml` 里的 `8008:8008` 端口映射 |
| `server_name` | `homeserver.yaml` 里的 `server_name` 字段 |
| `registration_shared_secret` | `homeserver.yaml` 里的同名字段（**整段照搬**） |
| `admins[]` | step 5 创建的 `@user:server_name` |
| `agents[].user` / `password` | 自己起名+密码；Mosaic 首启时用 shared_secret 替你注册 |

## 8. 起 Mosaic + 测试

```bash
make install
launchctl load ~/Library/LaunchAgents/com.danny0.mosaic.plist
tail -f ~/.mosaic/agent.log
```

在 Element 里：

1. 创建 Space "MyOrg" → 在里面创建子 Space "Project-X" → 在 Project-X 里建 room "test"
2. 邀请 `@cindy:localhost` 加入 test room（直接 `/invite @cindy:localhost`）
3. 发一句 `hi`，cindy 应该回

## 9. 常见坑

- **Element 提示房间 "Unable to decrypt"**：bot 没拿到 megolm key。让 bot 重启一次，或者用 `/discardsession` 重新发消息
- **`M_FORBIDDEN` on auto-join**：Space restricted-room 邀请 bot 的已知 Synapse bug，手动 invite 一次就行
- **Synapse 启动报数据库连接失败**：检查 `database.args.host: postgres`（service 名），不是 `localhost`
- **server_name 后悔了**：删 `data/` + `pgdata/` 重做。改不了。

## 10. 升级 / 备份要点

- 升级 Synapse：`docker compose pull synapse && docker compose up -d`。重大版本前先看 [upgrade notes](https://element-hq.github.io/synapse/latest/upgrade.html)
- 备份：`data/` + `pgdata/` 整个 tar 起来即可；keystore（`signing.key`、`*.signing.key`）丢了房间历史不可恢复
- 公网部署：在前面套 nginx/caddy 做 HTTPS，开 `8448` 给 federation（如果要联邦）
