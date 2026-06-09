# Mosaic — design notes for AI assistants

This file captures the **why** behind Mosaic's architecture so a future
session (yours, mine, or another agent) can reason about changes without
re-deriving every decision. Read this before proposing structural changes.

## What this project is

Mosaic is a **self-hosted, end-to-end-encrypted, multi-agent collaboration
workspace** built on top of [Matrix](https://matrix.org/) (Synapse) and
Element. Each "agent" is a Matrix bot user wrapping a CodingAgent runtime
(currently [Claude Code](https://claude.com/claude-code), pluggable later).
A user can have several agents (Cindy / Alice / a code reviewer …) with
distinct personas, all conversing in topic-rooms inside Project-Spaces.

The product analogy is "Slack with AI colleagues you actually own": same
chat-channel UX, but every "person" can be a human or an AI, and your
homeserver is yours.

## Naming

- **Mosaic** — the daemon / project codename. Each agent is a tile in
  your work-mosaic; each Space is a mosaic of related rooms. Picked over
  "Codex/Codea/Codec" (overlapped trademarks) and "agentic" (buzzword
  rot). Inherits ambient brand-credit from NCSA Mosaic (1993, the first
  mainstream web browser) — long-decommissioned, no current rights claim.
- **Agent** — user-facing term for a Matrix bot user (Cindy, Alice).
  Distinct from **CodingAgent** (Claude Code, OpenCode, …) which is the
  *runtime* under each agent. Don't conflate.
- **Project** — a Matrix Space. Holds shared cwd + memory (PROJECT.md,
  DECISIONS.md, SUMMARY.md).
- **Topic / Session / Room** — a Matrix Room inside a Project-Space.
  One conversation thread / sub-task. Lives forever; gets `/archive`'d
  when done.

## Three-layer Matrix hierarchy

```
Org Space (e.g. "CoinSummer", "Personal")
└── Project Space (e.g. "cs-argus-agent")
    └── Topic Room (e.g. "test", "feat-acl-rewrite")
```

The agent only resolves **the room's immediate parent Space** as the
project. Nested Org-Spaces are organisational only — Mosaic doesn't walk
up the chain. If you want shared cwd at the Org level, you'd configure it
on each Project-Space individually (or extend resolve to walk up — open
design point).

**Auto-init on new sub-Space**: when a bot auto-joins an `m.space.child`
whose target is itself a Space (i.e. a project Space created under an
existing Org Space), the bridge fires `EnsureProject` (insert-if-absent
keyed by Space ID, default name = the Space's `m.room.name`) and the
winning agent creates a single default topic-room named
`<project>-main` as a child (see `handleSpaceJoinedWithInvite` in
`pkg/agent/agent.go`). One room per project matches how users actually
organise — extra topic rooms get added on demand instead of populating
empty `dev`/`deploy` shells up front; this also keeps the room-creation
burst short enough to stay under Synapse's `rc_*` rate limits. Multi-agent
races are resolved by `EnsureProject` returning `created=true` exactly
once. Direct invites to a top-level Space don't trigger this — only the
auto-join-from-parent path.

## Core data model

```
~/.mosaic/                                  ← XDG-ish home (slock-style)
├── config.yaml                             ← all agent/project/room config + secrets
├── agent.log                               ← launchd stdout/stderr
└── data/
    ├── agents/<agent-id>/                  ← per-agent state (PRIVATE)
    │   ├── crypto.db, pickle.key           Matrix E2E (olm/megolm + cross-signing)
    │   ├── sessions.json                   {sessions: room→sid, archived: room→bool}
    │   └── MEMORY.md                       persona / role / style (slock-style)
    └── projects/<spaceID>/                 ← cross-agent SHARED
        ├── PROJECT.md                      project facts (architecture, deps)
        ├── DECISIONS.md                    decision log
        └── rooms/<roomID>/SUMMARY.md       /compact output (shared across agents
                                            so a /compact by Cindy gets seen by Alice)
```

The `agents/` vs `projects/` split is deliberate: *identity* is private
per agent, *project memory* is shared so multi-agent collaboration on the
same room sees the same context. SUMMARY.md is the only file Mosaic
writes itself — the others are user-curated.

## Why Matrix, not Slack/RC/Mattermost

Hard requirement: **end-to-end encryption + self-host**. The relevant
options ranked:

| Option | E2EE | Self-host | Verdict |
|---|---|---|---|
| Matrix (Synapse + Element) | Olm/Megolm, audit-mature | ✅ | **chose** |
| Rocket.Chat | Bolted-on, weak | ✅ | rejected |
| Mattermost | none | ✅ | rejected |
| Slack | none | ❌ | n/a |

E2EE has a strong implication: **the orchestrator can't sit on the
server** (it'd see only ciphertext). All agent logic must run client-side
with the keys. Mosaic is a Matrix *client* daemon, not a Matrix
*server-side bot*. This is the same architecture as Happy (slopus/happy)
and Slock — for the same reason.

## Why one process, multi-account

Each agent is a `mautrix.Client` instance with its own `crypto.db`,
`pickle.key`, `MEMORY.md`, and Matrix `device_id`. They all live in the
same Go process, sharing a `*FileConfig` and an `AgentRuntime`.
Reasons:

- **Cross-signing** state is per-account; a single process can hold
  N independent crypto stores cleanly with mautrix-go.
- **Memory shared** at the project level (single read of files per
  spawn).
- **Hot-add via /agent new** — register Synapse user via shared secret,
  append config, spawn goroutine, all without restart.

Trade-off accepted: a process crash takes all agents down. Wrap mosaic in
whatever process manager fits your OS (launchd / systemd / supervisor /
docker) with restart-on-exit enabled; conversations resume via
`--resume <sid>` from `sessions.json` after the restart.

## Per-room serial inbox

```
handleMessage → enqueue (channel, buffer 32) → dispatchLoop → runTurn
```

Multiple messages to the same room **must** serialise. Two parallel
`runTurn` goroutines on one room would race-drain the same `proc.Events()`
channel and produce scrambled output. The dispatch loop is per-room, so
*different* rooms parallelise (each has its own claude subprocess).

## Streaming / message rendering

Each Claude content block becomes its **own** Matrix message:

- text block → streamed via 200ms-throttled `m.room.message` edits
- tool_use block → static one-line message (formatted via
  `pkg/agent/format.go` per-tool prettyprinter)
- tool_result → silent on success; surfaced only on `is_error: true`

The earlier "one big edited bubble" approach scrambled the order ("已修改"
ended up at the top because text and tool_use lived in two separate
buffers). Per-block-per-message preserves chronology naturally and
matches what Element's UI shows best.

Markdown → HTML via goldmark (GFM table extension), filling
`formatted_body` so Element renders tables / code blocks / lists
properly.

## Slash commands

Two prefixes, both work: `/foo` and `!foo`. Element's web UI shows an
"Unknown Command" warning on unknown `/`-prefix commands (it doesn't
know any of ours), so `!` is the noise-free alternative.

```
General:    /help  /status  /new-session  /compact  /archive  /unarchive
Agent mgmt: /agent help|list|new
Project:    /project help|status|list|list-all|cwd|name
```

`/agent new` and `/project cwd|name` are **admin-gated** (config
`admins:` list of full Matrix user IDs).

`/agent new` accepts a slock-style multi-line body:

```
/agent new alice
name: Alice
description: 你是 Alice，code reviewer。
model: sonnet
```

It registers the Matrix user via Synapse's
`shared_secret_registration` (no admin token needed — uses the
`registration_shared_secret` from homeserver.yaml stored in our config),
generates a random password, appends to config.yaml, drops a MEMORY.md
template prefilled with `description`, and spawns the agent in-process.

## Display folding (server_name suffix)

Matrix IDs are **globally unique**: `!abc:server`, `@user:server`. The
`:server` part is mandatory at the protocol layer (federation routing).
But it's noise when displaying IDs that belong to *our own* homeserver.
We fold:

```
config: server_name: localhost
display: !HubAKxod...:localhost  →  !HubAKxod...
display: @cindy:localhost         →  @cindy
display: @bob:matrix.org           →  @bob:matrix.org   (federation, kept)
```

`server_name` defaults to URL-host parse if unset, but for `127.0.0.1`-style
URLs the actual Synapse `server_name` (e.g. "localhost") differs from the
URL host — set it explicitly in config.yaml.

## Memory / context lifecycle

Three layers stack into the runtime's system prompt on every fresh
session (claude: `--append-system-prompt`; codex: inlined into the
first prompt's `<mosaic_system_prompt>` block):

1. `data/agents/<id>/MEMORY.md` — agent identity (persona)
2. `data/projects/<spaceID>/PROJECT.md` + `DECISIONS.md` — shared facts
3. `data/projects/<spaceID>/rooms/<roomID>/SUMMARY.md` — last `/compact`

When a room conversation gets long, user runs `/compact`:

1. Mosaic injects a synthesised user message ("summarise this conversation
   into structured markdown…") via `compactPrompt`.
2. Claude streams a markdown summary back (visible to the user too).
3. Mosaic captures the final assistant text, writes to SUMMARY.md
   (atomic via tmp+rename).
4. Mosaic ends the session (clears in-memory `roomSession`, drops the
   resume id from sessions.json).
5. Next user message → fresh claude session that gets SUMMARY.md
   re-injected as "earlier conversation summary".

Net effect: the room's context is bounded. Long-running rooms still
behave well after any number of `/compact` cycles.

## Failure modes & recovery

| Failure | Detection | Recovery |
|---|---|---|
| Claude subprocess died between turns | `proc.Send` returns "file already closed" | evictSession() + getOrCreate (re-spawn with `--resume <sid>`) + retry once |
| Claude stdin/stdout EOF mid-turn | `Events()` channel closes | evictSession() + tell user to retry; conversation memory intact via sessions.json |
| Claude `result.subtype = error_*` | result event | translate to friendly text (rate limit / max turns / max tokens), no auto-retry |
| Synapse rate-limits login | `M_LIMIT_EXCEEDED` on agent restart | rc_login config in homeserver.yaml relaxed for dev |
| Restricted-room auto-join rejects bot | `M_FORBIDDEN` on m.space.child autojoin | log clearly, fall back to manual invite |
| Agent's pickle key deleted | crypto.db unreadable | catastrophic — E2E history lost; SSSS recovery key was printed at first cross-signing bootstrap |

## Important conventions

- **Paths in config** can use `~` / `~/`. `expandHome()` runs at the end
  of `resolve()` so claude always gets an absolute path. Don't store
  expanded paths back to config (re-resolves on each restart).
- **Don't write to `data/projects/<spaceID>/PROJECT.md` from Mosaic** —
  it's user-curated. Only `SUMMARY.md` is agent-managed.
- **goolm, not libolm**. Build with `-tags goolm` (Makefile does this).
  libolm is upstream-deprecated; goolm is mautrix-go's pure-Go port.
- **`make install` puts the binary at `$GOBIN`** (defaults to
  `~/go/bin/mosaic`). Whichever process manager you use should point at
  that path and chdir to `~/.mosaic`. Re-run `make install` after every
  code change before restarting the daemon.

## Open design points

- **Org-level cwd inheritance**: today only the immediate parent Space's
  `cwd` is consulted. Nested Org → Project Space inheritance is not
  implemented. Could walk parent chain in `resolve()`.
- **Multi-machine ("COMPUTER")**: slock's create-agent dialog has a
  COMPUTER selector for choosing which machine the runtime spawns on.
  We're single-host. To go multi-host, the daemon would need to either
  (a) federate (multiple mosaic instances sharing config via a sync
  layer), or (b) drive remote shells.
- **Restricted-room auto-join bug**: Synapse 1.152 rejects auth even when
  the bot is a Space member. Likely a Synapse bug. Workaround: manual
  invite. Worth filing upstream.
- **Per-agent runtime** — wired. `BotConfig.Runtime` ("" / "claude" /
  "codex") selects which driver under `pkg/runtime/` to spawn. Set in
  config.yaml or via `/agent new` body (`runtime: codex`). Codex driver
  spawns one `codex exec` per turn (no long-lived stdin), captures
  `thread_id` for resume, and inlines MEMORY/PROJECT/SUMMARY into the
  first prompt because codex has no `--append-system-prompt`. Open
  point: after `/compact` rewrites SUMMARY.md, a resumed codex thread
  won't see it — would need to also evict the thread on /compact for
  the new summary to flow in. OpenCode / custom-SDK drivers are still
  TODO (just need a new `pkg/runtime/<name>/` package implementing
  `runtime.Driver`).
- **ENVIRONMENT VARIABLES**: slock UI has env-var injection per agent.
  Not implemented; `BotConfig.Claude` would need an `env: map[string]string`
  passed to `streamjson.Spawn`'s `extraEnv`.

## When you (Claude) make changes here

- **Don't** rename `bots:` back to anything other than `agents:` —
  user-facing terminology was deliberately switched.
- **Don't** silently auto-retry result errors (`error_during_execution`
  etc). Some are systemic and a retry loop will burn tokens.
- **Do** keep server_name folding consistent: any new place that
  displays a Matrix ID needs `FoldHomeServer(id, b.opts.ServerName)`.
- **Do** preserve the agents/projects directory split when adding
  new file-backed state. Per-agent → `data/agents/<id>/`. Cross-agent
  shared → `data/projects/<spaceID>/`. Don't let them creep back into
  `data/agents/<id>/projects/...`.
- **Do** treat config.yaml writes as atomic: marshal full FileConfig,
  write to `path.tmp`, rename. The user may have it open in an editor.
- **Run order for changes affecting agent behaviour**: edit code →
  `make install` → restart your mosaic process (via whatever process
  manager you wired up) → tail `~/.mosaic/agent.log` → poke from Element.
