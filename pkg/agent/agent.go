// Package agent bridges Matrix and Claude: when a user posts text in
// a room, spawn `claude` (long-lived per room), feed the text into its
// stdin, and stream the assistant output back as a single Matrix
// message edited in place.
//
// Streaming policy: claude emits stream-json with --include-partial-
// messages, so we get `stream_event` content_block_delta records per
// token. We accumulate text per turn and flush via Matrix message edit
// at most every flushInterval (default 200ms) to avoid hammering the
// homeserver. Tool calls and result metadata are appended as plain
// text annotations into the same message — single editable bubble per
// turn keeps the UI tidy.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/deng00/mosaic/pkg/claude/streamjson"
	"github.com/deng00/mosaic/pkg/matrix"
)

// ProjectConfig is the cwd/model defaults for a Matrix Space (= a
// "project"). All rooms that have m.space.parent pointing at this
// space inherit these values. Identified by the space's Matrix room ID.
type ProjectConfig struct {
	Name  string
	Cwd   string
	Model string
}

// RoomConfig overrides project / fallback values for a single room.
// All fields are optional; empty means "fall through".
type RoomConfig struct {
	Cwd   string
	Model string
	// Env is extra KEY=VALUE pairs merged into the per-spawn env for
	// THIS room only. Used by the dispatcher to inject task-scoped
	// callback credentials (MOSAIC_TASK_ID, MOSAIC_TOKEN, etc.).
	Env map[string]string
}

// resolution is the per-room derived settings: which project the room
// belongs to (if any) and the final cwd/model after layering room
// override over project default over fallback.
type resolution struct {
	SpaceID     id.RoomID
	ProjectName string
	Cwd         string
	Model       string
	Env         map[string]string // per-room env merged on top of Options.Env
}

// Options configures the Bridge. Cwd defaults to the process cwd if
// empty. Model is passed to claude --model.
type Options struct {
	Cwd            string
	Model          string
	PermissionMode string // e.g. "bypassPermissions" for hands-off agents
	FlushInterval  time.Duration
	Binary         string // claude binary; default "claude"

	// Projects maps Space room ID → cwd/model defaults shared by all
	// rooms inside that space. Optional.
	Projects map[string]ProjectConfig

	// Rooms maps Room ID → overrides that win over project + fallback.
	// Optional, used sparingly.
	Rooms map[string]RoomConfig

	// Sessions persists (roomID → claude session_id) so we can pass
	// `--resume <sid>` after restarts and preserve conversation
	// context. nil disables persistence.
	Sessions *SessionStore

	// Memory layers per-workspace / per-project / per-room markdown
	// files into claude's --append-system-prompt at spawn time, and
	// receives /compact output. nil disables.
	Memory *Memory

	// Manager is the daemon-level fleet API. Used by /agent list
	// and /agent new. nil → those slash commands report "unavailable".
	Manager AgentManager

	// Admins are full Matrix user IDs allowed to run /agent new and
	// other management commands. Always implicitly allowed to drive
	// agents (no need to also list in Members).
	Admins []string

	// Members are non-admin Matrix user IDs allowed to chat with /
	// drive the agent (claude turns + tier-2 slashes like
	// /new-session, /compact). Empty = admin-only. Read-only commands
	// (/help, /status, /agent help|list, /project help|status|list)
	// remain accessible to anyone in the room.
	Members []string

	// Env is extra KEY=VALUE pairs injected into every spawned
	// claude subprocess. Useful for CLAUDE_CODE_OAUTH_TOKEN etc.
	Env map[string]string

	// ServerName is our own Matrix server (e.g. "localhost"). Used
	// to fold the `:server` suffix off displayed room/user IDs that
	// belong to us — purely a UI sweetener.
	ServerName string
}

// Bridge owns one Matrix client and a per-room claude session map.
type Bridge struct {
	mx   *matrix.Client
	opts Options

	mu             sync.Mutex
	sessions       map[id.RoomID]*roomSession
	resolutions    map[id.RoomID]resolution // cache of room → cwd/model/space
	pendingCompact map[id.RoomID]bool       // rooms whose next-completed turn should be saved as SUMMARY.md
}

func New(mx *matrix.Client, opts Options) *Bridge {
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 200 * time.Millisecond
	}
	return &Bridge{
		mx:             mx,
		opts:           opts,
		sessions:       make(map[id.RoomID]*roomSession),
		resolutions:    make(map[id.RoomID]resolution),
		pendingCompact: make(map[id.RoomID]bool),
	}
}

// InvalidateResolutions clears the per-room cwd/model resolution
// cache. Called by the manager after /project mutations so the next
// claude spawn picks up the new config without an agent restart.
// Also re-reads Projects/Rooms from Options — main.go updates those
// pointers in-place when config.yaml is rewritten.
func (b *Bridge) InvalidateResolutions(projects map[string]ProjectConfig, rooms map[string]RoomConfig) {
	b.mu.Lock()
	b.resolutions = make(map[id.RoomID]resolution)
	if projects != nil {
		b.opts.Projects = projects
	}
	if rooms != nil {
		b.opts.Rooms = rooms
	}
	b.mu.Unlock()
}

// UpdateMembers swaps in a new allow-list. Called from the manager
// after /agent allow / /agent revoke so the new value is visible to
// every running bridge without an agent restart.
func (b *Bridge) UpdateMembers(members []string) {
	b.mu.Lock()
	b.opts.Members = members
	b.mu.Unlock()
}

// RegisterRoomOverride installs (or replaces) a per-room cwd / model /
// env override. Used by the dispatcher to attach a workspace path +
// task-callback env to a freshly created topic-room before spawning
// claude in it. Drops any cached resolution so the next spawn picks up
// the new values.
func (b *Bridge) RegisterRoomOverride(roomID id.RoomID, rc RoomConfig) {
	b.mu.Lock()
	if b.opts.Rooms == nil {
		b.opts.Rooms = map[string]RoomConfig{}
	}
	b.opts.Rooms[string(roomID)] = rc
	delete(b.resolutions, roomID)
	b.mu.Unlock()
}

// MatrixUserID exposes the agent's own Matrix user id for callers
// (e.g. dispatcher needs it to know who created a room).
func (b *Bridge) MatrixUserID() id.UserID {
	return b.mx.UserID()
}

// CreateTaskRoom asks the underlying matrix client to create a topic-
// room owned by this agent, attached to parentSpace, with the given
// invitees. Convenience wrapper so the dispatcher doesn't need its
// own *matrix.Client handle.
func (b *Bridge) CreateTaskRoom(ctx context.Context, name, topic string, parentSpace id.RoomID, invite []id.UserID) (id.RoomID, error) {
	return b.mx.CreateRoom(ctx, matrix.CreateRoomOpts{
		Name:        name,
		Topic:       topic,
		ParentSpace: parentSpace,
		Invite:      invite,
		Preset:      "private_chat",
	})
}

// RunTaskTurn kicks off a fresh claude turn in roomID. Skips the usual
// ACL / sender-filter path because the dispatcher (not a human user)
// is the trigger. The room must already have its per-room override
// registered via RegisterRoomOverride so the spawn picks up the
// workspace cwd + task env.
//
// kickoff is the visible chat message that triggers the turn; agents
// see it as a regular user message. Keep it short — the heavy lifting
// belongs in the system-prompt layer (Memory / TASK.md).
func (b *Bridge) RunTaskTurn(roomID id.RoomID, kickoff string) error {
	ctx := context.Background()
	// Render the kickoff visibly into the room first so users in
	// Element see what was sent to the agent. Best-effort.
	if _, err := b.mx.SendText(ctx, roomID, kickoff); err != nil {
		log.Printf("[agent] task kickoff send failed: %v", err)
	}
	sess := b.getOrCreate(ctx, roomID)
	if sess == nil || sess.inbox == nil {
		return fmt.Errorf("agent: failed to spawn claude for task room %s", roomID)
	}
	// Push directly into the inbox — handleMessage filters out the
	// bridge's own UID, which would make a self-sent kickoff a no-op.
	select {
	case sess.inbox <- turnRequest{sender: b.mx.UserID(), text: kickoff}:
		return nil
	default:
		return fmt.Errorf("agent: task room %s inbox full", roomID)
	}
}

// expandHome expands a leading ~ to $HOME. Go's exec/chdir don't do
// this (the shell does), so user-typed config paths like
// `~/Code/foo` literally try to chdir into a directory named "~",
// which fails. Apply this everywhere a config-supplied path is
// handed to claude or filepath operations.
func expandHome(p string) string {
	if p == "" {
		return p
	}
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			if p == "~" {
				return home
			}
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

// resolve answers "which cwd / model should I use for this room?".
// Layered: per-room override → parent space's project → bot fallback.
// Result is cached per-room (parents rarely change at runtime).
func (b *Bridge) resolve(ctx context.Context, roomID id.RoomID) resolution {
	b.mu.Lock()
	if r, ok := b.resolutions[roomID]; ok {
		b.mu.Unlock()
		return r
	}
	b.mu.Unlock()

	r := resolution{Cwd: b.opts.Cwd, Model: b.opts.Model}

	// Walk parent spaces. A room can technically belong to multiple
	// spaces; we pick the first one that has a project config.
	parents, err := b.mx.ParentSpaces(ctx, roomID)
	if err == nil {
		for _, sid := range parents {
			if pc, ok := b.opts.Projects[string(sid)]; ok {
				r.SpaceID = sid
				r.ProjectName = pc.Name
				if pc.Cwd != "" {
					r.Cwd = pc.Cwd
				}
				if pc.Model != "" {
					r.Model = pc.Model
				}
				break
			}
		}
	}

	// Per-room override wins over both. Useful for one-off rooms
	// that should aim a sandboxed cwd while still being visually
	// inside a regular Space, and for the dispatcher's per-task
	// rooms (workspace cwd + task callback env).
	if rc, ok := b.opts.Rooms[string(roomID)]; ok {
		if rc.Cwd != "" {
			r.Cwd = rc.Cwd
		}
		if rc.Model != "" {
			r.Model = rc.Model
		}
		if len(rc.Env) > 0 {
			r.Env = make(map[string]string, len(rc.Env))
			for k, v := range rc.Env {
				r.Env[k] = v
			}
		}
	}

	// Expand `~` in the final cwd (config files often use ~/Code/...
	// since users hand-write them — Go's chdir won't expand it).
	r.Cwd = expandHome(r.Cwd)

	b.mu.Lock()
	b.resolutions[roomID] = r
	b.mu.Unlock()
	return r
}

// Start wires the message handler. Call once before mx.Sync.
func (b *Bridge) Start() {
	b.mx.OnMessage(b.handleMessage)
}

// roomSession is the per-room state: a long-lived claude process and
// a serial work queue. Only one turn at a time in any given room —
// claude's stdin/stdout is naturally sequential (one user message →
// one turn → result event), so we mirror that on our side via the
// queue. Messages arriving while a turn is in flight wait their
// place and process FIFO.
type roomSession struct {
	mu     sync.Mutex
	proc   *streamjson.Process
	cancel context.CancelFunc
	turns  int
	roomID id.RoomID

	// inbox is unbounded in spirit (large buffer) — incoming user
	// messages enqueue here; a single dispatcher goroutine drains
	// them and runs each turn end-to-end before pulling the next.
	inbox chan turnRequest
}

type turnRequest struct {
	sender id.UserID
	text   string
}

func (b *Bridge) handleMessage(ctx context.Context, roomID id.RoomID, eventID id.EventID, sender id.UserID, text string) {
	log.Printf("[agent] %s in %s: %s", sender, roomID, truncate(text, 80))

	// /unarchive is the only thing honored when archived; everything
	// else (claude turns, other slashes) gets bounced with a hint.
	if b.opts.Sessions != nil && b.opts.Sessions.IsArchived(string(roomID)) {
		if strings.TrimSpace(text) != "/unarchive" {
			_, _ = b.mx.SendText(context.Background(), roomID,
				"📦 此 room 已归档，发送 `/unarchive` 唤醒后再继续。memory 文件保留不变。")
			return
		}
	}

	// Slash commands are dispatched synchronously here; they're fast
	// and shouldn't fight Claude turns for the same room. Both `/foo`
	// and `!foo` are accepted — Element's web UI shows an "Unknown
	// Command" warning on `/`-prefix sends, so `!` is the noise-free
	// alternative.
	dispatch := text
	if strings.HasPrefix(text, "!") {
		dispatch = "/" + text[1:]
	}
	isSlash := strings.HasPrefix(dispatch, "/")

	// ACL: only admins + listed members may drive the agent. Read-only
	// slash commands (/help / /status / list-style queries) bypass the
	// gate so a stranger can at least see they're not authorized.
	if !b.isAllowed(sender) && !(isSlash && isReadOnlySlash(dispatch)) {
		log.Printf("[agent] denied: %s (not in admins/members)", sender)
		_, _ = b.mx.SendText(context.Background(), roomID, fmt.Sprintf(
			"🔒 你（%s）不在本 agent 的访问名单。请联系管理员（%s）加白：`/agent allow %s`",
			FoldHomeServer(string(sender), b.opts.ServerName),
			strings.Join(b.opts.Admins, ", "),
			FoldHomeServer(string(sender), b.opts.ServerName)))
		return
	}

	if isSlash {
		if handled := b.handleSlash(roomID, sender, dispatch); handled {
			return
		}
	}
	// Enqueue onto the room's serial inbox. The dispatcher goroutine
	// (spawned alongside the claude process by getOrCreate) runs each
	// turn end-to-end before picking up the next.
	sess := b.getOrCreate(context.Background(), roomID)
	if sess == nil || sess.inbox == nil {
		// getOrCreate logged the actual spawn error; surface a
		// chat-visible hint so the user isn't left wondering why
		// the message vanished. Most likely cause: configured cwd
		// doesn't exist on this host.
		log.Printf("[agent] no session inbox for %s — claude spawn failed; replying error to user", roomID)
		_, _ = b.mx.SendText(context.Background(), roomID,
			"❌ 起 claude 子进程失败 —— 多半是配置的 cwd 在这台机器上不存在。"+
				"用 `!project status` 看当前解析的 cwd，再 `!project cwd <有效路径>` 改正。"+
				"详细错见 `~/.mosaic/agent.log`.")
		return
	}
	select {
	case sess.inbox <- turnRequest{sender: sender, text: text}:
	default:
		// Buffer full → tell the user we're swamped rather than block
		// the sync goroutine.
		_, _ = b.mx.SendText(context.Background(), roomID,
			"⏳ 排队太多了，暂时无法接收。请稍候再试。")
	}
}

// isReadOnlySlash returns true for slashes any room member is allowed
// to invoke (queries / help, no state mutation, no claude spawn).
// Anything else (claude turns + state-mutating slashes) requires
// isAllowed.
func isReadOnlySlash(text string) bool {
	cmd := strings.TrimSpace(text)
	if i := strings.IndexAny(cmd, " \t"); i > 0 {
		cmd = cmd[:i]
	}
	switch cmd {
	case "/help", "/status", "/agent", "/project":
		// /agent and /project sub-dispatch their own ACL — list/help
		// is fine; mutating subs (new / cwd / allow) re-check.
		return true
	}
	return false
}

// handleSlash returns true when the input was a recognized slash
// command. Unknown / commands fall through to claude (they may be
// claude's own slash commands like /clear).
func (b *Bridge) handleSlash(roomID id.RoomID, sender id.UserID, text string) bool {
	trimmed := strings.TrimSpace(text)
	cmd := trimmed
	rest := ""
	if i := strings.IndexAny(cmd, " \t"); i > 0 {
		rest = strings.TrimSpace(cmd[i+1:])
		cmd = cmd[:i]
	}
	log.Printf("[agent] slash %s in %s", cmd, roomID)
	ctx := context.Background()
	switch cmd {
	case "/agent":
		return b.handleAgentSlash(ctx, roomID, sender, rest)
	case "/project":
		return b.handleProjectSlash(ctx, roomID, sender, rest)
	case "/new-session":
		b.endSession(roomID)
		_, _ = b.mx.SendText(ctx, roomID, "🌱 已开启新会话（前序对话不再传递）")
		return true
	case "/status":
		b.sendStatus(ctx, roomID)
		return true
	case "/help":
		_, _ = b.mx.SendText(ctx, roomID, slashHelp)
		return true
	case "/archive":
		b.endSession(roomID)
		if b.opts.Sessions != nil {
			if err := b.opts.Sessions.SetArchived(string(roomID), true); err != nil {
				log.Printf("[agent] SetArchived failed: %v", err)
			}
		}
		// Best-effort: tag the bot's own view of the room as
		// low-priority so the bot's account treats it as "shelved".
		// Element only honors this for the user who set the tag, so
		// danny himself still needs to right-click → low-priority on
		// his own client to clean up his sidebar.
		if err := b.mx.SetUserRoomTag(context.Background(), roomID, "m.lowpriority"); err != nil {
			log.Printf("[agent] tag m.lowpriority failed: %v", err)
		}
		_, _ = b.mx.SendText(ctx, roomID,
			"📦 已归档。bot 不再处理本 room 的消息（除了 `/unarchive`）。memory 文件保留。\n\n"+
				"想从你的 Element 主侧边栏挪走，可以右键此 room → **Low priority** 或 **Move to People/Other**。")
		return true
	case "/unarchive":
		if b.opts.Sessions != nil {
			if err := b.opts.Sessions.SetArchived(string(roomID), false); err != nil {
				log.Printf("[agent] unarchive failed: %v", err)
			}
		}
		_ = b.mx.DeleteUserRoomTag(context.Background(), roomID, "m.lowpriority")
		_, _ = b.mx.SendText(ctx, roomID, "🌅 已唤醒。bot 恢复响应；下一条消息会基于 SUMMARY.md（如有）开新会话。")
		return true
	case "/compact":
		// Mark room as "save next turn body to SUMMARY.md, then end
		// session". Inject the summarization prompt as if the user
		// asked for it — the resulting markdown becomes the room's
		// memory and is also visible in chat.
		b.mu.Lock()
		b.pendingCompact[roomID] = true
		b.mu.Unlock()
		_, _ = b.mx.SendText(ctx, roomID, "🗜️ 正在生成会话摘要并归档（这一轮完成后会清空 LLM 上下文）...")
		go b.runTurn(roomID, b.mx.UserID(), compactPrompt)
		return true
	}
	return false
}

const compactPrompt = `请把本次对话的全部要点压缩成一份结构化的 markdown 摘要，作为本会话以后的"记忆基线"。包括：
- **目标 / 上下文**：当前在做什么、为什么
- **关键决策**：定下来的方案、约定、命名
- **进展**：已完成的步骤
- **未决项**：明确待办、悬而未决的问题
- **关键文件 / 路径 / 代码位置**：后续会需要再次精确引用的

风格：紧凑、完整、不寒暄。直接输出 markdown，不要前后空话。`

// handleAgentSlash dispatches /agent <subcmd>. Read-only subcmds
// (list / help) anyone can run; mutating ones (new) are gated by
// the Admins list.
func (b *Bridge) handleAgentSlash(ctx context.Context, roomID id.RoomID, sender id.UserID, args string) bool {
	subcmd := args
	rest := ""
	if i := strings.IndexAny(args, " \t"); i > 0 {
		subcmd = args[:i]
		rest = strings.TrimSpace(args[i+1:])
	}

	switch subcmd {
	case "", "help":
		_, _ = b.mx.SendText(ctx, roomID, agentSlashHelp)
		return true

	case "list":
		if b.opts.Manager == nil {
			_, _ = b.mx.SendText(ctx, roomID, "⚠️ agent manager unavailable in this build")
			return true
		}
		agents := b.opts.Manager.List()
		var sb strings.Builder
		sb.WriteString("**Configured agents**\n\n")
		sb.WriteString("| ID | User | Device | Online |\n|---|---|---|---|\n")
		for _, a := range agents {
			online := "❌"
			if a.Online {
				online = "✅"
			}
			fmt.Fprintf(&sb, "| `%s` | `%s` | %s | %s |\n",
				a.ID, FoldHomeServer(a.UserID, b.opts.ServerName), a.DeviceName, online)
		}
		_, _ = b.mx.SendText(ctx, roomID, sb.String())
		return true

	case "new":
		if !b.isAdmin(sender) {
			_, _ = b.mx.SendText(ctx, roomID,
				fmt.Sprintf("⛔ `/agent new` 需要管理员权限。当前 admins: %v", b.opts.Admins))
			return true
		}
		if b.opts.Manager == nil {
			_, _ = b.mx.SendText(ctx, roomID, "⚠️ agent manager unavailable in this build")
			return true
		}
		req, err := parseCreateAgentBody(rest)
		if err != nil {
			_, _ = b.mx.SendText(ctx, roomID, "❌ "+err.Error()+"\n\n"+createAgentSyntax)
			return true
		}
		info, ierr := b.opts.Manager.Create(req)
		if ierr != nil {
			_, _ = b.mx.SendText(ctx, roomID, "❌ 创建失败："+ierr.Error())
			return true
		}
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf(
			"✅ 已创建 agent **%s**\n\n"+
				"- user: `%s`\n"+
				"- data dir: `data/%s/`\n"+
				"- 模板已写入 `data/%s/MEMORY.md`（其中 description 已填进 Role 段）\n"+
				"- 想细化 role / goals / style 直接编辑那个文件，下次新 session 生效\n\n"+
				"接下来：把 `%s` 邀请到你想让 ta 进的 Space / room。",
			info.DeviceName, info.UserID, info.ID, info.ID, info.UserID))
		return true

	case "members":
		// Show the current allow-list (admins + members). Anyone in
		// the room can run this — useful for "why am I being denied?".
		var sb strings.Builder
		sb.WriteString("**访问名单**\n\n")
		sb.WriteString("**Admins** (always allowed, can run /agent new etc.):\n")
		for _, a := range b.opts.Admins {
			fmt.Fprintf(&sb, "- `%s`\n", FoldHomeServer(a, b.opts.ServerName))
		}
		sb.WriteString("\n**Members** (allowed to drive agents, no admin power):\n")
		members := b.opts.Members
		if b.opts.Manager != nil {
			members = b.opts.Manager.Members() // live config
		}
		if len(members) == 0 {
			sb.WriteString("_(空 — 仅 admins 可用)_\n")
		} else {
			for _, m := range members {
				fmt.Fprintf(&sb, "- `%s`\n", FoldHomeServer(m, b.opts.ServerName))
			}
		}
		_, _ = b.mx.SendText(ctx, roomID, sb.String())
		return true

	case "allow":
		if !b.isAdmin(sender) {
			_, _ = b.mx.SendText(ctx, roomID, "⛔ `/agent allow` 需要管理员权限")
			return true
		}
		if b.opts.Manager == nil {
			_, _ = b.mx.SendText(ctx, roomID, "⚠️ manager unavailable")
			return true
		}
		if rest == "" {
			_, _ = b.mx.SendText(ctx, roomID, "用法：`/agent allow @user:server`（短形式 `@user` 也行）")
			return true
		}
		uid := ExpandHomeServer(rest, b.opts.ServerName)
		if err := b.opts.Manager.AddMember(uid); err != nil {
			_, _ = b.mx.SendText(ctx, roomID, "❌ "+err.Error())
			return true
		}
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf(
			"✅ 已加白 `%s`，现在 ta 可以 @ 任意 agent 聊天 / 跑 `/new-session` 等。",
			FoldHomeServer(uid, b.opts.ServerName)))
		return true

	case "revoke":
		if !b.isAdmin(sender) {
			_, _ = b.mx.SendText(ctx, roomID, "⛔ `/agent revoke` 需要管理员权限")
			return true
		}
		if b.opts.Manager == nil {
			_, _ = b.mx.SendText(ctx, roomID, "⚠️ manager unavailable")
			return true
		}
		if rest == "" {
			_, _ = b.mx.SendText(ctx, roomID, "用法：`/agent revoke @user:server`")
			return true
		}
		uid := ExpandHomeServer(rest, b.opts.ServerName)
		if err := b.opts.Manager.RemoveMember(uid); err != nil {
			_, _ = b.mx.SendText(ctx, roomID, "❌ "+err.Error())
			return true
		}
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf(
			"✅ 已从白名单移除 `%s`",
			FoldHomeServer(uid, b.opts.ServerName)))
		return true

	default:
		_, _ = b.mx.SendText(ctx, roomID,
			"未知子命令 `"+subcmd+"`。试 `/agent help`。")
		return true
	}
}

func (b *Bridge) isAdmin(sender id.UserID) bool {
	for _, a := range b.opts.Admins {
		if a == string(sender) {
			return true
		}
	}
	return false
}

// isAllowed = admin OR explicitly listed in Members. Used to gate
// claude turns and state-mutating slash commands. Default (Members
// empty) is admin-only.
func (b *Bridge) isAllowed(sender id.UserID) bool {
	if b.isAdmin(sender) {
		return true
	}
	s := string(sender)
	for _, m := range b.opts.Members {
		if m == s {
			return true
		}
	}
	return false
}

// handleProjectSlash dispatches /project <subcmd>. Read-only sub-
// commands (status / list / help) anyone can run; mutating ones
// (cwd / name) are gated by the Admins list.
func (b *Bridge) handleProjectSlash(ctx context.Context, roomID id.RoomID, sender id.UserID, args string) bool {
	subcmd := args
	rest := ""
	if i := strings.IndexAny(args, " \t"); i > 0 {
		subcmd = args[:i]
		rest = strings.TrimSpace(args[i+1:])
	}

	switch subcmd {
	case "", "help":
		_, _ = b.mx.SendText(ctx, roomID, projectSlashHelp)
		return true

	case "status":
		r := b.resolve(ctx, roomID)
		var sb strings.Builder
		sb.WriteString("**当前 room 的 project 解析**\n\n")
		fmt.Fprintf(&sb, "- room: `%s`\n", FoldHomeServer(string(roomID), b.opts.ServerName))
		if r.SpaceID == "" {
			sb.WriteString("- 该 room **不在任何 Space 下**（没有 m.space.parent state event）\n")
			sb.WriteString("- 在 Element 里把这个 room 加进某个 Space 即可纳管\n")
		} else {
			fmt.Fprintf(&sb, "- space: `%s`\n", FoldHomeServer(string(r.SpaceID), b.opts.ServerName))
			if r.ProjectName != "" {
				fmt.Fprintf(&sb, "- project name: **%s**\n", r.ProjectName)
			} else {
				sb.WriteString("- project name: _(未配置；用 `/project name <名字>` 设置)_\n")
			}
			fmt.Fprintf(&sb, "- cwd: `%s`\n", or(r.Cwd, "(默认值)"))
			fmt.Fprintf(&sb, "- model: %s\n", or(r.Model, "_(默认)_"))
		}
		_, _ = b.mx.SendText(ctx, roomID, sb.String())
		return true

	case "list":
		// Default: only the project of the current room's Space —
		// "list of projects relevant to where I'm asking from".
		// Use /project list-all for the global directory.
		if b.opts.Manager == nil {
			_, _ = b.mx.SendText(ctx, roomID, "⚠️ manager unavailable")
			return true
		}
		r := b.resolve(ctx, roomID)
		if r.SpaceID == "" {
			_, _ = b.mx.SendText(ctx, roomID,
				"该 room 不在任何 Space 下，没有所属 project。\n用 `/project list-all` 看全局所有 project。")
			return true
		}
		var found *ProjectInfo
		for _, p := range b.opts.Manager.Projects() {
			if p.SpaceID == string(r.SpaceID) {
				cp := p
				found = &cp
				break
			}
		}
		var sb strings.Builder
		sb.WriteString("**当前 Space 的 project**\n\n")
		fmt.Fprintf(&sb, "- space: `%s`\n", FoldHomeServer(string(r.SpaceID), b.opts.ServerName))
		if found == nil {
			sb.WriteString("- 还没配置——发 `/project name <名字>` 或 `/project cwd <path>` 即可初始化\n")
		} else {
			fmt.Fprintf(&sb, "- name: %s\n", or(found.Name, "_(none)_"))
			fmt.Fprintf(&sb, "- cwd: `%s`\n", or(found.Cwd, "_(default)_"))
			fmt.Fprintf(&sb, "- model: %s\n", or(found.Model, "_(default)_"))
		}
		sb.WriteString("\n_看全部已配置 project: `/project list-all`_")
		_, _ = b.mx.SendText(ctx, roomID, sb.String())
		return true

	case "list-all":
		if b.opts.Manager == nil {
			_, _ = b.mx.SendText(ctx, roomID, "⚠️ manager unavailable")
			return true
		}
		ps := b.opts.Manager.Projects()
		if len(ps) == 0 {
			_, _ = b.mx.SendText(ctx, roomID, "_(还没有配置任何 project)_")
			return true
		}
		curSpace := string(b.resolve(ctx, roomID).SpaceID)
		var sb strings.Builder
		sb.WriteString("**所有已配置 project**（全局，跨 Space）\n\n")
		sb.WriteString("| Space ID | Name | cwd | Model | |\n|---|---|---|---|---|\n")
		for _, p := range ps {
			marker := ""
			if p.SpaceID == curSpace && curSpace != "" {
				marker = "← 当前 Space"
			}
			fmt.Fprintf(&sb, "| `%s` | %s | `%s` | %s | %s |\n",
				FoldHomeServer(p.SpaceID, b.opts.ServerName),
				or(p.Name, "_(none)_"), or(p.Cwd, "_(default)_"), or(p.Model, "_(default)_"),
				marker)
		}
		_, _ = b.mx.SendText(ctx, roomID, sb.String())
		return true

	case "cwd":
		if !b.isAdmin(sender) {
			_, _ = b.mx.SendText(ctx, roomID, "⛔ `/project cwd` 需要管理员权限")
			return true
		}
		if rest == "" {
			_, _ = b.mx.SendText(ctx, roomID, "用法：`/project cwd /path/to/project`")
			return true
		}
		return b.applyProjectMutation(ctx, roomID, "", rest, "")

	case "name":
		if !b.isAdmin(sender) {
			_, _ = b.mx.SendText(ctx, roomID, "⛔ `/project name` 需要管理员权限")
			return true
		}
		if rest == "" {
			_, _ = b.mx.SendText(ctx, roomID, "用法：`/project name <人类可读的名字>`")
			return true
		}
		return b.applyProjectMutation(ctx, roomID, rest, "", "")

	default:
		_, _ = b.mx.SendText(ctx, roomID, "未知子命令 `"+subcmd+"`。试 `/project help`。")
		return true
	}
}

// applyProjectMutation resolves the current room's parent Space and
// hands name/cwd/model off to Manager.SetProject. Empty fields pass
// through (mean "leave unchanged").
func (b *Bridge) applyProjectMutation(ctx context.Context, roomID id.RoomID, name, cwd, model string) bool {
	if b.opts.Manager == nil {
		_, _ = b.mx.SendText(ctx, roomID, "⚠️ manager unavailable")
		return true
	}
	parents, err := b.mx.ParentSpaces(ctx, roomID)
	if err != nil || len(parents) == 0 {
		_, _ = b.mx.SendText(ctx, roomID,
			"⚠️ 当前 room 不在任何 Space 下。先在 Element 里把它加进一个 Space，然后再 `/project cwd ...`。")
		return true
	}
	spaceID := string(parents[0])
	if err := b.opts.Manager.SetProject(spaceID, name, cwd, model); err != nil {
		_, _ = b.mx.SendText(ctx, roomID, "❌ 写 config 失败："+err.Error())
		return true
	}
	parts := []string{}
	if name != "" {
		parts = append(parts, "name=**"+name+"**")
	}
	if cwd != "" {
		parts = append(parts, "cwd=`"+cwd+"`")
	}
	if model != "" {
		parts = append(parts, "model="+model)
	}
	_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf(
		"✅ 已更新 project (`%s`) %s\n\n下次该 Space 下任意 room **新起的 claude session** 立即生效。当前 session 仍用旧 cwd——发 `/new-session` 强制刷新。",
		FoldHomeServer(spaceID, b.opts.ServerName), strings.Join(parts, "  ·  ")))
	return true
}

const projectSlashHelp = `**` + "`/project`" + ` 命令家族**

- ` + "`/project status`" + ` — 显示当前 room 的 Space / project / cwd 解析结果
- ` + "`/project list`" + ` — 列出所有已配置 project
- ` + "`/project cwd <path>`" + ` — 给当前 room 所属 Space 设工作目录 ⛔ admin only
- ` + "`/project name <name>`" + ` — 给当前 Space 起个人类可读的名字 ⛔ admin only
- ` + "`/project help`" + ` — 这条帮助

机制：通过 Matrix 的 ` + "`m.space.parent`" + ` state event 自动反查父 Space ID，写入 ` + "`config.yaml`" + ` 的 ` + "`projects`" + ` 段。下次新 session 自动用新 cwd。`

// parseCreateAgentBody handles the slock-style multi-line form:
//
//	<localpart>
//	name: Alice
//	description: Onboarding lead. Helps users get started.
//	model: sonnet
//
// First line is the localpart; subsequent `key: value` lines populate
// the rest. Bare `/agent new` (empty body) returns an error.
func parseCreateAgentBody(body string) (CreateRequest, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return CreateRequest{}, fmt.Errorf("缺少 localpart")
	}
	lines := strings.Split(body, "\n")
	req := CreateRequest{Localpart: strings.TrimSpace(lines[0])}
	if req.Localpart == "" {
		return req, fmt.Errorf("缺少 localpart")
	}
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k = strings.ToLower(strings.TrimSpace(k))
		v = strings.TrimSpace(v)
		switch k {
		case "name", "display", "display_name":
			req.DisplayName = v
		case "description", "desc", "role":
			req.Description = v
		case "model":
			req.Model = v
		case "runtime":
			req.Runtime = v
		}
	}
	if req.DisplayName == "" {
		req.DisplayName = req.Localpart
	}
	return req, nil
}

const createAgentSyntax = "用法（slock 风格 multi-line）:\n```\n" +
	"/agent new alice\n" +
	"name: Alice\n" +
	"description: Onboarding lead. Helps users get started.\n" +
	"model: sonnet\n" +
	"```\n仅 localpart 必填；其余可选，缺省的会写一个通用模板。"

const agentSlashHelp = `**` + "`/agent`" + ` 命令家族**

- ` + "`/agent list`" + ` — 列出所有已配置 agent + 在线状态
- ` + "`/agent new <localpart> [display name]`" + ` — 创建新 agent（注册 Matrix 账号 + 写 config + 即时上线 + 创建 ` + "`MEMORY.md`" + ` 模板）⛔ admin only
- ` + "`/agent members`" + ` — 显示访问名单（admins + members）
- ` + "`/agent allow @user`" + ` — 加白：让该用户可以驱动 agent（claude 对话 + tier-2 slashes）⛔ admin only
- ` + "`/agent revoke @user`" + ` — 从白名单移除 ⛔ admin only
- ` + "`/agent help`" + ` — 这条帮助

新 agent 的 ` + "`MEMORY.md`" + ` 是其 persona / role：

` + "```markdown" + `
# Cindy
## Role
You are Cindy, the onboarding lead.
## Core Goals
- ...
` + "```" + `

每次 fresh claude session 启动时，` + "`MEMORY.md`" + ` 会作为 system prompt 注入。`

const slashHelp = `**可用命令**

- ` + "`/new-session`" + ` — 直接抛弃当前会话（不留摘要），下一条消息起全新 claude session
- ` + "`/compact`" + ` — 让 claude 把当前会话总结成一份 markdown 摘要，归档到本 room 的 ` + "`SUMMARY.md`" + `；之后的新会话会自动注入这份摘要作为系统提示
- ` + "`/archive`" + ` — 把本 room 标记为已归档：bot 不再响应（除 ` + "`/unarchive`" + `），但 memory 文件保留
- ` + "`/unarchive`" + ` — 唤醒已归档的 room
- ` + "`/status`" + ` — 显示当前 room 的 session id / project / cwd
- ` + "`/agent`" + ` — agent 管理（list / new …）— 见 ` + "`/agent help`" + `
- ` + "`/project`" + ` — project 管理（status / cwd / name …）— 见 ` + "`/project help`" + `
- ` + "`/help`" + ` — 这条帮助

> Element web 对 ` + "`/`" + ` 起头的未知命令会弹 "Unknown Command" 提示——回车或点 Send as message 即可发出。
> 也可以把任何命令的 ` + "`/`" + ` 换成 ` + "`!`" + `（如 ` + "`!status`" + ` / ` + "`!agent list`" + `）等价生效，不触发 Element 警告。

未识别的 ` + "`/xxx`" + ` 会原样发给 claude（claude 自己的 slash command 会生效）。`

func (b *Bridge) sendStatus(ctx context.Context, roomID id.RoomID) {
	r := b.resolve(ctx, roomID)
	log.Printf("[agent] /status %s → project=%q space=%s cwd=%s model=%s\n",
		roomID, r.ProjectName, r.SpaceID, r.Cwd, r.Model)
	resume := ""
	if b.opts.Sessions != nil {
		resume = b.opts.Sessions.Get(string(roomID))
	}
	b.mu.Lock()
	_, alive := b.sessions[roomID]
	b.mu.Unlock()
	procStatus := "未启动（下一条消息会启动）"
	if alive {
		procStatus = "running"
	}

	var sb strings.Builder
	sb.WriteString("**room status**\n\n")
	fmt.Fprintf(&sb, "- room: `%s`\n", FoldHomeServer(string(roomID), b.opts.ServerName))
	if r.SpaceID != "" {
		fmt.Fprintf(&sb, "- project: %s (`%s`)\n", r.ProjectName, FoldHomeServer(string(r.SpaceID), b.opts.ServerName))
	} else {
		sb.WriteString("- project: _（未挂在任何配置过的 Space 下）_\n")
	}
	fmt.Fprintf(&sb, "- cwd: `%s`\n", or(r.Cwd, "(claude default)"))
	fmt.Fprintf(&sb, "- model: %s\n", or(r.Model, "_(default)_"))
	fmt.Fprintf(&sb, "- claude process: %s\n", procStatus)
	if resume != "" {
		fmt.Fprintf(&sb, "- resumable session id: `%s`\n", resume)
	} else {
		sb.WriteString("- resumable session id: _(none yet)_\n")
	}
	_, _ = b.mx.SendText(ctx, roomID, sb.String())
}

// evictSession drops the cached roomSession (closing the inbox so the
// dispatch loop exits) but leaves the session_id in SessionStore so a
// fresh spawn will `--resume` and keep the conversation memory. Used
// when we detect a dead claude subprocess (Send returned "file
// already closed", or Events() channel closed unexpectedly). The
// next message in the room reaches getOrCreate's slow path and
// transparently spawns a new claude with --resume.
func (b *Bridge) evictSession(roomID id.RoomID) {
	b.mu.Lock()
	s, ok := b.sessions[roomID]
	if ok {
		delete(b.sessions, roomID)
	}
	b.mu.Unlock()
	if ok && s != nil {
		if s.inbox != nil {
			close(s.inbox)
		}
		if s.proc != nil {
			_ = s.proc.Close(0)
		}
		if s.cancel != nil {
			s.cancel()
		}
	}
}

// endSession kills the running claude subprocess for this room (if
// any) and forgets its session id so /resume won't bring it back.
// Closes the room's inbox so the dispatch loop can exit cleanly. The
// next user message in the room will spawn a fresh session.
func (b *Bridge) endSession(roomID id.RoomID) {
	b.mu.Lock()
	s, ok := b.sessions[roomID]
	if ok {
		delete(b.sessions, roomID)
	}
	b.mu.Unlock()
	if ok && s != nil {
		if s.inbox != nil {
			close(s.inbox)
		}
		if s.proc != nil {
			_ = s.proc.Close(2 * time.Second)
		}
		if s.cancel != nil {
			s.cancel()
		}
	}
	if b.opts.Sessions != nil {
		_ = b.opts.Sessions.Set(string(roomID), "")
	}
}

func or(a, fallback string) string {
	if a == "" {
		return fallback
	}
	return a
}

func (b *Bridge) runTurn(roomID id.RoomID, sender id.UserID, text string) {
	ctx := context.Background()
	log.Printf("[agent] runTurn start in %s", roomID)

	sess := b.getOrCreate(ctx, roomID)
	if sess == nil || sess.proc == nil {
		log.Printf("[agent] no claude proc available for %s — aborting turn", roomID)
		_, _ = b.mx.SendText(ctx, roomID, "❌ failed to start claude (see agent logs)")
		return
	}
	sess.mu.Lock()
	sess.turns++
	turnIdx := sess.turns
	sess.mu.Unlock()

	log.Printf("[agent] sending text into claude stdin (turn %d, %d bytes)", turnIdx, len(text))
	if err := sess.proc.Send(streamjson.NewTextMessage(text)); err != nil {
		// Most common: claude died between turns and stdin pipe is
		// closed ("write |1: file already closed"). Evict the dead
		// session and respawn — the new session will --resume the
		// same claude session_id, preserving conversation memory.
		log.Printf("[agent] proc.Send failed: %v — evicting + respawning", err)
		b.evictSession(roomID)
		sess = b.getOrCreate(ctx, roomID)
		if sess == nil || sess.proc == nil {
			_, _ = b.mx.SendText(ctx, roomID, "❌ claude 重启失败（见 agent log），请稍后重试")
			return
		}
		sess.mu.Lock()
		sess.turns++
		turnIdx = sess.turns
		sess.mu.Unlock()
		if err2 := sess.proc.Send(streamjson.NewTextMessage(text)); err2 != nil {
			log.Printf("[agent] retry Send still failed: %v", err2)
			_, _ = b.mx.SendText(ctx, roomID, "❌ claude 仍无法接收消息："+err2.Error())
			return
		}
		log.Printf("[agent] respawned claude session, retry sent (turn %d)", turnIdx)
	}

	// One Matrix message per claude content block. Text blocks are
	// streamed (created lazily on first delta, edited on flush ticks,
	// finalized on the assistant event). tool_use blocks become their
	// own short messages. The natural Matrix timeline order is the
	// chronological order claude emitted them — no edits-of-edits, no
	// scrambled "final summary at top" effect.
	t := newTurn(b, roomID)
	defer t.cleanup()
	t.startTyping(ctx)

	flush := time.NewTicker(b.opts.FlushInterval)
	defer flush.Stop()

	for {
		select {
		case <-flush.C:
			t.flushPendingText(ctx)
		case raw, ok := <-sess.proc.Events():
			if !ok {
				log.Printf("[agent] turn %d in %s: claude EOF — evicting session", turnIdx, roomID)
				b.evictSession(roomID)
				_, _ = b.mx.SendText(ctx, roomID, "❌ claude 进程退出（已清理本会话状态，下条消息将自动 resume 重启）")
				return
			}
			done := t.consume(ctx, raw)
			if done {
				t.flushPendingText(ctx)
				log.Printf("[agent] turn %d in %s: done (final-text=%dB, tool_msgs=%d)",
					turnIdx, roomID, len(t.lastFinalText), t.toolCount)
				if b.opts.Sessions != nil && t.lastSessionID != "" {
					if err := b.opts.Sessions.Set(string(roomID), t.lastSessionID); err != nil {
						log.Printf("[agent] sessionstore.Set failed: %v", err)
					}
				}
				// /compact post-turn hook: persist the final assistant
				// text (the actual summary, not tool calls) as the
				// room's SUMMARY.md and reset the session.
				b.mu.Lock()
				wantCompact := b.pendingCompact[roomID]
				if wantCompact {
					delete(b.pendingCompact, roomID)
				}
				b.mu.Unlock()
				if wantCompact && b.opts.Memory != nil {
					r := b.resolve(context.Background(), roomID)
					summary := strings.TrimSpace(t.lastFinalText)
					if err := b.opts.Memory.WriteSummary(r.SpaceID, roomID, summary); err != nil {
						log.Printf("[agent] write SUMMARY.md failed: %v", err)
						_, _ = b.mx.SendText(context.Background(), roomID, "⚠️ /compact 摘要写入失败："+err.Error())
					} else {
						log.Printf("[agent] /compact saved SUMMARY.md for room %s (space=%s, %d bytes)", roomID, r.SpaceID, len(summary))
						b.endSession(roomID)
						_, _ = b.mx.SendText(context.Background(), roomID,
							fmt.Sprintf("✅ 已归档（%d 字节 → SUMMARY.md）。LLM 上下文已重置；下一条消息会基于摘要开新会话。", len(summary)))
					}
				}
				return
			}
		}
	}
}

// turn drives one round-trip with claude, mapping content blocks to
// Matrix messages. Single-goroutine; no internal locking needed.
type turn struct {
	b      *Bridge
	roomID id.RoomID

	// Current streaming text block.
	textEvent   id.EventID      // empty until first delta lands; then the streamed msg's id
	pending     strings.Builder // accumulated delta NOT yet flushed
	lastFlushed string          // body we last sent (skip edit if unchanged)

	// Cumulative tool call count (for end-of-turn diagnostic).
	toolCount int

	// Latest finalized assistant text body — what /compact saves to
	// SUMMARY.md (just the summary, not tool calls).
	lastFinalText string

	// system/init carries the current session_id; we mirror it here
	// so SessionStore can persist it after the turn ends.
	lastSessionID string

	typing bool
}

func newTurn(b *Bridge, roomID id.RoomID) *turn {
	return &turn{b: b, roomID: roomID}
}

func (t *turn) cleanup() {
	if t.typing {
		_ = t.b.mx.Typing(context.Background(), t.roomID, false, 0)
		t.typing = false
	}
}

func (t *turn) startTyping(ctx context.Context) {
	if err := t.b.mx.Typing(ctx, t.roomID, true, 30000); err == nil {
		t.typing = true
	}
}

// flushPendingText sends or edits the streaming text message with the
// current pending body. No-op when nothing new since last flush.
func (t *turn) flushPendingText(ctx context.Context) {
	body := t.pending.String()
	if body == t.lastFlushed {
		return
	}
	t.lastFlushed = body
	if t.textEvent == "" {
		evID, err := t.b.mx.SendText(ctx, t.roomID, body)
		if err != nil {
			log.Printf("[agent] send streaming text failed: %v", err)
			return
		}
		t.textEvent = evID
		return
	}
	if err := t.b.mx.EditText(ctx, t.roomID, t.textEvent, body); err != nil {
		log.Printf("[agent] edit streaming text failed: %v", err)
	}
}

// finalizeText caps the current text block: forces a final flush
// (with the canonical text from the assistant event, which may
// differ slightly from accumulated deltas due to stream merging),
// then resets state so the next delta starts a new message.
func (t *turn) finalizeText(ctx context.Context, canonical string) {
	if canonical != "" {
		t.pending.Reset()
		t.pending.WriteString(canonical)
	}
	t.flushPendingText(ctx)
	if canonical != "" {
		t.lastFinalText = canonical
	}
	t.textEvent = ""
	t.pending.Reset()
	t.lastFlushed = ""
}

// consume one stream-json event. Returns done=true on `result`.
func (t *turn) consume(ctx context.Context, raw json.RawMessage) bool {
	var head struct {
		Type    string `json:"type"`
		Subtype string `json:"subtype"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return false
	}

	switch head.Type {
	case "stream_event":
		// Partial text delta; lazy-create the streaming text msg.
		var ev struct {
			Event struct {
				Type  string `json:"type"`
				Delta struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"delta"`
			} `json:"event"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			return false
		}
		if ev.Event.Type == "content_block_delta" && ev.Event.Delta.Type == "text_delta" {
			t.pending.WriteString(ev.Event.Delta.Text)
		}
		return false

	case "assistant":
		// One full assistant message — walk its blocks in order.
		var ev struct {
			Message struct {
				Content []struct {
					Type  string          `json:"type"`
					Text  string          `json:"text"`
					Name  string          `json:"name"`
					ID    string          `json:"id"`
					Input json.RawMessage `json:"input"`
				} `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			return false
		}
		for _, blk := range ev.Message.Content {
			switch blk.Type {
			case "text":
				// finalize any in-progress text msg with the canonical
				// text from this block (deltas should match it but be
				// authoritative)
				t.finalizeText(ctx, blk.Text)
			case "thinking":
				// Surface as a quiet italic line in its own message.
				t.finalizeText(ctx, "")
				_, _ = t.b.mx.SendText(ctx, t.roomID, "_💭 "+truncate(oneLineCondensed(blk.Text), 600)+"_")
			case "tool_use":
				// Whatever text was streaming, close it before the
				// tool message so timeline order is preserved.
				t.finalizeText(ctx, "")
				body := FormatToolUse(blk.Name, blk.Input)
				if _, err := t.b.mx.SendText(ctx, t.roomID, body); err != nil {
					log.Printf("[agent] send tool_use msg failed: %v", err)
				}
				t.toolCount++
			}
		}
		return false

	case "user":
		// tool_result blocks. Most are silent; surface only errors.
		var ev struct {
			Message struct {
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			return false
		}
		var blocks []struct {
			Type      string          `json:"type"`
			ToolUseID string          `json:"tool_use_id"`
			Content   json.RawMessage `json:"content"`
			IsError   bool            `json:"is_error"`
		}
		if err := json.Unmarshal(ev.Message.Content, &blocks); err != nil {
			return false
		}
		for _, blk := range blocks {
			if blk.Type != "tool_result" || !blk.IsError {
				continue
			}
			// We don't know the tool name from this event; surface a
			// generic error block with the content so the user knows
			// something went wrong.
			body := FormatToolResult("tool", blk.Content, true)
			if body != "" {
				_, _ = t.b.mx.SendText(ctx, t.roomID, body)
			}
		}
		return false

	case "system":
		if head.Subtype == "init" {
			var ev struct {
				SessionID string `json:"session_id"`
			}
			if err := json.Unmarshal(raw, &ev); err == nil {
				t.lastSessionID = ev.SessionID
			}
		}
		return false

	case "result":
		// End of turn. Translate claude's terse error subtypes into
		// something a chat user can act on.
		var ev struct {
			Subtype string `json:"subtype"`
		}
		_ = json.Unmarshal(raw, &ev)
		if strings.HasPrefix(ev.Subtype, "error") {
			_, _ = t.b.mx.SendText(ctx, t.roomID, formatResultError(ev.Subtype))
		}
		return true
	}
	return false
}

// formatResultError renders claude's `result.subtype=error_*` into a
// chat-friendly message. We don't auto-retry — claude's errors can be
// systematic (rate limit, exhausted tools), and a retry loop in the
// agent could spin up cost / make things worse.
func formatResultError(subtype string) string {
	switch subtype {
	case "error_during_execution":
		return "❌ Claude 执行中出错（多半是工具调用失败 / API 限流 / 网络抖动）。重发上一条消息可重试；若反复出现请检查 `~/.mosaic/agent.log`。"
	case "error_max_turns":
		return "❌ 触达 turn 上限。Claude 一次会话内的工具调用回合数有限，可能任务过于复杂。建议 `/compact` 总结后重起。"
	case "error_max_tokens":
		return "❌ 输出 token 上限。这一轮太长了——可让 Claude 分步输出，或调小请求范围。"
	default:
		return "❌ Claude error: " + subtype + "（未识别的错误码）"
	}
}

func oneLineCondensed(s string) string {
	s = strings.ReplaceAll(s, "\n\n", " ⏎⏎ ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}

func (b *Bridge) getOrCreate(ctx context.Context, roomID id.RoomID) *roomSession {
	// Fast path: cached session.
	b.mu.Lock()
	if s, ok := b.sessions[roomID]; ok {
		b.mu.Unlock()
		return s
	}
	b.mu.Unlock()

	// Resolve outside the lock — resolve() acquires b.mu itself for
	// the resolution cache, and Go's sync.Mutex is non-reentrant.
	r := b.resolve(ctx, roomID)
	cwd := r.Cwd
	model := r.Model

	// If we have a stored session id for this room, resume it; the
	// claude CLI will load its prior state and the next user message
	// continues the prior conversation.
	resume := ""
	if b.opts.Sessions != nil {
		resume = b.opts.Sessions.Get(string(roomID))
	}

	procCtx, cancel := context.WithCancel(context.Background())
	appendSP := ""
	if b.opts.Memory != nil && resume == "" {
		// Only inject memory on a fresh session — when resuming,
		// claude already has the prior in-conversation context, and
		// re-injecting an outdated SUMMARY.md would confuse it.
		appendSP = b.opts.Memory.SystemPrompt(r.SpaceID, roomID)
	}
	mergedEnv := mergeEnv(b.opts.Env, r.Env)
	log.Printf("[agent] spawning claude (cwd=%s model=%q resume=%q sysPromptLen=%d envKeys=%d)", cwd, model, resume, len(appendSP), len(mergedEnv))
	proc, err := streamjson.Spawn(procCtx, streamjson.Options{
		Cwd:                cwd,
		Model:              model,
		PermissionMode:     b.opts.PermissionMode,
		Binary:             b.opts.Binary,
		Resume:             resume,
		ExtraEnv:           envMapToSlice(mergedEnv),
		AppendSystemPrompt: appendSP,
	})
	if err != nil {
		cancel()
		log.Printf("[agent] spawn claude failed: %v", err)
		return &roomSession{cancel: cancel}
	}
	scope := "no-project"
	if r.ProjectName != "" {
		scope = "project=" + r.ProjectName
	} else if r.SpaceID != "" {
		scope = "space=" + string(r.SpaceID)
	}
	if resume != "" {
		log.Printf("[agent] resumed claude session %s for room %s (cwd=%s, %s)", resume, roomID, cwd, scope)
	} else {
		log.Printf("[agent] spawned fresh claude session for room %s (cwd=%s, %s)", roomID, cwd, scope)
	}

	// Re-acquire the lock to register the session, racing-aware: if
	// another goroutine spawned one for the same room while we were
	// in resolve / Spawn, prefer the existing one and tear ours down.
	b.mu.Lock()
	if existing, ok := b.sessions[roomID]; ok {
		b.mu.Unlock()
		log.Printf("[agent] race: another goroutine spawned for %s; closing duplicate", roomID)
		_ = proc.Close(0)
		cancel()
		return existing
	}
	s := &roomSession{
		proc:   proc,
		cancel: cancel,
		roomID: roomID,
		inbox:  make(chan turnRequest, 32),
	}
	b.sessions[roomID] = s
	b.mu.Unlock()

	// Per-room serial dispatcher: runs one turn at a time in FIFO.
	go b.dispatchLoop(s)

	// Pump session-id observation in a background goroutine: each
	// system/init event carries the session_id claude is using; we
	// snapshot it to the SessionStore so we can --resume next run.
	if b.opts.Sessions != nil {
		go b.observeSessionID(s)
	}
	return s
}

// dispatchLoop drains a room's inbox FIFO and runs one turn end-to-end
// per message. Exits when proc.Events() closes (claude died) or
// when the session is removed (e.g. /new-session, /compact).
func (b *Bridge) dispatchLoop(s *roomSession) {
	for req := range s.inbox {
		b.runTurn(s.roomID, req.sender, req.text)
		// Note: if /new-session or /compact ran during the turn, the
		// session was removed from b.sessions but THIS goroutine
		// keeps draining its old inbox (now orphaned). The next
		// handleMessage will resolve a fresh session via getOrCreate.
		// To stop this old loop, /new-session / /compact close the
		// inbox via endSession.
		b.mu.Lock()
		_, stillCurrent := b.sessions[s.roomID]
		b.mu.Unlock()
		if !stillCurrent {
			log.Printf("[agent] dispatchLoop %s: session was replaced, exiting", s.roomID)
			return
		}
	}
	log.Printf("[agent] dispatchLoop %s: inbox closed", s.roomID)
}

func (b *Bridge) observeSessionID(s *roomSession) {
	// Note: we don't drain the events channel here — runTurn does that.
	// Instead we tap session_id via render.consume which already
	// extracts it from system/init. To avoid double-draining, we
	// wire the SessionStore.Set call into render.consume itself.
	// This goroutine exists as a placeholder for future per-session
	// background work (heartbeats / health checks).
	_ = s
}

// ----- helpers -----

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func envMapToSlice(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}

// mergeEnv returns base ⊕ overlay (overlay wins). Either side may be
// nil; result is nil only when both are.
func mergeEnv(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overlay {
		out[k] = v
	}
	return out
}
