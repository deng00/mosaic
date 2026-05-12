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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/deng00/mosaic/pkg/runtime"
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
// Both fields are optional; empty means "fall through".
type RoomConfig struct {
	Cwd   string
	Model string
}

// resolution is the per-room derived settings: which project the room
// belongs to (if any) and the final cwd/model after layering room
// override over project default over fallback.
type resolution struct {
	SpaceID     id.RoomID
	ProjectName string
	Cwd         string
	Model       string
}

// Options configures the Bridge. Cwd defaults to the process cwd if
// empty. Model is passed to the coding agent (--model).
type Options struct {
	// Runtime selects the coding-agent driver ("claude", "codex").
	// Empty defaults to "claude" via pkg/runtime.Get.
	Runtime string

	Cwd            string
	Model          string
	// Effort maps to claude --effort (low / medium / high / xhigh / max).
	// Codex ignores. Per-agent default; no per-project override layered today.
	Effort         string
	PermissionMode string // claude-only; e.g. "bypassPermissions"
	FlushInterval  time.Duration
	Binary         string // override binary path; default per-driver

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

	// IgnoreToolsMsg is the set of tool names whose ToolUse events
	// the bridge should drop silently (not post into the room).
	// Keys are lower-cased; the daemon resolves config defaults
	// before building Options.
	IgnoreToolsMsg map[string]bool

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
	pendingAsks    map[id.RoomID]*pendingAsk // latest open ask_user prompt per room

	// membershipCache memoises `/joined_members` per room so routing
	// decisions don't pay a homeserver round-trip per inbound message.
	// 5-minute TTL trades off responsiveness for cost — room
	// membership changes infrequently and the worst-case error is a
	// brief mis-classification (broadcast vs targeted) until refresh.
	membershipCache map[id.RoomID]*membershipEntry
}

type membershipEntry struct {
	members map[id.UserID]bool
	fetched time.Time
}

const membershipTTL = 5 * time.Minute

// inboxFullMsg is the user-facing notice when a per-room dispatch
// inbox can't accept a new turnRequest. Shown in both the main message
// path and the ask_user reaction path so behaviour stays consistent.
const inboxFullMsg = "⏳ 排队太多了，暂时无法接收。请稍候再试。"

// pendingAsk tracks an open <ask_user> question awaiting a number-emoji
// reaction. eventID is the Matrix message the bot pre-seeded with
// 1️⃣..N reactions; options[i] is the text injected as the next user
// turn when the user picks emoji (i+1).
type pendingAsk struct {
	eventID id.EventID
	options []string
}

func New(mx *matrix.Client, opts Options) *Bridge {
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 200 * time.Millisecond
	}
	return &Bridge{
		mx:              mx,
		opts:            opts,
		sessions:        make(map[id.RoomID]*roomSession),
		resolutions:     make(map[id.RoomID]resolution),
		pendingCompact:  make(map[id.RoomID]bool),
		pendingAsks:     make(map[id.RoomID]*pendingAsk),
		membershipCache: make(map[id.RoomID]*membershipEntry),
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
	// inside a regular Space.
	if rc, ok := b.opts.Rooms[string(roomID)]; ok {
		if rc.Cwd != "" {
			r.Cwd = rc.Cwd
		}
		if rc.Model != "" {
			r.Model = rc.Model
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
	b.mx.OnSpaceJoined(b.handleSpaceJoined)
	b.mx.OnReaction(b.handleReaction)
}

// askUserProtocol is appended to every spawn's system prompt so the
// model knows the exact envelope to emit when it wants the user to
// pick from discrete options. The bridge parses these blocks out of
// the final assistant text and renders an interactive Matrix
// message with 1️⃣..N reactions; the user's emoji pick becomes the
// next user turn verbatim.
//
// Constraints we enforce in the parser:
//   - 2 ≤ option count ≤ 10
//   - each option on its own bullet line ("- " or "* ")
//   - question is everything else inside the block (typically one line)
//
// Malformed blocks are LEFT INLINE so the model can see in the next
// turn's context that the format wasn't honored.
const askUserProtocol = `## Mosaic protocol: ask_user

When you need the user to pick from a small set of discrete options,
output exactly this block (and nothing else around it that mimics it):

` + "```" + `
<ask_user>
your question on one line
- option 1 text
- option 2 text
- option 3 text
</ask_user>
` + "```" + `

Rules:
- 2 to 10 options. Fewer than 2 = it's not a multiple-choice question, just ask plainly.
- Each option on its own bullet line ("- " or "* ").
- Mosaic strips this block from the visible reply and renders the question
  as a separate message with 1️⃣ 2️⃣ … reactions; the user's pick becomes
  the next user turn verbatim.
- Don't use this for yes/no — just ask in plain text.
- Don't paste this block inside other text or code fences — it must be
  at the top level of your reply.`

// askUserNumberEmoji maps a 1-based index to the keycap-number emoji
// Element renders as a button. Indexes outside 1–10 yield "" — the
// caller should cap option counts at 10.
func askUserNumberEmoji(i int) string {
	switch i {
	case 1:
		return "1️⃣"
	case 2:
		return "2️⃣"
	case 3:
		return "3️⃣"
	case 4:
		return "4️⃣"
	case 5:
		return "5️⃣"
	case 6:
		return "6️⃣"
	case 7:
		return "7️⃣"
	case 8:
		return "8️⃣"
	case 9:
		return "9️⃣"
	case 10:
		return "🔟"
	}
	return ""
}

// indexFromAskEmoji is the inverse of askUserNumberEmoji. Returns 0
// for unrelated emojis (the bridge ignores reactions outside the ask
// keycap set so users can still 👍 / ❤️ messages without triggering
// a turn injection).
func indexFromAskEmoji(key string) int {
	for i := 1; i <= 10; i++ {
		if askUserNumberEmoji(i) == key {
			return i
		}
	}
	return 0
}

// askUser captures one parsed <ask_user> block.
type askUser struct {
	question string
	options  []string
}

// extractAskUserBlocks pulls every <ask_user>...</ask_user> block out
// of text and returns (cleaned text, parsed asks). Quiet on malformed
// blocks (no closer / fewer than 2 options): the block is left inline
// and no ask is parsed, so the model sees its own output in the next
// turn's context as a hint that the protocol wasn't honored.
//
// Expected block format (1-indented list, dash bullets, ≤ 10 options):
//
//	<ask_user>
//	question text on one line
//	- option A
//	- option B
//	- option C
//	</ask_user>
func extractAskUserBlocks(text string) (string, []askUser) {
	const open = "<ask_user>"
	const close = "</ask_user>"
	var (
		asks  []askUser
		out   strings.Builder
		head  = 0
	)
	for {
		i := strings.Index(text[head:], open)
		if i < 0 {
			out.WriteString(text[head:])
			break
		}
		startBlock := head + i
		j := strings.Index(text[startBlock:], close)
		if j < 0 {
			// Unterminated — give up parsing further blocks.
			out.WriteString(text[head:])
			break
		}
		endBlock := startBlock + j + len(close)
		raw := text[startBlock+len(open) : startBlock+j]
		if a, ok := parseAskBlock(raw); ok {
			out.WriteString(text[head:startBlock])
			asks = append(asks, a)
		} else {
			// Keep the malformed block visible so the user / next turn
			// can see what went wrong.
			out.WriteString(text[head:endBlock])
		}
		head = endBlock
	}
	cleaned := strings.TrimSpace(out.String())
	return cleaned, asks
}

// parseAskBlock parses the inner body of an <ask_user> block. Returns
// ok=false when the block has fewer than 2 options or no question
// line — both signal "model didn't follow the format, leave it
// inline."
func parseAskBlock(raw string) (askUser, bool) {
	var (
		question string
		options  []string
	)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Option lines: "- text" or "* text". Anything else is the
		// question (only the first such line is kept; extras are
		// folded into the question with spaces).
		if strings.HasPrefix(line, "- ") || strings.HasPrefix(line, "* ") {
			opt := strings.TrimSpace(line[2:])
			if opt != "" {
				options = append(options, opt)
			}
			continue
		}
		if question == "" {
			question = line
		} else {
			question += " " + line
		}
	}
	if question == "" || len(options) < 2 {
		return askUser{}, false
	}
	if len(options) > 10 {
		options = options[:10]
	}
	return askUser{question: question, options: options}, true
}

// renderAskUser posts the question + numbered options as one Matrix
// message, pre-seeds 1️⃣..N reactions on it, and stores the room's
// pendingAsk so handleReaction can resolve the user's pick.
func (t *turn) renderAskUser(ctx context.Context, a askUser) {
	var sb strings.Builder
	sb.WriteString("❓ ")
	sb.WriteString(a.question)
	sb.WriteString("\n\n")
	for i, opt := range a.options {
		// Blank line between options — goldmark collapses single \n
		// to a space (paragraph behavior), so a hard line break
		// requires either two trailing spaces or a blank line. Blank
		// line wins on portability.
		fmt.Fprintf(&sb, "%s %s\n\n", askUserNumberEmoji(i+1), opt)
	}
	sb.WriteString("_点下面对应的数字 emoji 选一个。_")

	evID, err := t.b.mx.SendText(ctx, t.roomID, sb.String())
	if err != nil {
		log.Printf("[agent] ask_user send failed: %v", err)
		return
	}
	for i := range a.options {
		key := askUserNumberEmoji(i + 1)
		if err := t.b.mx.SendReaction(ctx, t.roomID, evID, key); err != nil {
			log.Printf("[agent] ask_user pre-seed reaction %s failed: %v", key, err)
		}
	}
	t.b.mu.Lock()
	t.b.pendingAsks[t.roomID] = &pendingAsk{eventID: evID, options: a.options}
	t.b.mu.Unlock()
}

// handleReaction is the m.reaction handler. We only act when the
// reaction targets an open pendingAsk for the room AND the emoji
// matches a 1️⃣..🔟 keycap. The first matching reaction wins; we
// clear the pendingAsk so follow-on clicks don't kick a second turn.
func (b *Bridge) handleReaction(ctx context.Context, roomID id.RoomID, sourceEventID id.EventID, sender id.UserID, key string) {
	idx := indexFromAskEmoji(key)
	if idx == 0 {
		return
	}
	if !b.isAllowed(sender) {
		return
	}
	b.mu.Lock()
	pending, ok := b.pendingAsks[roomID]
	if !ok || pending.eventID != sourceEventID {
		b.mu.Unlock()
		return
	}
	if idx < 1 || idx > len(pending.options) {
		b.mu.Unlock()
		return
	}
	chosen := pending.options[idx-1]
	delete(b.pendingAsks, roomID)
	b.mu.Unlock()
	log.Printf("[agent] ask_user resolved in %s: %s → %q", roomID, key, truncate(chosen, 80))

	// Acknowledge the choice visibly so the user sees what was picked
	// (the reaction click itself is subtle), then enqueue as a turn.
	_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf("> %s %s", key, chosen))

	sess := b.getOrCreate(ctx, roomID)
	if sess == nil || sess.inbox == nil {
		log.Printf("[agent] ask_user could not enqueue (no inbox) in %s", roomID)
		return
	}
	select {
	case sess.inbox <- turnRequest{sender: sender, text: chosen}:
	default:
		// Same swamped-notification policy as the main message path
		// (#812): silent drops on the reaction path leave the user
		// staring at a room that never reacts to their click.
		log.Printf("[agent] ask_user inbox full in %s", roomID)
		_, _ = b.mx.SendText(context.Background(), roomID, inboxFullMsg)
	}
}

// handleSpaceJoined runs after the bot auto-joins a fresh sub-Space
// (a child Space of an existing Space the bot is in). Auto-join has
// no human "trigger" to invite to welcome — passes "" so the welcome
// room exists as a Space child only, visible after the user joins
// the Space in Element.
func (b *Bridge) handleSpaceJoined(ctx context.Context, parentSpace, newSpace id.RoomID, spaceName string) {
	b.handleSpaceJoinedWithInvite(ctx, parentSpace, newSpace, spaceName, "")
}

// handleSpaceJoinedWithInvite is the shared post-Space-join init.
// Two idempotent side effects:
//
//  1. EnsureProject inserts a project entry keyed by the new Space's
//     ID, defaulting the project name to the Space's m.room.name.
//     Only the first agent to win the race gets created=true.
//  2. The winning agent creates a "welcome" topic-room as a child of
//     the new Space and posts a short bootstrap message. Other agents
//     no-op so we don't end up with multiple welcome rooms.
//
// inviteUser is the human who should be pinged into the welcome room
// directly (the /project new caller). Pass "" for the auto-join path
// where there's no specific caller.
//
// Failures are logged and swallowed — auto-init is a convenience, not
// a precondition for the Space being usable.
func (b *Bridge) handleSpaceJoinedWithInvite(ctx context.Context, parentSpace, newSpace id.RoomID, spaceName string, inviteUser id.UserID) {
	if b.opts.Manager == nil {
		return
	}
	defaultName := spaceName
	if defaultName == "" {
		defaultName = FoldHomeServer(string(newSpace), b.opts.ServerName)
	}
	created, err := b.opts.Manager.EnsureProject(string(newSpace), defaultName)
	if err != nil {
		log.Printf("[agent] auto-init project for %s failed: %v", newSpace, err)
		return
	}
	if !created {
		// Another agent (or a prior run) already initialised this
		// Space. Nothing to do — welcome room, if any, was their job.
		return
	}
	log.Printf("[agent] auto-initialised project for sub-space %s (name=%q, parent=%s)",
		newSpace, defaultName, parentSpace)

	var invites []id.UserID
	if inviteUser != "" && inviteUser != b.mx.UserID() {
		invites = []id.UserID{inviteUser}
	}

	roomID, err := b.mx.CreateRoom(ctx, matrix.CreateRoomOpts{
		Name:             "welcome",
		Topic:            "🎉 " + defaultName + " — 起步房间",
		ParentSpace:      newSpace,
		Invite:           invites,
		Preset:           "private_chat",
		StrictParentLink: true,
	})
	if err != nil {
		log.Printf("[agent] welcome room creation in space %s failed: %v", newSpace, err)
		// Most common cause: bot lacks PL 50 in the new sub-Space, so
		// linking m.space.child fails. Surface a hint in the parent
		// Org Space (where the bot was originally invited and likely
		// has higher PL — at least the user is watching it) so the
		// user knows what to do next. Best-effort.
		hint := fmt.Sprintf(
			"⚠️ 在新 Space **%s** 里建 welcome room 失败：我在该 Space 没有足够权限"+
				"（需要 Moderator / PL 50 才能挂 child room）。\n\n"+
				"在 Element 里把我提为 Moderator，再重新创建一次该 Space 就会自动建 welcome room。",
			defaultName)
		if _, sendErr := b.mx.SendText(ctx, parentSpace, hint); sendErr != nil {
			log.Printf("[agent] welcome failure hint send to %s failed: %v", parentSpace, sendErr)
		}
		return
	}
	greeting := fmt.Sprintf(
		"👋 欢迎来到 **%s**！\n\n"+
			"这个 room 由 mosaic 自动建好作为项目起步入口。常用动作：\n\n"+
			"- `/project status` — 查当前 Space / cwd\n"+
			"- `/project cwd <path>` — 设置工作目录（admin）\n"+
			"- `/project name <name>` — 改项目名（admin）\n"+
			"- 在 Space 下再开 topic room 即可分线协作\n",
		defaultName)
	if _, err := b.mx.SendText(ctx, roomID, greeting); err != nil {
		log.Printf("[agent] welcome message send to %s failed: %v", roomID, err)
	}
}

// roomSession is the per-room state: a long-lived claude process and
// a serial work queue. Only one turn at a time in any given room —
// claude's stdin/stdout is naturally sequential (one user message →
// one turn → result event), so we mirror that on our side via the
// queue. Messages arriving while a turn is in flight wait their
// place and process FIFO.
type roomSession struct {
	mu     sync.Mutex
	proc   runtime.Process
	cancel context.CancelFunc
	turns  int
	roomID id.RoomID

	// inbox is unbounded in spirit (large buffer) — incoming user
	// messages enqueue here; a single dispatcher goroutine drains
	// them and runs each turn end-to-end before pulling the next.
	inbox chan turnRequest
}

type turnRequest struct {
	sender      id.UserID
	text        string
	attachments []matrix.Attachment
}

func (b *Bridge) handleMessage(ctx context.Context, im matrix.IncomingMessage) {
	roomID := im.RoomID
	sender := im.Sender
	text := im.Text
	if len(im.Attachments) > 0 {
		log.Printf("[agent] %s in %s: %s (+%d attachment)", sender, roomID, truncate(text, 80), len(im.Attachments))
	} else {
		log.Printf("[agent] %s in %s: %s", sender, roomID, truncate(text, 80))
	}

	// Multi-agent routing: in a room with several agents, route by
	// @-mention. If the user explicitly @'d another agent, this one
	// stays silent; if the sender is a peer agent, stay silent unless
	// they @'d us. No mention at all ⇒ broadcast (each agent responds).
	if skip, reason := b.shouldIgnoreForRouting(roomID, im); skip {
		log.Printf("[agent] %s skip in %s: %s", FoldHomeServer(string(b.mx.UserID()), b.opts.ServerName), roomID, reason)
		return
	}

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
		// Belt-and-suspenders: never reply to a peer agent. The mention
		// router above should already drop these, but if it ever fails
		// open (e.g. Manager.List empty during startup) the denial reply
		// becomes ping-pong fuel between two unallowlisted bots.
		if !b.isPeerAgent(sender) {
			_, _ = b.mx.SendText(context.Background(), roomID, fmt.Sprintf(
				"🔒 你（%s）不在本 agent 的访问名单。请联系管理员（%s）加白：`/agent allow %s`",
				FoldHomeServer(string(sender), b.opts.ServerName),
				strings.Join(b.opts.Admins, ", "),
				FoldHomeServer(string(sender), b.opts.ServerName)))
		}
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
	case sess.inbox <- turnRequest{sender: sender, text: text, attachments: im.Attachments}:
	default:
		// Buffer full → tell the user we're swamped rather than block
		// the sync goroutine.
		_, _ = b.mx.SendText(context.Background(), roomID, inboxFullMsg)
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
		go b.runTurn(roomID, b.mx.UserID(), compactPrompt, nil)
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

// isAllowed = admin OR explicitly listed in Members OR a peer agent
// in our fleet. Peer agents are trusted infrastructure (we run them);
// requiring them to be on each other's Members list defeats inter-
// agent collaboration. The mention router already ensures peer
// messages only land here when they explicitly @-mentioned us, so
// open-ended bot spam is bounded by that gate, not this one.
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
	if b.isPeerAgent(sender) {
		return true
	}
	return false
}

// mentionRE matches plain-text `@localpart` tokens in a message body.
// Element's autocomplete pill is reflected in the event's m.mentions
// field directly, but a user (or another bot) who types `@cindy` by
// hand has no metadata — we parse the body to recover that case,
// restricted to known agent localparts so an unrelated `@foo` isn't
// treated as a mention.
var mentionRE = regexp.MustCompile(`@([A-Za-z0-9._=\-+/]+)`)

// localpartOf returns the localpart of a full Matrix user id
// (`@alice:example.org` → `alice`), lowercased.
func localpartOf(u id.UserID) (string, bool) {
	s := string(u)
	if len(s) < 2 || s[0] != '@' {
		return "", false
	}
	end := strings.IndexByte(s, ':')
	if end <= 1 {
		return "", false
	}
	return strings.ToLower(s[1:end]), true
}

// shouldIgnoreForRouting decides whether to silently drop a message
// based on per-room mention routing:
//
//   - if this room contains ≤1 of our fleet's agents (i.e. only me),
//     always respond — single-agent rooms broadcast everything.
//   - in a multi-agent room a human sender must explicitly @ this
//     agent (m.mentions OR plain-text `@localpart`); otherwise stay
//     silent. No @ = no reply, even if no other agent was named.
//   - in a multi-agent room a peer-agent sender must include this
//     agent in the protocol-level m.mentions field (text regex is
//     unsafe for bot-to-bot because reply bodies routinely echo the
//     recipient's @id and would loop).
//
// Falls open (returns false) on any lookup failure — better to over-
// respond than to drop a legit message.
func (b *Bridge) shouldIgnoreForRouting(roomID id.RoomID, im matrix.IncomingMessage) (skip bool, reason string) {
	if b.opts.Manager == nil {
		return false, ""
	}
	me := b.mx.UserID()

	inRoom := b.agentsInRoom(roomID)
	if len(inRoom) <= 1 {
		return false, "" // single-agent room: always reply
	}

	// Peer-agent sender: only m.mentions counts.
	if im.Sender != me && inRoom[im.Sender] {
		for _, u := range im.Mentions {
			if u == me {
				return false, ""
			}
		}
		return true, "peer agent, no explicit @me"
	}

	// Human sender in multi-agent room: require explicit @me. Combine
	// the m.mentions field (Element autocomplete pill) with a regex
	// parse of the body (hand-typed `@cindy`). Mention of *another*
	// agent without including me ⇒ still skip; lack of any mention ⇒
	// also skip (no broadcast in multi-agent rooms).
	for _, u := range im.Mentions {
		if u == me {
			return false, ""
		}
	}
	if myLP, ok := localpartOf(me); ok {
		for _, m := range mentionRE.FindAllStringSubmatch(im.Text, -1) {
			if strings.ToLower(m[1]) == myLP {
				return false, ""
			}
		}
	}
	return true, "multi-agent room, not @me"
}

// agentsInRoom returns the set of our fleet's agents currently joined
// to roomID. Backed by a TTL cache to avoid a homeserver round-trip
// per message. Returns an empty set (treated as "single-agent →
// respond") if the lookup fails.
func (b *Bridge) agentsInRoom(roomID id.RoomID) map[id.UserID]bool {
	if b.opts.Manager == nil {
		return nil
	}
	members := b.roomMembers(roomID)
	if members == nil {
		return nil
	}
	out := map[id.UserID]bool{}
	for _, ai := range b.opts.Manager.List() {
		uid := id.UserID(ai.UserID)
		if uid != "" && members[uid] {
			out[uid] = true
		}
	}
	return out
}

// roomMembers returns the cached joined-members set for roomID,
// refreshing if older than membershipTTL. Nil on lookup failure.
func (b *Bridge) roomMembers(roomID id.RoomID) map[id.UserID]bool {
	b.mu.Lock()
	if e, ok := b.membershipCache[roomID]; ok && time.Since(e.fetched) < membershipTTL {
		b.mu.Unlock()
		return e.members
	}
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	members, err := b.mx.JoinedMemberSet(ctx, roomID)
	if err != nil {
		log.Printf("[agent] joined_members(%s) failed: %v", roomID, err)
		return nil
	}
	b.mu.Lock()
	b.membershipCache[roomID] = &membershipEntry{members: members, fetched: time.Now()}
	b.mu.Unlock()
	return members
}

// invalidateMembership drops the cached membership for roomID. Called
// from join/leave handlers so a follow-on routing decision sees the
// fresh roster.
func (b *Bridge) invalidateMembership(roomID id.RoomID) {
	b.mu.Lock()
	delete(b.membershipCache, roomID)
	b.mu.Unlock()
}

// peerAgentMentionsInBody scans an outbound message body for plain-
// text `@localpart` tokens that resolve to another fleet agent, and
// returns the corresponding Matrix user ids. Used to populate
// m.mentions on agent-generated messages so peer routers fire on
// intentional pings (their bot-to-bot dispatch trusts only the
// protocol field, not text). Returns nil when no peers match.
func (b *Bridge) peerAgentMentionsInBody(body string) []id.UserID {
	if b.opts.Manager == nil || body == "" {
		return nil
	}
	me := b.mx.UserID()
	lp2id := map[string]id.UserID{}
	for _, ai := range b.opts.Manager.List() {
		uid := id.UserID(ai.UserID)
		if uid == "" || uid == me {
			continue
		}
		if lp, ok := localpartOf(uid); ok {
			lp2id[lp] = uid
		}
	}
	if len(lp2id) == 0 {
		return nil
	}
	seen := map[id.UserID]bool{}
	var out []id.UserID
	for _, m := range mentionRE.FindAllStringSubmatch(body, -1) {
		if uid, ok := lp2id[strings.ToLower(m[1])]; ok && !seen[uid] {
			seen[uid] = true
			out = append(out, uid)
		}
	}
	return out
}

// isPeerAgent reports whether sender is a different configured agent
// in our fleet (not us, but in Manager.List()). Used to silence ACL
// denial replies when a peer bot's message slips through the router —
// e.g. before Manager.List() is populated at startup. Plain ACL log
// still fires; only the chat-visible reply is suppressed.
func (b *Bridge) isPeerAgent(sender id.UserID) bool {
	if b.opts.Manager == nil {
		return false
	}
	me := b.mx.UserID()
	if sender == me {
		return false
	}
	for _, ai := range b.opts.Manager.List() {
		if id.UserID(ai.UserID) == sender {
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

	case "new":
		if !b.isAdmin(sender) {
			_, _ = b.mx.SendText(ctx, roomID, "⛔ `/project new` 需要管理员权限")
			return true
		}
		if rest == "" {
			_, _ = b.mx.SendText(ctx, roomID,
				"用法：`/project new <project-name>`\n\n"+
					"机制：bot 直接建一个子 Space（bot 是 creator，PL 100），挂在当前所在的 Org Space 下，"+
					"自动建 welcome room。在 Org Space 自身的 timeline 里发也行。")
			return true
		}
		return b.handleProjectNew(ctx, roomID, sender, rest)

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

// handleProjectNew creates a fresh sub-Space owned by the bot under the
// current room's parent Org Space, then runs the standard space-joined
// init flow (EnsureProject + welcome room). Doing it bot-side dodges
// the PL-50 cliff that hits Element-created sub-Spaces: the bot is
// the Space creator → PL 100 → all subsequent state ops succeed.
//
// Parent resolution walks the parent chain looking for the highest
// Space the bot has PL ≥ 50 in. Rationale: the user typically
// promotes the bot in the Org Space (top-level), not in every
// intermediate Project Space, so picking the immediate parent often
// fails the m.space.child link. Falls back to immediate parent if no
// candidate has sufficient PL — the resulting orphan-from-org Space
// is still usable, just not visible under the Org tree.
func (b *Bridge) handleProjectNew(ctx context.Context, roomID id.RoomID, sender id.UserID, name string) bool {
	parentSpace := b.resolveProjectParent(ctx, roomID)
	if parentSpace == "" {
		_, _ = b.mx.SendText(ctx, roomID,
			"⚠️ 当前 room 不在任何 Space 下，无法决定要把新 project 挂到哪。"+
				"在某个 Org Space 下的 room 里跑这条命令，或直接在 Org Space 的 timeline 里发。")
		return true
	}

	newSpace, parentLinkErr, err := b.mx.CreateSpace(ctx, name, parentSpace, sender)
	if err != nil {
		_, _ = b.mx.SendText(ctx, roomID, "❌ 建 Space 失败："+err.Error())
		return true
	}
	log.Printf("[agent] /project new created sub-space %s (name=%q, parent=%s, link_err=%v)",
		newSpace, name, parentSpace, parentLinkErr)

	// Reuse the auto-init path: EnsureProject + welcome room. Bot is
	// PL 100 in newSpace so every state op inside succeeds. Forward
	// `sender` as an extra invite so they get the welcome room ping
	// even though their newSpace invite is still pending. If the
	// async OnSpaceJoined later fires for the same Space, EnsureProject
	// returns created=false and the side effects no-op.
	b.handleSpaceJoinedWithInvite(ctx, parentSpace, newSpace, name, sender)

	var sb strings.Builder
	fmt.Fprintf(&sb, "✅ 建好 project **%s**\n\n", name)
	fmt.Fprintf(&sb, "- space: `%s`\n", FoldHomeServer(string(newSpace), b.opts.ServerName))
	fmt.Fprintf(&sb, "- parent: `%s`\n", FoldHomeServer(string(parentSpace), b.opts.ServerName))
	if parentLinkErr != nil {
		fmt.Fprintf(&sb, "\n⚠️ 但没能挂到父 Space 下（我在父 Space 没 PL 50）：%v\n\n"+
			"新 Space 现在是顶级独立 Space，你在 Element 的 rooms 列表能看到。"+
			"如果想挂回 Org Space 下，把我在父 Space 里提到 Moderator，再手动 add child。",
			parentLinkErr)
	} else {
		sb.WriteString("\n下面已自动建好 welcome room。")
	}
	_, _ = b.mx.SendText(ctx, roomID, sb.String())
	return true
}

// resolveProjectParent picks the best Space to nest a new project
// Space under, given the room the user ran /project new from. BFS
// walks the inverted m.space.child graph (built from every Space the
// bot is joined to) up to the topmost ancestor and prefers the
// highest one the bot has PL ≥ 50 in.
//
// Why the inverse graph: most clients (Element specifically) only
// publish m.space.child in parents, not m.space.parent in children.
// Walking via ParentSpaces alone breaks at the first Element-added
// nesting and never reaches the Org Space at the root.
//
// Falls back to the closest discovered ancestor on no PL match, and
// to roomID itself when roomID is already a Space.
func (b *Bridge) resolveProjectParent(ctx context.Context, roomID id.RoomID) id.RoomID {
	if isSpace, _ := b.mx.IsSpace(ctx, roomID); isSpace {
		return roomID
	}
	hierarchy, err := b.mx.SpaceHierarchy(ctx)
	if err != nil {
		log.Printf("[agent] resolveProjectParent: hierarchy scan failed: %v", err)
		return ""
	}
	visited := map[id.RoomID]bool{roomID: true}
	frontier := []id.RoomID{roomID}
	var ancestors []id.RoomID
	for len(frontier) > 0 {
		var next []id.RoomID
		for _, r := range frontier {
			for _, p := range hierarchy[r] {
				if visited[p] {
					continue
				}
				visited[p] = true
				ancestors = append(ancestors, p)
				next = append(next, p)
			}
		}
		frontier = next
	}
	if len(ancestors) == 0 {
		return ""
	}
	// ancestors is BFS-ordered (closer first); iterate from the back
	// to prefer the topmost Org Space when bot has PL there.
	for i := len(ancestors) - 1; i >= 0; i-- {
		if pl, err := b.mx.MyPowerLevel(ctx, ancestors[i]); err == nil && pl >= 50 {
			return ancestors[i]
		}
	}
	return ancestors[0]
}

const projectSlashHelp = `**` + "`/project`" + ` 命令家族**

- ` + "`/project status`" + ` — 显示当前 room 的 Space / project / cwd 解析结果
- ` + "`/project list`" + ` — 列出所有已配置 project
- ` + "`/project new <name>`" + ` — bot 自己建子 Space + welcome room（绕过 PL 50 限制）⛔ admin only
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
			_ = s.proc.Close()
		}
		if s.cancel != nil {
			s.cancel()
		}
	}
}

// endSession kills the running coding-agent subprocess for this room
// (if any) and forgets its session id so /resume won't bring it back.
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
			_ = s.proc.Close()
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

// toRuntimeAttachments converts matrix.Attachment values to the
// runtime package's shape, dropping the matrix package import from
// every driver. nil-in/nil-out.
func toRuntimeAttachments(in []matrix.Attachment) []runtime.Attachment {
	if len(in) == 0 {
		return nil
	}
	out := make([]runtime.Attachment, 0, len(in))
	for _, a := range in {
		out = append(out, runtime.Attachment{
			Path:     a.Path,
			MimeType: a.MimeType,
			Kind:     a.Kind,
			Filename: a.Filename,
		})
	}
	return out
}

func (b *Bridge) runTurn(roomID id.RoomID, sender id.UserID, text string, attachments []matrix.Attachment) {
	ctx := context.Background()
	log.Printf("[agent] runTurn start in %s", roomID)

	sess := b.getOrCreate(ctx, roomID)
	if sess == nil || sess.proc == nil {
		log.Printf("[agent] no agent proc available for %s — aborting turn", roomID)
		_, _ = b.mx.SendText(ctx, roomID, "❌ failed to start agent (see agent logs)")
		return
	}
	sess.mu.Lock()
	sess.turns++
	turnIdx := sess.turns
	sess.mu.Unlock()

	rmsg := runtime.Message{Text: text, Attachments: toRuntimeAttachments(attachments)}
	log.Printf("[agent] sending message to agent (turn %d, %d bytes, %d att)", turnIdx, len(text), len(rmsg.Attachments))
	if err := sess.proc.Send(rmsg); err != nil {
		// Most common: claude died between turns and stdin pipe is
		// closed ("write |1: file already closed"). Evict the dead
		// session and respawn — the new session will --resume the
		// same session_id, preserving conversation memory.
		log.Printf("[agent] proc.Send failed: %v — evicting + respawning", err)
		b.evictSession(roomID)
		sess = b.getOrCreate(ctx, roomID)
		if sess == nil || sess.proc == nil {
			_, _ = b.mx.SendText(ctx, roomID, "❌ agent 重启失败（见 agent log），请稍后重试")
			return
		}
		sess.mu.Lock()
		sess.turns++
		turnIdx = sess.turns
		sess.mu.Unlock()
		if err2 := sess.proc.Send(rmsg); err2 != nil {
			log.Printf("[agent] retry Send still failed: %v", err2)
			_, _ = b.mx.SendText(ctx, roomID, "❌ agent 仍无法接收消息："+err2.Error())
			return
		}
		log.Printf("[agent] respawned agent session, retry sent (turn %d)", turnIdx)
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
			// Keep the typing indicator alive even when the agent is
			// silent (long tool execution, slow LLM). flushPendingText
			// early-returns on an unchanged buffer, so refresh has to
			// run independently here — not after.
			t.refreshTypingIfStale(ctx)
			t.flushPendingText(ctx, false)
		case ev, ok := <-sess.proc.Events():
			if !ok {
				log.Printf("[agent] turn %d in %s: agent EOF — evicting session", turnIdx, roomID)
				b.evictSession(roomID)
				_, _ = b.mx.SendText(ctx, roomID, "❌ agent 进程退出（已清理本会话状态，下条消息将自动 resume 重启）")
				return
			}
			done := t.consume(ctx, ev)
			if done {
				t.flushPendingText(ctx, true)
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

	typing       bool
	lastTypingAt time.Time
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
		t.lastTypingAt = time.Now()
	}
}

// refreshTypingIfStale re-emits the typing notification when the last
// one is close to the 30s server-side expiry. Called inline before
// each outbound message so long turns (multi-tool, slow LLM) keep the
// "is typing…" indicator alive without a background goroutine. The
// 25s threshold leaves 5s of headroom for the EDU to actually land
// at the homeserver before the previous one lapses.
func (t *turn) refreshTypingIfStale(ctx context.Context) {
	if !t.typing {
		return
	}
	if time.Since(t.lastTypingAt) < 25*time.Second {
		return
	}
	if err := t.b.mx.Typing(ctx, t.roomID, true, 30000); err == nil {
		t.lastTypingAt = time.Now()
	}
}

// flushPendingText sends or edits the streaming text message with the
// current pending body. No-op when nothing new since last flush.
//
// Auto-populates m.mentions for any peer-agent `@localpart` tokens in
// the body. Peer routers ignore edit events (they only inspect the
// INITIAL m.room.message), so a streamed message whose `@peer` token
// appears late in the body would otherwise reach the peer without
// mention metadata. In multi-agent rooms we therefore defer ALL
// flushes until finalize, so the whole text goes out as one event
// with complete m.mentions — at the cost of no streaming UX, which
// is acceptable in collaborative rooms. Single-agent rooms keep the
// 200ms streaming behaviour.
func (t *turn) flushPendingText(ctx context.Context, final bool) {
	body := t.pending.String()
	if body == t.lastFlushed {
		return
	}
	mentions := t.b.peerAgentMentionsInBody(body)
	if !final && t.textEvent == "" && len(t.b.agentsInRoom(t.roomID)) > 1 {
		// Deferred: hold until finalizeText so the initial send
		// carries the full body + m.mentions atomically.
		return
	}
	t.refreshTypingIfStale(ctx)
	if t.textEvent == "" {
		evID, err := t.b.mx.SendTextMentions(ctx, t.roomID, body, mentions)
		if err != nil {
			// Don't advance lastFlushed: next tick will retry the
			// same (or grown) body rather than treating this tail
			// as "already delivered".
			log.Printf("[agent] send streaming text failed: %v", err)
			return
		}
		t.textEvent = evID
		t.lastFlushed = body
		return
	}
	if err := t.b.mx.EditTextMentions(ctx, t.roomID, t.textEvent, body, mentions); err != nil {
		log.Printf("[agent] edit streaming text failed: %v", err)
		return
	}
	t.lastFlushed = body
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
	t.flushPendingText(ctx, true)
	if canonical != "" {
		t.lastFinalText = canonical
	}
	t.textEvent = ""
	t.pending.Reset()
	t.lastFlushed = ""
}

// consume one normalized runtime.Event. Returns done=true on
// TurnDone (end of turn). Side effects: sends/edits Matrix messages,
// updates t.lastFinalText / t.lastSessionID / t.toolCount.
func (t *turn) consume(ctx context.Context, ev runtime.Event) bool {
	switch e := ev.(type) {
	case runtime.TextDelta:
		t.pending.WriteString(e.Text)
		return false

	case runtime.TextFinal:
		// Extract any ask_user protocol block before finalizing — the
		// raw <ask_user> envelope is bridge plumbing and shouldn't
		// reach the user. Two paths:
		//   - Some prose remains around the envelope → finalize with
		//     the cleaned body (Element edits the streamed bubble).
		//   - The reply was nothing but the envelope → redact the
		//     streamed bubble so only the rendered question card
		//     stays visible. finalizeText("") can't help here: it's
		//     guarded against blank canonical, and even if we passed
		//     a single space the empty bubble is uglier than a clean
		//     redact.
		body, asks := extractAskUserBlocks(e.Body)
		if len(asks) > 0 && strings.TrimSpace(body) == "" && t.textEvent != "" {
			if err := t.b.mx.Redact(ctx, t.roomID, t.textEvent, "ask_user envelope"); err != nil {
				log.Printf("[agent] redact ask-only streamed bubble failed: %v", err)
				// Fallback: edit to a single space so the raw block at
				// least doesn't shout. Element renders this as a thin
				// empty bubble — strictly less bad than leaving the
				// envelope inline.
				_ = t.b.mx.EditText(ctx, t.roomID, t.textEvent, " ")
			}
			t.textEvent = ""
			t.pending.Reset()
			t.lastFlushed = ""
			t.lastFinalText = ""
		} else {
			t.finalizeText(ctx, body)
		}
		for _, ask := range asks {
			t.renderAskUser(ctx, ask)
		}
		return false

	case runtime.Thinking:
		return false

	case runtime.ToolUse:
		// Close any in-flight streaming text first so timeline order
		// is preserved.
		t.finalizeText(ctx, "")
		t.toolCount++
		if t.b.opts.IgnoreToolsMsg[strings.ToLower(e.Name)] {
			return false
		}
		body := FormatToolUse(e.Name, e.Input)
		t.refreshTypingIfStale(ctx)
		if _, err := t.b.mx.SendText(ctx, t.roomID, body); err != nil {
			log.Printf("[agent] send tool_use msg failed: %v", err)
		}
		return false

	case runtime.ToolResult:
		if !e.IsError {
			return false
		}
		name := e.ToolName
		if name == "" {
			name = "tool"
		}
		body := FormatToolResult(name, e.Content, true)
		if body != "" {
			t.refreshTypingIfStale(ctx)
			_, _ = t.b.mx.SendText(ctx, t.roomID, body)
		}
		return false

	case runtime.ImageFinal:
		// Close any in-flight streaming text so the image appears
		// after the surrounding prose, not above it.
		t.finalizeText(ctx, "")
		t.refreshTypingIfStale(ctx)
		if _, err := t.b.mx.SendImage(ctx, t.roomID, e.Path, e.MimeType, e.Caption); err != nil {
			log.Printf("[agent] send image %s failed: %v", e.Path, err)
		}
		return false

	case runtime.SessionInfo:
		t.lastSessionID = e.SessionID
		return false

	case runtime.TurnDone:
		if e.Err != "" || e.Reason != "" {
			t.refreshTypingIfStale(ctx)
			_, _ = t.b.mx.SendText(ctx, t.roomID, formatTurnError(e))
		}
		return true
	}
	return false
}

// formatTurnError renders a TurnDone failure into a chat-friendly
// message. We don't auto-retry — these errors can be systematic
// (rate limit, exhausted tools) and a retry loop could burn cost.
func formatTurnError(td runtime.TurnDone) string {
	switch td.Reason {
	case "max_turns":
		return "❌ 触达 turn 上限。一次会话内的工具调用回合数有限，可能任务过于复杂。建议 `/compact` 总结后重起。"
	case "max_tokens":
		return "❌ 输出 token 上限。这一轮太长了——可让 agent 分步输出，或调小请求范围。"
	case "rate_limit":
		return "❌ 上游限流。稍后重发。"
	default:
		if td.Err != "" {
			return "❌ agent 执行出错：" + td.Err
		}
		return "❌ agent 未知错误"
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
	// askUserProtocol is always appended (including on resume) so the
	// runtime always knows the convention even after long-running
	// sessions where the original directive may have aged out.
	if appendSP != "" {
		appendSP += "\n\n"
	}
	appendSP += askUserProtocol
	rt := b.opts.Runtime
	if rt == "" {
		rt = "claude"
	}
	drv, err := runtime.Get(rt)
	if err != nil {
		cancel()
		log.Printf("[agent] get runtime driver failed: %v", err)
		return &roomSession{cancel: cancel}
	}
	log.Printf("[agent] spawning %s (cwd=%s model=%q effort=%q resume=%q sysPromptLen=%d envKeys=%d)", rt, cwd, model, b.opts.Effort, resume, len(appendSP), len(b.opts.Env))
	proc, err := drv.Spawn(procCtx, runtime.Options{
		Cwd:                cwd,
		Model:              model,
		Effort:             b.opts.Effort,
		PermissionMode:     b.opts.PermissionMode,
		Binary:             b.opts.Binary,
		Resume:             resume,
		ExtraEnv:           envMapToSlice(b.opts.Env),
		AppendSystemPrompt: appendSP,
	})
	if err != nil {
		cancel()
		log.Printf("[agent] spawn %s failed: %v", rt, err)
		return &roomSession{cancel: cancel}
	}
	scope := "no-project"
	if r.ProjectName != "" {
		scope = "project=" + r.ProjectName
	} else if r.SpaceID != "" {
		scope = "space=" + string(r.SpaceID)
	}
	if resume != "" {
		log.Printf("[agent] resumed %s session %s for room %s (cwd=%s, %s)", rt, resume, roomID, cwd, scope)
	} else {
		log.Printf("[agent] spawned fresh %s session for room %s (cwd=%s, %s)", rt, roomID, cwd, scope)
	}

	// Re-acquire the lock to register the session, racing-aware: if
	// another goroutine spawned one for the same room while we were
	// in resolve / Spawn, prefer the existing one and tear ours down.
	b.mu.Lock()
	if existing, ok := b.sessions[roomID]; ok {
		b.mu.Unlock()
		log.Printf("[agent] race: another goroutine spawned for %s; closing duplicate", roomID)
		_ = proc.Close()
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
		b.runTurn(s.roomID, req.sender, req.text, req.attachments)
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
