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
	"bufio"
	"context"
	"encoding/json"
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
	"github.com/deng00/mosaic/pkg/export"
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

	// Admins are full Matrix user IDs allowed to interact with the bot
	// at all. Two things at once:
	//   1. only admins (and peer agents in the fleet) can chat with
	//      the bot — everyone else's messages are silently dropped at
	//      handleMessage entry.
	//   2. mutating slash commands (`/agent new`, `/project cwd`,
	//      `/export`, …) are gated to admins; peer-agent senders pass
	//      the gate-1 check but still can't run admin-only slashes.
	Admins []string

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

	// DataDir is the daemon-level data root (fc.DataDir). The bridge
	// only uses it to derive sibling subdirectories like
	// `<DataDir>/exports/` for /export output. Per-agent / per-project
	// state has its own dedicated fields (Memory, Sessions); don't
	// pile new file-backed state onto DataDir without thinking about
	// the agents/projects split documented in CLAUDE.md.
	DataDir string
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

// pendingAsk tracks an open question awaiting a vote. eventID is the
// poll-start event the bot posted; handlePollResponse matches inbound
// m.poll.response events against it and looks up options[idx-1] for
// the chosen answer id ("opt-N"). Resolution injects the chosen text
// as a fresh user turn.
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
	b.mx.OnPollResponse(b.handlePollResponse)
}

// askUserDeferredProtocol tells the model how Mosaic handles the
// AskUserQuestion tool. The SDK auto-fails this tool in headless mode
// (no TTY); without this directive claude reads the error and just
// keeps going on its own assumptions, so the user's later poll vote
// arrives mid-other-work and confuses the conversation.
//
// We instead want claude to STOP the moment the failure arrives and
// wait for the user's answer to surface as a fresh user turn (the
// bridge injects the chosen option text via handlePollResponse). End
// the current turn cleanly — no further tool calls, no preemptive
// follow-up — so the next user turn lands on a clean conversational
// boundary.
const askUserDeferredProtocol = `## Mosaic protocol: AskUserQuestion is deferred

When you call the AskUserQuestion tool, Mosaic intercepts it and
renders the question as an interactive poll for the user. The tool
itself fails with an error tool_result (Mosaic runs headless, no
TTY) — this failure is EXPECTED and is NOT a signal to proceed
without the user's answer.

On seeing the AskUserQuestion error tool_result:
- Do NOT call any further tools in this turn.
- Do NOT speculate, retry, or proceed with assumed defaults.
- End the turn immediately with at most a single short
  acknowledgement (e.g. "等你的选择" / "waiting for your pick"),
  or end silently — either is fine.
- The user's choice arrives later as a regular user message
  containing the picked option's text verbatim. Treat that as the
  answer to the question and resume work from there.`

// parseAskUserQuestionTool decodes the AskUserQuestion tool_use input
// into an askUser. Only the first question is rendered today
// (questions[1..] are ignored — the emoji-reaction UI handles one at
// a time). multiSelect is dropped on the floor; we always treat the
// pick as single-select. The auto-appended "Other" free-text option
// is not surfaced (no free-text input via reactions).
//
// Schema (per the SDK tool def):
//
//	{
//	  "questions": [
//	    { "question": "...",
//	      "options": [ {"label": "...", ...} ] }
//	  ]
//	}
func parseAskUserQuestionTool(raw json.RawMessage) (askUser, bool) {
	var v struct {
		Questions []struct {
			Question string `json:"question"`
			Options  []struct {
				Label string `json:"label"`
			} `json:"options"`
		} `json:"questions"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return askUser{}, false
	}
	if len(v.Questions) == 0 {
		return askUser{}, false
	}
	q := v.Questions[0]
	if q.Question == "" || len(q.Options) < 2 {
		return askUser{}, false
	}
	opts := make([]string, 0, len(q.Options))
	for _, o := range q.Options {
		if o.Label == "" {
			continue
		}
		opts = append(opts, o.Label)
		if len(opts) >= 10 { // poll UI caps at 10 buttons
			break
		}
	}
	if len(opts) < 2 {
		return askUser{}, false
	}
	return askUser{question: q.Question, options: opts}, true
}

// askUser captures one parsed prompt from an `AskUserQuestion`
// tool_use. Feeds the poll renderer.
type askUser struct {
	question string
	options  []string
}

// renderAskUser posts the prompt as a native Matrix poll
// (m.poll.start, MSC3381 unstable). Element / Element X render this
// with built-in click-to-vote buttons. Resolution arrives via
// handlePollResponse, which references the poll's start event id —
// stored here as pendingAsk.eventID.
//
// Option IDs are "opt-1".."opt-N" so the response handler can recover
// the chosen index without a separate id→index map.
func (t *turn) renderAskUser(ctx context.Context, a askUser) {
	answers := make([]matrix.PollAnswer, 0, len(a.options))
	for i, opt := range a.options {
		answers = append(answers, matrix.PollAnswer{
			ID:   askOptionID(i + 1),
			Text: opt,
		})
	}
	evID, err := t.b.mx.SendPollStart(ctx, t.roomID, a.question, answers)
	if err != nil {
		log.Printf("[agent] ask_user poll send failed: %v", err)
		return
	}
	t.b.mu.Lock()
	t.b.pendingAsks[t.roomID] = &pendingAsk{
		eventID: evID,
		options: a.options,
	}
	t.b.mu.Unlock()
}

// askOptionID returns the deterministic id for option i (1-based).
// Kept short and predictable so logs are readable.
func askOptionID(i int) string {
	return fmt.Sprintf("opt-%d", i)
}

// askOptionIndex is the inverse of askOptionID. Returns 0 (1-based
// would be 1+) when the id isn't one of ours.
func askOptionIndex(id string) int {
	var n int
	if _, err := fmt.Sscanf(id, "opt-%d", &n); err != nil {
		return 0
	}
	return n
}

// handlePollResponse is the org.matrix.msc3381.poll.response handler.
// Resolves the pending ask in roomID, closes the poll so Element
// greys out the buttons, and injects the chosen option as a fresh
// user turn. First valid response wins.
func (b *Bridge) handlePollResponse(ctx context.Context, roomID id.RoomID, pollStartID id.EventID, sender id.UserID, answerIDs []string) {
	if len(answerIDs) == 0 {
		return
	}
	idx := askOptionIndex(answerIDs[0])
	if idx == 0 {
		return
	}
	b.mu.Lock()
	pending, ok := b.pendingAsks[roomID]
	if !ok || pending.eventID != pollStartID {
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
	log.Printf("[agent] ask_user (poll) resolved in %s: %s → %q", roomID, answerIDs[0], truncate(chosen, 80))

	// Close the poll so Element greys out the buttons and shows the
	// result; the summary text is the fallback m.text for non-poll-
	// aware clients.
	if err := b.mx.SendPollEnd(ctx, roomID, pollStartID, fmt.Sprintf("Poll closed — %s", chosen)); err != nil {
		log.Printf("[agent] poll.end send failed in %s: %v", roomID, err)
	}

	// Acknowledge in chat (the vote in Element is visible but quiet),
	// then enqueue as a fresh user turn. Same downstream path as the
	// envelope flow.
	_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf("> %s", chosen))

	sess := b.getOrCreate(ctx, roomID)
	if sess == nil || sess.inbox == nil {
		log.Printf("[agent] ask_user (poll) could not enqueue (no inbox) in %s", roomID)
		return
	}
	select {
	case sess.inbox <- turnRequest{sender: sender, text: chosen}:
	default:
		log.Printf("[agent] ask_user (poll) inbox full in %s", roomID)
		_, _ = b.mx.SendText(context.Background(), roomID, inboxFullMsg)
	}
}

// handleSpaceJoined runs after the bot auto-joins a fresh sub-Space
// (a child Space of an existing Space the bot is in). Auto-join has
// no human "trigger" — passes "" so the default rooms exist as Space
// children only, visible after the user joins the Space in Element.
func (b *Bridge) handleSpaceJoined(ctx context.Context, parentSpace, newSpace id.RoomID, spaceName string) {
	b.handleSpaceJoinedWithInvite(ctx, parentSpace, newSpace, spaceName, "")
}

// handleSpaceJoinedWithInvite is the shared post-Space-join init.
// Two idempotent side effects:
//
//  1. EnsureProject inserts a project entry keyed by the new Space's
//     ID, defaulting the project name to the Space's m.room.name.
//     Only the first agent to win the race gets created=true.
//  2. The winning agent creates the default topic-rooms (see
//     defaultProjectRooms) as children of the new Space. Other agents
//     no-op so we don't end up with duplicate rooms.
//
// inviteUser is the human who should be pinged into the default rooms
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
		// Space. Nothing to do — default rooms, if any, were their job.
		return
	}
	log.Printf("[agent] auto-initialised project for sub-space %s (name=%q, parent=%s)",
		newSpace, defaultName, parentSpace)

	var invites []id.UserID
	if inviteUser != "" && inviteUser != b.mx.UserID() {
		invites = []id.UserID{inviteUser}
	}

	for i, r := range defaultProjectRooms {
		// Space + every room is a separate state-write burst. Synapse's
		// rc_message / rc_joins / rc_create_room limits will start
		// dropping requests if we fire them back-to-back — historically
		// we saw only the first 1–2 default rooms appear before the
		// rest silently failed. 200ms between rooms keeps us under
		// every commonly-configured limit.
		if i > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		_, err := b.mx.CreateRoom(ctx, matrix.CreateRoomOpts{
			Name:             r.name,
			Topic:            r.topic,
			ParentSpace:      newSpace,
			Invite:           invites,
			Preset:           "private_chat",
			StrictParentLink: true,
		})
		if err != nil {
			log.Printf("[agent] default room %q in space %s failed: %v", r.name, newSpace, err)
			// Two failure modes hit this branch with very different fixes,
			// so route the hint by error code:
			//   - M_LIMIT_EXCEEDED: Synapse rc_* throttling — transient,
			//     just retry. Likely when N rooms are being created
			//     back-to-back; the 200ms between CreateRoom calls above
			//     usually keeps us under the limit, but bursts elsewhere
			//     in the same minute can still trip it.
			//   - everything else (typically M_FORBIDDEN): bot lacks PL 50
			//     in newSpace, can't write m.space.child. Real fix is to
			//     promote the bot to Moderator. Only happens on the
			//     auto-join path (bot joining a Space someone else built);
			//     /project new creates the Space itself so the bot is
			//     PL 100 there.
			// Bail on the first failure either way — both modes affect
			// every subsequent room in the loop. The hint posts into the
			// parent Org Space, where the user is watching.
			var hint string
			if matrix.IsRateLimited(err) {
				hint = fmt.Sprintf(
					"⚠️ 在新 Space **%s** 里建默认 room `%s` 被 Synapse 限流（`M_LIMIT_EXCEEDED`）。\n\n"+
						"稍等十几秒后重发 `/project new %s`，或在该 Space 里手动 add child room。",
					defaultName, r.name, defaultName)
			} else {
				hint = fmt.Sprintf(
					"⚠️ 在新 Space **%s** 里建默认 room `%s` 失败：%v\n\n"+
						"常见原因：我在该 Space 不是 Moderator（需要 PL ≥ 50 才能挂 child room）。"+
						"在 Element 里把我提为 Moderator，再重新创建一次该 Space 就会自动建好默认 rooms。",
					defaultName, r.name, err)
			}
			if _, sendErr := b.mx.SendText(ctx, parentSpace, hint); sendErr != nil {
				log.Printf("[agent] default-room failure hint send to %s failed: %v", parentSpace, sendErr)
			}
			return
		}
	}
}

// defaultProjectRooms is the canonical set of topic rooms created
// inside a freshly-initialised project Space. Order is the order they
// appear under the Space in Element.
var defaultProjectRooms = []struct {
	name  string
	topic string
}{
	{"dev", "开发讨论：代码 / PR / bug / 功能 / 测试"},
	{"deploy", "部署 / CI/CD / 发布"},
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
	// compact marks this request as the /compact summarization turn:
	// dispatchLoop sets pendingCompact just before runTurn fires, so the
	// "save final text to SUMMARY.md + end session" hook in runTurn
	// triggers on this turn and only this turn. Setting the flag at
	// enqueue time would let a preceding user-message turn consume it
	// instead and save the wrong body.
	compact bool
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

	// Sender allowlist: only configured admins and peer agents in our
	// fleet are allowed to drive the bot. Everyone else is silently
	// dropped (no chat-visible refusal — avoid leaking presence and
	// avoid bot-to-stranger loops). Matrix-layer self-echo filter
	// already handles `sender == me` upstream.
	if !b.isAdmin(sender) && !b.isPeerAgent(sender) {
		log.Printf("[agent] drop %s in %s: not admin/peer-agent", sender, roomID)
		return
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
	// A new user turn supersedes any pending poll in this room — close
	// it so a late accidental vote doesn't inject a stale turn after
	// the conversation has moved on.
	b.cancelPendingAsk(context.Background(), roomID, "superseded by new message")

	select {
	case sess.inbox <- turnRequest{sender: sender, text: text, attachments: im.Attachments}:
	default:
		// Buffer full → tell the user we're swamped rather than block
		// the sync goroutine.
		_, _ = b.mx.SendText(context.Background(), roomID, inboxFullMsg)
	}
}

// cancelPendingAsk closes any open poll/pending-ask in roomID. Drops
// the in-memory pendingAsks entry first (so the response handler
// short-circuits on a race), then emits poll.end so Element greys
// out the buttons. Both steps are best-effort; no-op when there's
// no pending ask.
func (b *Bridge) cancelPendingAsk(ctx context.Context, roomID id.RoomID, reason string) {
	b.mu.Lock()
	pending, ok := b.pendingAsks[roomID]
	if ok {
		delete(b.pendingAsks, roomID)
	}
	b.mu.Unlock()
	if !ok || pending == nil {
		return
	}
	summary := "Poll closed"
	if reason != "" {
		summary = "Poll closed — " + reason
	}
	if err := b.mx.SendPollEnd(ctx, roomID, pending.eventID, summary); err != nil {
		log.Printf("[agent] cancelPendingAsk: poll.end failed in %s: %v", roomID, err)
	}
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
	case "/session":
		return b.handleSessionSlash(ctx, roomID, rest)
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
	case "/export":
		b.handleExportSlash(ctx, roomID, sender, rest)
		return true
	case "/compact":
		// Inject the summarization prompt as a fresh turn through the
		// per-room inbox so it serialises with any in-flight turn —
		// running it as `go b.runTurn(...)` would race-drain the same
		// proc.Events() channel as the active dispatchLoop and produce
		// scrambled output. dispatchLoop sets pendingCompact right
		// before runTurn fires, so the "save SUMMARY.md + endSession"
		// hook latches onto this specific turn.
		sess := b.getOrCreate(context.Background(), roomID)
		if sess == nil || sess.inbox == nil {
			_, _ = b.mx.SendText(ctx, roomID,
				"❌ 起 claude 子进程失败，无法 /compact。先 `!project status` 看 cwd 是否有效。")
			return true
		}
		select {
		case sess.inbox <- turnRequest{sender: b.mx.UserID(), text: compactPrompt, compact: true}:
			_, _ = b.mx.SendText(ctx, roomID, "🗜️ 已排队生成会话摘要（前面如果还有未完成的回合会先跑完，再做摘要并清空 LLM 上下文）...")
		default:
			_, _ = b.mx.SendText(ctx, roomID, inboxFullMsg)
		}
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
		// Auto-create the directory if missing — saves a "ssh in + mkdir"
		// trip when bootstrapping a new project. Resolve ~ first so we
		// don't try to mkdir a literal "~"; the config keeps the
		// unexpanded form so it stays portable across hosts.
		abs := expandHome(rest)
		if info, err := os.Stat(abs); err != nil {
			if !os.IsNotExist(err) {
				_, _ = b.mx.SendText(ctx, roomID, "❌ 检查路径失败："+err.Error())
				return true
			}
			if err := os.MkdirAll(abs, 0o755); err != nil {
				_, _ = b.mx.SendText(ctx, roomID, "❌ 创建目录失败："+err.Error())
				return true
			}
			_, _ = b.mx.SendText(ctx, roomID, "📁 已创建目录 `"+abs+"`")
		} else if !info.IsDir() {
			_, _ = b.mx.SendText(ctx, roomID, "❌ 路径已存在但不是目录：`"+abs+"`")
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
					"自动建默认 rooms（git / deploy / bugs / feature / test）。在 Org Space 自身的 timeline 里发也行。")
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

	// Setting cwd on a project that has no name yet ⇒ derive a name
	// from the cwd's last path component (e.g. `/srv/work/argus` →
	// "argus"). Saves an extra `/project name` round-trip for the
	// common case of "first-time setup just gave me a path". Only
	// fills when the existing name is blank; an explicit name passed
	// in this call wins as usual.
	if name == "" && cwd != "" {
		existing := ""
		for _, p := range b.opts.Manager.Projects() {
			if p.SpaceID == spaceID {
				existing = p.Name
				break
			}
		}
		if existing == "" {
			if base := filepath.Base(strings.TrimRight(expandHome(cwd), "/")); base != "" && base != "." && base != "/" {
				name = base
			}
		}
	}

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
		"✅ 已更新 project (`%s`) %s\n\n下次该 Space 下任意 room **新起的 claude session** 立即生效。当前 session 仍用旧 cwd——发 `/session new` 强制刷新。",
		FoldHomeServer(spaceID, b.opts.ServerName), strings.Join(parts, "  ·  ")))
	return true
}

// handleProjectNew creates a fresh sub-Space owned by the bot under the
// current room's parent Org Space, then runs the standard space-joined
// init flow (EnsureProject + default rooms). Doing it bot-side dodges
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
	parentSpace, topmost, err := b.resolveProjectParent(ctx, roomID)
	if err != nil {
		_, _ = b.mx.SendText(ctx, roomID, "❌ 解析父 Space 失败："+err.Error())
		return true
	}
	if parentSpace == "" && topmost == "" {
		_, _ = b.mx.SendText(ctx, roomID,
			"⚠️ 当前 room 不在任何 Space 下，无法决定要把新 project 挂到哪。"+
				"在某个 Org Space 下的 room 里跑这条命令，或直接在 Org Space 的 timeline 里发。")
		return true
	}
	if parentSpace == "" {
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf(
			"⛔ 我在父 Space `%s` 里不是 Moderator/Admin（PL < 50），无法在它下面建子 Space。\n\n"+
				"在 Element 里打开那个 Space → 右上角 People → 找到我（`%s`）→ Promote 到 **Moderator**（PL 50）或 **Admin**（PL 100），然后重试 `/project new %s`。",
			FoldHomeServer(string(topmost), b.opts.ServerName),
			FoldHomeServer(string(b.mx.UserID()), b.opts.ServerName),
			name))
		return true
	}
	// Guard: refuse to nest under a Space we've already registered as a
	// project. That's the classic "bot didn't join the Org Space" trap —
	// SpaceHierarchy can only see Spaces the bot is in, so BFS stops at
	// the highest one it can reach. If that's another project Space, the
	// new sub-Space would dangle under a sibling project rather than the
	// real Org Space. Refuse loudly and tell the user to invite the bot
	// into the Org Space.
	if _, isProject := b.opts.Projects[string(parentSpace)]; isProject {
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf(
			"⛔ 我视野里最顶层的 Space `%s` 是另一个 project，不是 Org Space。\n\n"+
				"大概率是我没加入你的 Org Space —— Matrix 协议下我看不到没加入的 Space 的 child 关系，BFS 就在这里断了。\n\n"+
				"修法：在 Element 里把我（`%s`）邀请进 Org Space，并 Promote 到 **Moderator**（PL ≥ 50），然后重试 `/project new %s`。",
			FoldHomeServer(string(parentSpace), b.opts.ServerName),
			FoldHomeServer(string(b.mx.UserID()), b.opts.ServerName),
			name))
		return true
	}

	newSpace, parentLinkErr, err := b.mx.CreateSpace(ctx, name, parentSpace, sender)
	if err != nil {
		_, _ = b.mx.SendText(ctx, roomID, "❌ 建 Space 失败："+err.Error())
		return true
	}
	log.Printf("[agent] /project new created sub-space %s (name=%q, parent=%s, link_err=%v)",
		newSpace, name, parentSpace, parentLinkErr)

	// Reuse the auto-init path: EnsureProject + default rooms. Bot is
	// PL 100 in newSpace so every state op inside succeeds. Forward
	// `sender` as an extra invite so they get the default-room pings
	// even though their newSpace invite is still pending. If the
	// async OnSpaceJoined later fires for the same Space, EnsureProject
	// returns created=false and the side effects no-op.
	b.handleSpaceJoinedWithInvite(ctx, parentSpace, newSpace, name, sender)

	var sb strings.Builder
	fmt.Fprintf(&sb, "✅ 建好 project **%s**\n\n", name)
	fmt.Fprintf(&sb, "- space: `%s`\n", FoldHomeServer(string(newSpace), b.opts.ServerName))
	fmt.Fprintf(&sb, "- parent: `%s`\n", FoldHomeServer(string(parentSpace), b.opts.ServerName))
	if parentLinkErr != nil {
		fmt.Fprintf(&sb, "\n⚠️ 但没能挂到父 Space 下（预检通过却写 child 失败，多半是瞬时错误）：%v\n\n"+
			"新 Space 现在是顶级独立 Space，你在 Element 的 rooms 列表能看到。"+
			"在父 Space 里手动 add child 即可挂回去；或重试 `/project new`。",
			parentLinkErr)
	} else {
		sb.WriteString("\n下面已自动建好默认 rooms：`dev` / `deploy`。")
	}
	// The room-creation burst above (CreateSpace + N×CreateRoom) easily
	// trips Synapse rc_message/rc_joins limits — without this breather
	// the feedback SendText silently 429s and the user sees the new
	// Space + rooms in their sidebar but no confirmation message.
	time.Sleep(200 * time.Millisecond)
	if _, err := b.mx.SendText(ctx, roomID, sb.String()); err != nil {
		log.Printf("[agent] /project new feedback send to %s failed: %v", roomID, err)
	}
	return true
}

// resolveProjectParent picks the Space to nest a new project Space
// under, given the room the user ran /project new from. BFS walks the
// inverted m.space.child graph (built from every Space the bot is
// joined to) and returns the *single* topmost ancestor — no fallback
// to closer ancestors. A prior version preferred any PL ≥ 50 ancestor
// scanning from the top down; that silently picked a sibling project
// Space when the bot wasn't in the real Org Space, dangling new
// projects under a peer instead of the root.
//
// Why the inverse graph: most clients (Element specifically) only
// publish m.space.child in parents, not m.space.parent in children.
// Walking via ParentSpaces alone breaks at the first Element-added
// nesting and never reaches the Org Space at the root.
//
// Return contract:
//   - parent != "" → topmost ancestor the bot has PL ≥ 50 in; caller
//     should still verify it isn't one of our own registered project
//     Spaces (that's the "bot didn't join the Org Space" trap, handled
//     in handleProjectNew).
//   - parent == "", topmost == "" → no ancestor Spaces at all (room
//     not in any Space).
//   - parent == "", topmost != "" → topmost exists but bot lacks
//     PL ≥ 50 there. Caller names topmost in the promotion prompt.
func (b *Bridge) resolveProjectParent(ctx context.Context, roomID id.RoomID) (parent, topmost id.RoomID, err error) {
	if isSpace, _ := b.mx.IsSpace(ctx, roomID); isSpace {
		if pl, e := b.mx.MyPowerLevel(ctx, roomID); e == nil && pl >= 50 {
			return roomID, roomID, nil
		}
		return "", roomID, nil
	}
	hierarchy, hErr := b.mx.SpaceHierarchy(ctx)
	if hErr != nil {
		return "", "", fmt.Errorf("hierarchy scan: %w", hErr)
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
		return "", "", nil
	}
	topmost = ancestors[len(ancestors)-1]
	if pl, e := b.mx.MyPowerLevel(ctx, topmost); e == nil && pl >= 50 {
		parent = topmost
	}
	return parent, topmost, nil
}

const projectSlashHelp = `**` + "`/project`" + ` 命令家族**

- ` + "`/project status`" + ` — 显示当前 room 的 Space / project / cwd 解析结果
- ` + "`/project list`" + ` — 列出所有已配置 project
- ` + "`/project new <name>`" + ` — bot 自己建子 Space + 默认 rooms（git/deploy/bugs/feature/test，绕过 PL 50 限制）⛔ admin only
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

const sessionSlashHelp = `**` + "`/session`" + ` 命令家族**

- ` + "`/session new`" + ` — 直接抛弃当前会话（不留摘要），下一条消息起全新 claude session
- ` + "`/session set <uuid>`" + ` — 把本 room 绑定到一个已存在的 claude session id（例如从终端 ` + "`claude --resume`" + ` 出来的那条对话），下一条消息会 ` + "`--resume`" + ` 接上。会扫 ` + "`~/.claude/projects/*/<uuid>.jsonl`" + ` 校验 session 记录的 cwd 与 room 配置的 cwd 是否一致，不一致直接拒绝（claude 的 session 文件是按 cwd 切目录存的，不一致 ` + "`--resume`" + ` 会找不到）
- ` + "`/session show`" + ` — 显示本 room 当前绑定的 session id + cwd + 终端 ` + "`claude --resume`" + ` 复制命令
- ` + "`/session help`" + ` — 这条帮助`

// claudeSessionIDRE matches the UUID format claude code emits for
// session_id (8-4-4-4-12 lowercase hex). We're strict here because
// /session set turns into a `claude --resume <id>` invocation, and a
// malformed id would silently boot a fresh session instead.
var claudeSessionIDRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// handleSessionSlash dispatches /session sub-commands. Unknown subs
// echo a hint; bare /session shows help.
func (b *Bridge) handleSessionSlash(ctx context.Context, roomID id.RoomID, rest string) bool {
	sub := strings.TrimSpace(rest)
	arg := ""
	if i := strings.IndexAny(sub, " \t"); i > 0 {
		arg = strings.TrimSpace(sub[i+1:])
		sub = sub[:i]
	}
	switch sub {
	case "", "help":
		_, _ = b.mx.SendText(ctx, roomID, sessionSlashHelp)
		return true
	case "new":
		b.endSession(roomID)
		_, _ = b.mx.SendText(ctx, roomID, "🌱 已开启新会话（前序对话不再传递）")
		return true
	case "set":
		b.handleSessionSet(ctx, roomID, arg)
		return true
	case "show":
		b.handleSessionShow(ctx, roomID)
		return true
	}
	_, _ = b.mx.SendText(ctx, roomID, "未知 `/session` 子命令；见 `/session help`")
	return true
}

// handleSessionShow prints the room's currently bound claude session
// id (if any) and the cwd it lives under, plus a copy-pasteable
// terminal `claude --resume` command. Subset of /status focused
// purely on the session — handy for grabbing the id to migrate the
// other direction (room → terminal).
func (b *Bridge) handleSessionShow(ctx context.Context, roomID id.RoomID) {
	sid := ""
	if b.opts.Sessions != nil {
		sid = b.opts.Sessions.Get(string(roomID))
	}
	if sid == "" {
		_, _ = b.mx.SendText(ctx, roomID, "_(本 room 还没有 resumable session — 发一条消息会自动起一个 fresh session)_")
		return
	}
	r := b.resolve(ctx, roomID)
	var sb strings.Builder
	fmt.Fprintf(&sb, "- session id: `%s`\n", sid)
	if r.Cwd != "" {
		fmt.Fprintf(&sb, "- cwd: `%s`\n", r.Cwd)
		fmt.Fprintf(&sb, "- 终端恢复：`cd %s && claude --resume %s`\n", r.Cwd, sid)
		sb.WriteString("  （先发 `/archive` 或 `/session new` 释放 mosaic 这边的 claude 进程，避免两个进程并发写同一会话文件）")
	}
	_, _ = b.mx.SendText(ctx, roomID, sb.String())
}

// handleSessionSet binds an existing claude session id (e.g. one
// running in a terminal `claude --resume`) to this room. The cwd
// recorded inside the session jsonl must match the room's resolved
// cwd, otherwise `claude --resume` would silently start a fresh
// session — refusing up-front is clearer than silent failure.
func (b *Bridge) handleSessionSet(ctx context.Context, roomID id.RoomID, sid string) {
	sid = strings.TrimSpace(sid)
	if sid == "" {
		_, _ = b.mx.SendText(ctx, roomID, "用法：`/session set <session-uuid>`")
		return
	}
	if !claudeSessionIDRE.MatchString(sid) {
		_, _ = b.mx.SendText(ctx, roomID, "❌ 不像合法的 claude session id（应为 UUID 格式 8-4-4-4-12）")
		return
	}
	if b.opts.Sessions == nil {
		_, _ = b.mx.SendText(ctx, roomID, "❌ SessionStore 未初始化，不能持久化 session id")
		return
	}
	jsonlPath, sessionCwd, err := extractClaudeSessionCwd(sid)
	if err != nil {
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf("❌ 找不到本机 claude session：%s\n\n确认 session id 写对了，并且这台机器上 `~/.claude/projects/*/%s.jsonl` 存在。", err.Error(), sid))
		return
	}
	r := b.resolve(ctx, roomID)
	if r.Cwd == "" {
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf("❌ 本 room 还没配 cwd，无法绑定。\n\nclaude session 记录的 cwd 是 `%s`，先 `/project cwd %s` 给本 room 所属 Space 配上 cwd，再 `/session set %s`。", sessionCwd, sessionCwd, sid))
		return
	}
	roomCwd := filepath.Clean(expandHome(r.Cwd))
	sessionCwdNorm := filepath.Clean(sessionCwd)
	if roomCwd != sessionCwdNorm {
		_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf("❌ cwd 不一致，拒绝绑定。\n\n- room cwd: `%s`\n- session cwd（来自 `%s`）: `%s`\n\nclaude `--resume` 按 cwd 切目录存 session 文件，不一致会找不到。先 `/project cwd %s` 改 room/project cwd，再 `/session set %s`。", roomCwd, jsonlPath, sessionCwdNorm, sessionCwdNorm, sid))
		return
	}
	b.endSession(roomID)
	if err := b.opts.Sessions.Set(string(roomID), sid); err != nil {
		_, _ = b.mx.SendText(ctx, roomID, "❌ 写 sessions.json 失败："+err.Error())
		return
	}
	_, _ = b.mx.SendText(ctx, roomID, fmt.Sprintf("🔗 已绑定终端 claude session `%s`（cwd=`%s`）。下一条消息会 `--resume` 接续上下文。", sid, sessionCwdNorm))
}

// extractClaudeSessionCwd locates ~/.claude/projects/*/<sid>.jsonl
// and returns the cwd recorded in the first event that carries one.
// claude-code's project subdir name is a slugified cwd (slashes →
// dashes), but slugify is lossy, so we don't invert it — we glob
// across all project subdirs and trust the authoritative `cwd` field
// inside the jsonl.
func extractClaudeSessionCwd(sid string) (jsonlPath, cwd string, err error) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", "", fmt.Errorf("resolve home dir: %w", err)
	}
	matches, err := filepath.Glob(filepath.Join(home, ".claude", "projects", "*", sid+".jsonl"))
	if err != nil {
		return "", "", err
	}
	if len(matches) == 0 {
		return "", "", fmt.Errorf("no session file matching ~/.claude/projects/*/%s.jsonl", sid)
	}
	jsonlPath = matches[0]
	f, err := os.Open(jsonlPath)
	if err != nil {
		return jsonlPath, "", err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// Assistant turns can produce multi-MB lines; lift the default 64KB cap.
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for i := 0; i < 200 && scanner.Scan(); i++ {
		var ev struct {
			Cwd string `json:"cwd"`
		}
		if json.Unmarshal(scanner.Bytes(), &ev) == nil && ev.Cwd != "" {
			return jsonlPath, ev.Cwd, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return jsonlPath, "", err
	}
	return jsonlPath, "", fmt.Errorf("no cwd field in first 200 events of %s", jsonlPath)
}

const slashHelp = `**可用命令**

- ` + "`/session`" + ` — session 管理（new / set …）— 见 ` + "`/session help`" + `
- ` + "`/compact`" + ` — 让 claude 把当前会话总结成一份 markdown 摘要，归档到本 room 的 ` + "`SUMMARY.md`" + `；之后的新会话会自动注入这份摘要作为系统提示
- ` + "`/archive`" + ` — 把本 room 标记为已归档：bot 不再响应（除 ` + "`/unarchive`" + `），但 memory 文件保留
- ` + "`/unarchive`" + ` — 唤醒已归档的 room
- ` + "`/status`" + ` — 显示当前 room 的 session id / project / cwd
- ` + "`/agent`" + ` — agent 管理（list / new …）— 见 ` + "`/agent help`" + `
- ` + "`/project`" + ` — project 管理（status / cwd / name …）— 见 ` + "`/project help`" + `
- ` + "`/export`" + ` — admin-gated。把所有 agent 能看到的 room 历史解密后落盘到 ` + "`<data>/exports/`" + `，JSONL 格式，断点续传。后台运行，进度在原消息里 edit 更新
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
		if r.Cwd != "" {
			fmt.Fprintf(&sb, "- 终端恢复：`cd %s && claude --resume %s`\n", r.Cwd, resume)
			sb.WriteString("  （先发 `/archive` 或 `/session new` 释放 mosaic 这边的 claude 进程，避免两个进程并发写同一会话文件）\n")
		}
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
		// Finalize whatever was streaming with the canonical body
		// (deltas should agree but TextFinal is authoritative).
		t.finalizeText(ctx, e.Body)
		return false

	case runtime.Thinking:
		return false

	case runtime.ToolUse:
		// Close any in-flight streaming text first so timeline order
		// is preserved.
		t.finalizeText(ctx, "")
		t.toolCount++
		// AskUserQuestion: intercept and render as a native Matrix
		// poll instead of a plain tool_use bubble. The SDK still
		// auto-fails the deferred tool with an error tool_result —
		// silently dropped by the runtime.ToolResult branch above
		// (all tool errors are suppressed). The user's vote arrives
		// via handlePollResponse and is injected as a fresh user turn.
		if e.Name == "AskUserQuestion" {
			if ask, ok := parseAskUserQuestionTool(e.Input); ok {
				t.refreshTypingIfStale(ctx)
				t.renderAskUser(ctx, ask)
				return false
			}
			log.Printf("[agent] AskUserQuestion tool_use unparseable, falling through to generic render")
		}
		if t.b.opts.IgnoreToolsMsg[strings.ToLower(e.Name)] {
			return false
		}
		body := FormatToolUse(e.Name, e.Input)
		t.refreshTypingIfStale(ctx)
		// Bash is renderered as m.emote so it reads as "* <agent>
		// running deploy" — de-emphasized housekeeping vs the agent's
		// own dialogue (m.text). Other tools stay on m.text for now.
		send := t.b.mx.SendText
		if e.Name == "Bash" {
			send = t.b.mx.SendEmote
		}
		if _, err := send(ctx, t.roomID, body); err != nil {
			log.Printf("[agent] send tool_use msg failed: %v", err)
		}
		return false

	case runtime.ToolResult:
		// Tool results are entirely internal to the model loop —
		// success is implicit and errors are noise (the agent's own
		// next-turn text usually explains what happened in plain
		// language). Suppress everything.
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
	// askUserDeferredProtocol is always appended (incl. resume) — short
	// enough not to be costly, and the directive is easy to age out of
	// long-running conversations otherwise.
	if appendSP != "" {
		appendSP += "\n\n"
	}
	appendSP += askUserDeferredProtocol
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
// when the session is removed (e.g. /session new, /compact).
func (b *Bridge) dispatchLoop(s *roomSession) {
	for req := range s.inbox {
		if req.compact {
			b.mu.Lock()
			b.pendingCompact[s.roomID] = true
			b.mu.Unlock()
		}
		b.runTurn(s.roomID, req.sender, req.text, req.attachments)
		// Note: if /session new or /compact ran during the turn, the
		// session was removed from b.sessions but THIS goroutine
		// keeps draining its old inbox (now orphaned). The next
		// handleMessage will resolve a fresh session via getOrCreate.
		// To stop this old loop, /session new / /compact close the
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

// handleExportSlash kicks off a fleet-wide history export in the
// background and edits a single status message in place as work
// progresses. Admin-gated. Multiple concurrent /export invocations
// are not prevented — they share OutDir, and per-room state.json
// resumes safely; the worst case is two goroutines updating the
// same status message.
func (b *Bridge) handleExportSlash(ctx context.Context, roomID id.RoomID, sender id.UserID, args string) {
	_ = args // no flags in v1
	if !b.isAdmin(sender) {
		_, _ = b.mx.SendText(ctx, roomID,
			fmt.Sprintf("⛔ `/export` 需要管理员权限。当前 admins: %v", b.opts.Admins))
		return
	}
	if b.opts.Manager == nil {
		_, _ = b.mx.SendText(ctx, roomID, "⚠️ manager unavailable in this build")
		return
	}
	clients := b.opts.Manager.Clients()
	if len(clients) == 0 {
		_, _ = b.mx.SendText(ctx, roomID, "⚠️ 当前没有在线 agent —— 没法做历史拉取")
		return
	}
	if b.opts.DataDir == "" {
		_, _ = b.mx.SendText(ctx, roomID, "⚠️ DataDir 未配置，无法决定导出路径")
		return
	}
	outDir := filepath.Join(b.opts.DataDir, "exports")

	agentIDs := make([]string, 0, len(clients))
	for id := range clients {
		agentIDs = append(agentIDs, id)
	}
	statusBody := fmt.Sprintf(
		"🗂️ **导出已启动**\n\n- 输出目录: `%s`\n- 参与 agent: %v\n- 状态: 枚举 room 中...",
		outDir, agentIDs)
	statusEvent, err := b.mx.SendText(ctx, roomID, statusBody)
	if err != nil {
		log.Printf("[agent] /export status send: %v", err)
		return
	}

	// Detach from the inbound ctx — that one's per-message and will
	// be cancelled after handleMessage returns. Use background so the
	// export survives the slash invocation.
	go func() {
		bg := context.Background()
		startedAt := time.Now()
		var (
			mu       sync.Mutex
			done     int
			events   int
			failed   int
			lastEdit time.Time
		)
		editProgress := func(force bool) {
			mu.Lock()
			defer mu.Unlock()
			if !force && time.Since(lastEdit) < 2*time.Second {
				return
			}
			lastEdit = time.Now()
			body := fmt.Sprintf(
				"🗂️ **导出进行中**\n\n- 输出目录: `%s`\n- 完成 room: **%d**\n- 已写事件: **%d**\n- 解密失败: **%d**\n- 经过: %s",
				outDir, done, events, failed, time.Since(startedAt).Round(time.Second))
			if err := b.mx.EditText(bg, roomID, statusEvent, body); err != nil {
				log.Printf("[agent] /export edit progress: %v", err)
			}
		}

		exp, err := export.New(export.Options{
			OutDir:  outDir,
			Clients: clients,
			RoomDone: func(rs export.RoomSummary) {
				mu.Lock()
				done++
				events += rs.EventCount
				failed += rs.FailedCount
				mu.Unlock()
				editProgress(false)
			},
		})
		if err != nil {
			_ = b.mx.EditText(bg, roomID, statusEvent,
				fmt.Sprintf("❌ 导出初始化失败: %v", err))
			return
		}
		summary, runErr := exp.Run(bg)
		// Final edit — always force so the last RoomDone tick that
		// got throttled isn't the user's last view.
		var finalBody strings.Builder
		if runErr != nil {
			fmt.Fprintf(&finalBody,
				"⚠️ **导出结束（带错误）**: %v\n\n", runErr)
		} else {
			finalBody.WriteString("✅ **导出完成**\n\n")
		}
		fmt.Fprintf(&finalBody,
			"- 输出目录: `%s`\n- 总 room: **%d**\n- 总事件: **%d**\n- 解密失败: **%d**\n- 总耗时: %s\n- manifest: `%s/manifest.json`",
			outDir, summary.TotalRooms, summary.TotalEvents, summary.TotalFailed,
			summary.Duration.Round(time.Second), outDir)
		if summary.TotalFailed > 0 {
			fmt.Fprintf(&finalBody,
				"\n\n> 解密失败的 event 落到各 room 的 `failed.jsonl`。原因通常是本 agent 在该 event 写入时还没加入房间 / 没拿到 megolm key。可以让其他 agent 加入后再跑一次 /export，会自动 retry。")
		}
		if err := b.mx.EditText(bg, roomID, statusEvent, finalBody.String()); err != nil {
			log.Printf("[agent] /export final edit: %v", err)
		}
	}()
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
