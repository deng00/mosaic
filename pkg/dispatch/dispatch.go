// Package dispatch routes a task in state "in_progress" to a coding
// agent: pick an assignee, prepare an isolated workspace via configured
// hooks, create a topic-room owned by the agent, write the rendered
// WORKFLOW prompt as TASK.md (auto-injected via the agent's memory
// layers), and kick off a fresh claude turn.
//
// Subscribes to task.Store.OnChange. Hook fires synchronously inside
// the store; we hand work off to a goroutine immediately so the API
// caller's PATCH returns fast.
//
// Concurrency model: each agent runs at most one in_progress task at a
// time (mirrors per-room serial inbox). Per-project / global concurrency
// is bounded only by how many agents are configured.
//
// Modeled on cs-symphony's orchestrator but trimmed to fit Mosaic's
// long-lived agent + Matrix-room architecture (no per-issue subprocess
// pool — the agent process is the long-lived bridge).
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/deng00/mosaic/pkg/agent"
	"github.com/deng00/mosaic/pkg/task"
	"github.com/deng00/mosaic/pkg/workflow"
	"github.com/deng00/mosaic/pkg/workspace"
)

// AgentBridge is the subset of *agent.Bridge the dispatcher needs.
// Defined as an interface for unit-testability.
type AgentBridge interface {
	MatrixUserID() id.UserID
	CreateTaskRoom(ctx context.Context, name, topic string, parentSpace id.RoomID, invite []id.UserID) (id.RoomID, error)
	RegisterRoomOverride(roomID id.RoomID, rc agent.RoomConfig)
	RunTaskTurn(roomID id.RoomID, kickoff string) error
}

// AgentSink hands the dispatcher live snapshots of the agent fleet +
// project metadata. Implemented in main.go on top of AgentRuntime.
type AgentSink interface {
	// BridgeForAgent returns the live bridge for a configured agent
	// id (e.g. "cindy"). nil when offline.
	BridgeForAgent(agentID string) AgentBridge

	// AgentByUserID returns the configured agent id for a full Matrix
	// user id, or "" when not found.
	AgentByUserID(userID string) string

	// ConfiguredAgentIDs returns all configured agent ids in a stable
	// order — used for round-robin auto-assignment when no Assignee
	// is set on the task.
	ConfiguredAgentIDs() []string

	// ProjectByID returns workspace + WORKFLOW config for one Space.
	ProjectByID(spaceID string) (ProjectMeta, bool)
}

// ProjectMeta is the per-Space configuration the dispatcher cares about.
type ProjectMeta struct {
	SpaceID       string
	Name          string
	Prefix        string
	WorkspaceRoot string
	Hooks         workspace.Hooks
	WorkflowPath  string // optional explicit override; "" → look up by convention
	Cwd           string // project's "primary" cwd (informational, not used as workspace)
}

// MemoryWriter writes per-room markdown files (TASK.md). Implemented
// by *agent.Memory. Using a string spaceID + RoomID pair to spare
// callers a constant cast at every site.
type MemoryWriter interface {
	WriteTaskBrief(spaceID id.RoomID, roomID id.RoomID, body string) error
}

// Config wires the dispatcher to the rest of the daemon.
type Config struct {
	// DataDir is <data> (where projects/<id>/WORKFLOW.md lives).
	DataDir string
	// DefaultWorkspaceRoot is used when ProjectMeta.WorkspaceRoot is
	// empty. Typically <data>/workspaces.
	DefaultWorkspaceRoot string
	// APIBase + APIToken are injected as MOSAIC_API_URL / MOSAIC_TOKEN
	// in the per-room env so the agent can curl-PATCH state→in_review
	// when done. Empty when the web server is disabled.
	APIBase  string
	APIToken string
}

// Dispatcher orchestrates in_progress tasks. Subscribe via Start.
type Dispatcher struct {
	cfg    Config
	store  *task.Store
	memory MemoryWriter
	sink   AgentSink

	mu   sync.Mutex
	busy map[string]string // agentID → in_progress taskID currently held
}

// New returns a dispatcher. Call Start to register the store hook.
func New(cfg Config, store *task.Store, memory MemoryWriter, sink AgentSink) *Dispatcher {
	return &Dispatcher{
		cfg:    cfg,
		store:  store,
		memory: memory,
		sink:   sink,
		busy:   map[string]string{},
	}
}

// Start wires the dispatcher into the store. Idempotent at the store
// level (multiple Starts add multiple hook registrations — don't).
func (d *Dispatcher) Start() {
	d.store.OnChange(d.onChange)
}

// onChange is the store hook. Runs synchronously inside the store's
// per-project mutex — keep it cheap; long work goes to a goroutine.
func (d *Dispatcher) onChange(spaceID string, before, after *task.Task) {
	if after == nil {
		// Purge: free any agent slot tied to this task.
		if before != nil {
			d.releaseTask(before.ID)
		}
		return
	}

	// State left in_progress → free the agent slot (in_review, done,
	// cancelled, todo all release).
	if before != nil && before.State == task.StateInProgress && after.State != task.StateInProgress {
		d.releaseTask(after.ID)
	}

	// Transition INTO in_progress (covers create-as-in_progress).
	enteringInProgress := after.State == task.StateInProgress &&
		(before == nil || before.State != task.StateInProgress)
	if enteringInProgress {
		// Run the heavy work outside the store lock.
		go d.startTask(spaceID, *after)
	}
}

// releaseTask frees whichever busy slot maps to taskID.
func (d *Dispatcher) releaseTask(taskID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for agentID, tid := range d.busy {
		if tid == taskID {
			delete(d.busy, agentID)
			log.Printf("[dispatch] freed slot: agent=%s task=%s", agentID, taskID)
			return
		}
	}
}

// reserveAgent claims a free agent for a task. assigneeUserID is the
// task's preferred assignee (full Matrix user id, may be empty).
// Returns the chosen agentID + bridge, or an error explaining what
// went wrong (no available agent, etc.).
func (d *Dispatcher) reserveAgent(taskID, assigneeUserID string) (string, AgentBridge, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	tryClaim := func(agentID string) (AgentBridge, bool) {
		if _, busy := d.busy[agentID]; busy {
			return nil, false
		}
		br := d.sink.BridgeForAgent(agentID)
		if br == nil {
			return nil, false
		}
		d.busy[agentID] = taskID
		return br, true
	}

	// Preferred assignee path.
	if assigneeUserID != "" {
		agentID := d.sink.AgentByUserID(assigneeUserID)
		if agentID == "" {
			return "", nil, fmt.Errorf("assignee %s is not a configured agent", assigneeUserID)
		}
		br, ok := tryClaim(agentID)
		if !ok {
			return "", nil, fmt.Errorf("assignee %s (%s) is unavailable (offline or already running another task)", assigneeUserID, agentID)
		}
		return agentID, br, nil
	}

	// Round-robin: first idle agent in config order.
	for _, agentID := range d.sink.ConfiguredAgentIDs() {
		if br, ok := tryClaim(agentID); ok {
			return agentID, br, nil
		}
	}
	return "", nil, errors.New("no idle agent available — every configured agent is busy or offline")
}

// startTask runs the full pipeline for a task transitioning into
// in_progress. Errors are logged + reflected back into the task as a
// state revert to todo with a description note (so the user notices
// in the board UI).
func (d *Dispatcher) startTask(spaceID string, t task.Task) {
	log.Printf("[dispatch] starting task %s in space %s (assignee=%q)", t.ID, spaceID, t.Assignee)

	proj, ok := d.sink.ProjectByID(spaceID)
	if !ok {
		d.fail(spaceID, t.ID, fmt.Errorf("project not configured for space %s", spaceID))
		return
	}

	agentID, br, err := d.reserveAgent(t.ID, t.Assignee)
	if err != nil {
		log.Printf("[dispatch] %s: cannot reserve agent: %v", t.ID, err)
		d.fail(spaceID, t.ID, err)
		return
	}
	chosenUID := string(br.MatrixUserID())
	log.Printf("[dispatch] %s: assigned to agent=%s (%s)", t.ID, agentID, chosenUID)

	// Workspace prep.
	root := proj.WorkspaceRoot
	if root == "" {
		root = d.cfg.DefaultWorkspaceRoot
	}
	root = expandHome(root)
	hooks := proj.Hooks
	if hooks.Timeout <= 0 {
		hooks.Timeout = 5 * time.Minute
	}
	// Default after_create: clone the project's source dir into the
	// workspace and copy its `origin` remote URL over so the agent's
	// push lands at the right place. Skipped only if the user supplied
	// their own after_create.
	if hooks.AfterCreate == "" {
		hooks.AfterCreate = defaultAfterCreate(expandHome(proj.Cwd))
	}
	wsm := workspace.NewManager(root, hooks)
	wsCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ws, err := wsm.Create(wsCtx, t.ID)
	if err != nil {
		d.releaseTask(t.ID)
		d.fail(spaceID, t.ID, fmt.Errorf("workspace prepare: %w", err))
		return
	}
	log.Printf("[dispatch] %s: workspace=%s (createdNow=%v)", t.ID, ws.Path, ws.CreatedNow)

	// Render WORKFLOW (project's, or default).
	wf := d.loadWorkflow(spaceID, proj)
	vars := workflow.Vars{
		ID:           t.ID,
		Title:        t.Title,
		Description:  t.Description,
		Labels:       t.Labels,
		Assignee:     chosenUID,
		WorkspaceDir: ws.Path,
		SpaceID:      spaceID,
		MosaicAPI:    d.cfg.APIBase,
		MosaicToken:  d.cfg.APIToken,
	}
	branch, err := wf.RenderBranch(vars)
	if err != nil {
		d.releaseTask(t.ID)
		d.fail(spaceID, t.ID, fmt.Errorf("render branch: %w", err))
		return
	}
	vars.Branch = branch
	prompt, err := wf.Render(vars)
	if err != nil {
		d.releaseTask(t.ID)
		d.fail(spaceID, t.ID, fmt.Errorf("render workflow: %w", err))
		return
	}

	// Topic-room — invite the task creator so they see it appear in
	// Element. Agent itself is implicitly the creator/owner.
	roomName := fmt.Sprintf("Task %s: %s", t.ID, truncate(t.Title, 60))
	roomTopic := fmt.Sprintf("Auto-assigned by Mosaic dispatcher · workspace: %s · branch: %s", ws.Path, branch)
	var invite []id.UserID
	if t.CreatedBy != "" && t.CreatedBy != chosenUID {
		invite = append(invite, id.UserID(t.CreatedBy))
	}
	roomID, err := br.CreateTaskRoom(wsCtx, roomName, roomTopic, id.RoomID(spaceID), invite)
	if err != nil {
		d.releaseTask(t.ID)
		d.fail(spaceID, t.ID, fmt.Errorf("create topic room: %w", err))
		return
	}
	log.Printf("[dispatch] %s: topic room=%s", t.ID, roomID)

	// Persist room + workspace + branch into the task. Skip the hook
	// re-entrancy by using direct PATCH — the dispatcher's own onChange
	// handler will see this as an update with state still in_progress
	// and won't trigger a new dispatch.
	roomStr := string(roomID)
	if _, err := d.store.Update(spaceID, t.ID, task.UpdateInput{
		TopicRoom:     &roomStr,
		WorkspacePath: &ws.Path,
		Branch:        &branch,
	}); err != nil {
		log.Printf("[dispatch] %s: failed to persist room/workspace fields: %v (continuing)", t.ID, err)
	}

	// Write TASK.md so memory layer auto-injects on spawn.
	if err := d.memory.WriteTaskBrief(id.RoomID(spaceID), roomID, prompt); err != nil {
		log.Printf("[dispatch] %s: write TASK.md failed: %v (continuing without)", t.ID, err)
	}

	// Per-room override: workspace cwd + task callback env.
	br.RegisterRoomOverride(roomID, agent.RoomConfig{
		Cwd: ws.Path,
		Env: d.taskEnv(spaceID, t.ID),
	})

	// before_run hook (best-effort: failure logged but task continues
	// — agent can also do prep work itself).
	if err := wsm.BeforeRun(wsCtx, ws); err != nil {
		log.Printf("[dispatch] %s: before_run failed: %v (proceeding anyway)", t.ID, err)
	}

	// Kick off the agent. The kickoff message is short and visible in
	// the room timeline; the full WORKFLOW lives in TASK.md (system
	// prompt layer).
	kickoff := fmt.Sprintf("Working on **%s** — %s.\n\nFull task brief is in your system prompt as `TASK.md`. Workspace: `%s`. Branch: `%s`.\n\nProceed.",
		t.ID, t.Title, ws.Path, branch)
	if err := br.RunTaskTurn(roomID, kickoff); err != nil {
		log.Printf("[dispatch] %s: RunTaskTurn failed: %v", t.ID, err)
		d.releaseTask(t.ID)
		d.fail(spaceID, t.ID, fmt.Errorf("kick off agent: %w", err))
		return
	}
	log.Printf("[dispatch] %s: kicked off in room %s on agent %s", t.ID, roomID, agentID)
}

// loadWorkflow tries the project's WORKFLOW.md, falling back to the
// embedded default template.
func (d *Dispatcher) loadWorkflow(spaceID string, proj ProjectMeta) *workflow.File {
	candidates := []string{}
	if proj.WorkflowPath != "" {
		candidates = append(candidates, expandHome(proj.WorkflowPath))
	}
	candidates = append(candidates,
		filepath.Join(d.cfg.DataDir, "projects", safeID(spaceID), "WORKFLOW.md"))
	for _, p := range candidates {
		f, err := workflow.Load(p)
		if err != nil {
			log.Printf("[dispatch] WORKFLOW load %s: %v (falling back)", p, err)
			continue
		}
		if f != nil {
			return f
		}
	}
	// Default — parse the embedded template once per call (cheap; saves
	// us threading state). Errors here would mean a busted const, which
	// is a programming error.
	f, err := workflow.Parse("<default>", []byte(workflow.DefaultTemplate))
	if err != nil {
		log.Printf("[dispatch] default workflow parse failed: %v", err)
		return &workflow.File{Body: workflow.DefaultTemplate}
	}
	return f
}

// taskEnv assembles the env vars the agent needs to call back into
// the Mosaic API on completion. Returns nil when the web server is off.
func (d *Dispatcher) taskEnv(spaceID, taskID string) map[string]string {
	if d.cfg.APIBase == "" || d.cfg.APIToken == "" {
		return nil
	}
	return map[string]string{
		"MOSAIC_API_URL":  d.cfg.APIBase,
		"MOSAIC_TOKEN":    d.cfg.APIToken,
		"MOSAIC_TASK_ID":  taskID,
		"MOSAIC_SPACE_ID": spaceID,
	}
}

// fail rolls a failed task back to "todo" so it shows on the board
// instead of getting silently stuck in in_progress. Best-effort: if
// the rollback itself fails we just log loudly.
func (d *Dispatcher) fail(spaceID, taskID string, cause error) {
	log.Printf("[dispatch] task %s FAILED to start: %v — reverting to todo", taskID, cause)
	st := task.StateTodo
	if _, err := d.store.Update(spaceID, taskID, task.UpdateInput{State: &st}); err != nil {
		log.Printf("[dispatch] also failed to revert state: %v", err)
	}
}

// defaultAfterCreate is the workspace bootstrap script used when the
// project has no explicit workspace_hooks.after_create. Behavior:
//
//   - clones <projectCwd> into the workspace via local file:// (fast,
//     and works even when the launchd-launched mosaic process has no
//     SSH agent socket — the source dir is already on disk)
//   - copies the source dir's `origin` remote URL onto the workspace
//     so the agent's eventual `git push` lands at the right place
//   - leaves git user.name / user.email untouched — agent inherits
//     whatever ~/.gitconfig has configured globally
//
// Errors loudly when projectCwd is not a git repo so the dispatcher
// reverts the task to todo with a clear cause instead of silently
// dropping the agent into an empty directory.
func defaultAfterCreate(projectCwd string) string {
	q := shellQuote(projectCwd)
	return `set -e
if [ ! -d ` + q + `/.git ]; then
  echo "mosaic: source dir is not a git repo: " ` + q + ` >&2
  exit 1
fi
git clone ` + q + ` .
remote_url=$(git -C ` + q + ` config --get remote.origin.url || true)
if [ -n "$remote_url" ]; then
  git remote set-url origin "$remote_url"
fi
`
}

// shellQuote returns a single-quoted bash literal of s. Replaces every
// `'` with `'\''` so the result is safe to splice into a `set -e`
// script without shell-injection.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// expandHome resolves a leading ~ to $HOME.
func expandHome(p string) string {
	if p == "" || (p != "~" && !strings.HasPrefix(p, "~/")) {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	if p == "~" {
		return home
	}
	return filepath.Join(home, p[2:])
}

// safeID escapes ":" in Matrix IDs for filesystem use. Matches
// pkg/agent/memory.go and pkg/task/store.go.
func safeID(s string) string {
	return strings.ReplaceAll(s, ":", "_")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
