package dispatch

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/deng00/mosaic/pkg/agent"
	"github.com/deng00/mosaic/pkg/task"
	"github.com/deng00/mosaic/pkg/workspace"
)

func mustGit(t *testing.T, cwd string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = cwd
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s in %q: %v\n%s", strings.Join(args, " "), cwd, err, out)
	}
	return strings.TrimSpace(string(out))
}

func osWriteFile(path, body string) error {
	return os.WriteFile(path, []byte(body), 0o644)
}

func osStat(path string) (os.FileInfo, error) { return os.Stat(path) }

// fakeBridge records calls instead of touching Matrix.
type fakeBridge struct {
	uid           id.UserID
	createdRoomID id.RoomID

	mu       sync.Mutex
	override agent.RoomConfig
	kickoff  string
	roomName string
	parent   id.RoomID
	invited  []id.UserID
}

func (f *fakeBridge) MatrixUserID() id.UserID { return f.uid }
func (f *fakeBridge) CreateTaskRoom(ctx context.Context, name, topic string, parentSpace id.RoomID, invite []id.UserID) (id.RoomID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.roomName = name
	f.parent = parentSpace
	f.invited = invite
	return f.createdRoomID, nil
}
func (f *fakeBridge) RegisterRoomOverride(roomID id.RoomID, rc agent.RoomConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.override = rc
}
func (f *fakeBridge) RunTaskTurn(roomID id.RoomID, kickoff string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kickoff = kickoff
	return nil
}

type fakeSink struct {
	bridges  map[string]AgentBridge
	uidToID  map[string]string
	order    []string
	projects map[string]ProjectMeta
}

func (s *fakeSink) BridgeForAgent(agentID string) AgentBridge { return s.bridges[agentID] }
func (s *fakeSink) AgentByUserID(userID string) string        { return s.uidToID[userID] }
func (s *fakeSink) ConfiguredAgentIDs() []string              { return s.order }
func (s *fakeSink) ProjectByID(spaceID string) (ProjectMeta, bool) {
	p, ok := s.projects[spaceID]
	return p, ok
}

type fakeMemory struct {
	mu    sync.Mutex
	wrote map[string]string // "spaceID|roomID" → body
}

func (m *fakeMemory) WriteTaskBrief(spaceID, roomID id.RoomID, body string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wrote == nil {
		m.wrote = map[string]string{}
	}
	m.wrote[string(spaceID)+"|"+string(roomID)] = body
	return nil
}

func newFixture(t *testing.T) (*Dispatcher, *task.Store, *fakeBridge, *fakeSink, *fakeMemory) {
	t.Helper()
	dir := t.TempDir()
	store := task.NewStore(filepath.Join(dir, "projects"))
	br := &fakeBridge{uid: "@cindy:localhost", createdRoomID: "!task-room:localhost"}
	sink := &fakeSink{
		bridges: map[string]AgentBridge{"cindy": br},
		uidToID: map[string]string{"@cindy:localhost": "cindy"},
		order:   []string{"cindy"},
		projects: map[string]ProjectMeta{
			"!space:localhost": {
				SpaceID: "!space:localhost", Name: "Demo", Prefix: "MOS",
				WorkspaceRoot: filepath.Join(dir, "ws"),
				// ":" is a bash no-op — overrides the default git-clone
				// hook that would otherwise need a real source repo.
				Hooks: workspace.Hooks{AfterCreate: ":", Timeout: 5 * time.Second},
			},
		},
	}
	mem := &fakeMemory{}
	d := New(Config{
		DataDir:              dir,
		DefaultWorkspaceRoot: filepath.Join(dir, "ws"),
		APIBase:              "http://127.0.0.1:24527",
		APIToken:             "tok",
	}, store, mem, sink)
	d.Start()
	return d, store, br, sink, mem
}

// waitFor polls fn until it returns true or times out.
func waitFor(t *testing.T, label string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", label)
}

func TestDispatchOnTransitionToInProgress(t *testing.T) {
	_, store, br, _, mem := newFixture(t)

	tk, err := store.Create("!space:localhost", "MOS", task.CreateInput{
		Title:       "Make it work",
		Description: "Implement the feature.",
		Assignee:    "@cindy:localhost",
		CreatedBy:   "@danny:localhost",
	})
	if err != nil {
		t.Fatal(err)
	}

	st := task.StateInProgress
	if _, err := store.Update("!space:localhost", tk.ID, task.UpdateInput{State: &st}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "kickoff sent", func() bool {
		br.mu.Lock()
		defer br.mu.Unlock()
		return br.kickoff != ""
	})

	br.mu.Lock()
	defer br.mu.Unlock()
	if br.parent != "!space:localhost" {
		t.Errorf("parent space: %s", br.parent)
	}
	if br.override.Cwd == "" {
		t.Errorf("expected per-room cwd override")
	}
	if br.override.Env["MOSAIC_TASK_ID"] != "MOS-1" {
		t.Errorf("env MOSAIC_TASK_ID: %s", br.override.Env["MOSAIC_TASK_ID"])
	}
	if br.override.Env["MOSAIC_TOKEN"] != "tok" {
		t.Errorf("env MOSAIC_TOKEN: %s", br.override.Env["MOSAIC_TOKEN"])
	}
	if len(mem.wrote) != 1 {
		t.Errorf("expected TASK.md written, got %d", len(mem.wrote))
	}

	// Task should now have TopicRoom + WorkspacePath set.
	got, _ := store.Get("!space:localhost", tk.ID)
	if got.TopicRoom != "!task-room:localhost" {
		t.Errorf("topic_room: %s", got.TopicRoom)
	}
	if got.WorkspacePath == "" {
		t.Errorf("workspace_path empty")
	}
	if got.Branch != "task/MOS-1" {
		t.Errorf("branch: %s", got.Branch)
	}
}

func TestNoDispatchWithoutAssigneeAndNoIdleAgent(t *testing.T) {
	d, store, br, _, _ := newFixture(t)
	// Mark cindy busy by hand so reservation fails.
	d.mu.Lock()
	d.busy["cindy"] = "OTHER"
	d.mu.Unlock()

	tk, _ := store.Create("!space:localhost", "MOS", task.CreateInput{Title: "x"})
	st := task.StateInProgress
	_, _ = store.Update("!space:localhost", tk.ID, task.UpdateInput{State: &st})

	// Wait briefly; then verify the task was reverted to todo.
	waitFor(t, "rolled back to todo", func() bool {
		got, _ := store.Get("!space:localhost", tk.ID)
		return got.State == task.StateTodo
	})
	br.mu.Lock()
	defer br.mu.Unlock()
	if br.kickoff != "" {
		t.Errorf("should not have kicked off: %q", br.kickoff)
	}
}

// TestDefaultAfterCreateClonesProjectCwd exercises the built-in
// after_create hook (no per-project workspace_hooks). Sets up a real
// git repo as the source dir, runs dispatch, and asserts the workspace
// is a git checkout pointing at the source dir's origin.
func TestDefaultAfterCreateClonesProjectCwd(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source")
	mustGit(t, "", "init", "-b", "main", source)
	// Plant a file so clone has something to copy.
	if err := osWriteFile(filepath.Join(source, "README.md"), "hello\n"); err != nil {
		t.Fatal(err)
	}
	mustGit(t, source, "-c", "user.email=t@t", "-c", "user.name=t", "add", ".")
	mustGit(t, source, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "x")
	mustGit(t, source, "remote", "add", "origin", "git@github.com:fake/repo.git")

	store := task.NewStore(filepath.Join(dir, "projects"))
	br := &fakeBridge{uid: "@cindy:localhost", createdRoomID: "!task-room:localhost"}
	sink := &fakeSink{
		bridges: map[string]AgentBridge{"cindy": br},
		uidToID: map[string]string{"@cindy:localhost": "cindy"},
		order:   []string{"cindy"},
		projects: map[string]ProjectMeta{
			"!space:localhost": {
				SpaceID: "!space:localhost", Name: "Demo", Prefix: "MOS",
				WorkspaceRoot: filepath.Join(dir, "ws"),
				Cwd:           source,
				// No AfterCreate → dispatcher injects defaultAfterCreate.
				Hooks: workspace.Hooks{Timeout: 30 * time.Second},
			},
		},
	}
	d := New(Config{
		DataDir:              dir,
		DefaultWorkspaceRoot: filepath.Join(dir, "ws"),
	}, store, &fakeMemory{}, sink)
	d.Start()

	tk, _ := store.Create("!space:localhost", "MOS",
		task.CreateInput{Title: "x", Assignee: "@cindy:localhost"})
	st := task.StateInProgress
	_, _ = store.Update("!space:localhost", tk.ID, task.UpdateInput{State: &st})

	waitFor(t, "kickoff", func() bool {
		br.mu.Lock()
		defer br.mu.Unlock()
		return br.kickoff != ""
	})

	wsDir := filepath.Join(dir, "ws", "MOS-1")
	if _, err := osStat(filepath.Join(wsDir, "README.md")); err != nil {
		t.Fatalf("workspace not populated by clone: %v", err)
	}
	gotRemote := mustGit(t, wsDir, "config", "--get", "remote.origin.url")
	if gotRemote != "git@github.com:fake/repo.git" {
		t.Errorf("remote not rewritten to source's origin: %q", gotRemote)
	}
}

func TestReleasesSlotOnLeaveInProgress(t *testing.T) {
	d, store, br, _, _ := newFixture(t)

	tk, _ := store.Create("!space:localhost", "MOS", task.CreateInput{
		Title:    "x",
		Assignee: "@cindy:localhost",
	})
	st := task.StateInProgress
	_, _ = store.Update("!space:localhost", tk.ID, task.UpdateInput{State: &st})

	waitFor(t, "kickoff", func() bool {
		br.mu.Lock()
		defer br.mu.Unlock()
		return br.kickoff != ""
	})

	d.mu.Lock()
	if d.busy["cindy"] != tk.ID {
		t.Fatalf("expected cindy busy with %s, got %v", tk.ID, d.busy)
	}
	d.mu.Unlock()

	st2 := task.StateInReview
	_, _ = store.Update("!space:localhost", tk.ID, task.UpdateInput{State: &st2})

	waitFor(t, "slot freed", func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, busy := d.busy["cindy"]
		return !busy
	})
}
