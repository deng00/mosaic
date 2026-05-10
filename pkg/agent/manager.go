// Package agent — manager.go: callbacks the bridge needs to query /
// mutate the daemon-level agent fleet (list, create new). Implemented
// by the main package's AgentRuntime; broken out as an interface so
// the bridge doesn't import main.
package agent

// AgentInfo is what /agent list shows in chat.
type AgentInfo struct {
	ID         string // config id, also the data subdir name
	UserID     string // full @user:server
	DeviceName string
	Online     bool // currently logged in (sync goroutine alive)
}

// CreateRequest is the payload for AgentManager.Create. Mirrors the
// fields a slock-style "create agent" UI surfaces, minus the
// per-machine COMPUTER selector (we're single-host).
type CreateRequest struct {
	Localpart   string // required, also used as data subdir name
	DisplayName string // shown in Element member list
	Description string // role / mission — populates MEMORY.md "Role" section
	Model       string // claude --model (sonnet / opus / "" = default)
	Runtime     string // reserved: which CodingAgent to spawn (default "claude")
}

// ProjectInfo is what /project list / /project status surface.
type ProjectInfo struct {
	SpaceID string
	Name    string
	Cwd     string
	Model   string
}

// AgentManager is the bridge's view onto the runtime: enumerate /
// create agents and read / mutate per-Space project config. nil-safe:
// bridge tolerates a nil Manager (commands reply with "unavailable").
type AgentManager interface {
	List() []AgentInfo
	Create(req CreateRequest) (AgentInfo, error)

	// Projects returns the currently configured projects (Space →
	// cwd / name / model).
	Projects() []ProjectInfo

	// SetProject writes a Project entry for the given Space ID.
	// Empty fields are left unchanged (passing name="" preserves the
	// existing name; same for cwd, model). Persists config to disk
	// and invalidates per-room resolution caches so changes take
	// effect on the next claude spawn.
	SetProject(spaceID, name, cwd, model string) error

	// Members returns the current allow-list (non-admin users
	// authorised to drive agents). Admins are NOT included here —
	// callers should check Admins separately or use Bridge.isAllowed.
	Members() []string

	// AddMember / RemoveMember edit the allow-list and persist to
	// config.yaml. Both are idempotent. Broadcasts the new list to
	// all running bridges so /agent allow takes effect immediately.
	AddMember(userID string) error
	RemoveMember(userID string) error
}
