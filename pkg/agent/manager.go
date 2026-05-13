// Package agent — manager.go: callbacks the bridge needs to query /
// mutate the daemon-level agent fleet (list, create new). Implemented
// by the main package's AgentRuntime; broken out as an interface so
// the bridge doesn't import main.
package agent

import "github.com/deng00/mosaic/pkg/matrix"

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

	// EnsureProject inserts a Project entry for spaceID with the
	// given name iff one doesn't already exist. Returns created=true
	// only on first insert — used by the auto-init flow so that
	// follow-on side effects (e.g. creating a "welcome" room) run
	// exactly once even when several agents observe the new Space
	// concurrently. Empty name is allowed; caller is responsible for
	// deciding a fallback.
	EnsureProject(spaceID, name string) (created bool, err error)

	// Clients returns the live *matrix.Client for every currently
	// online agent, keyed by agent ID. Used by fleet-wide operations
	// (e.g. /export) that must talk to every agent's homeserver
	// session and crypto store. Offline agents are omitted.
	Clients() map[string]*matrix.Client
}
