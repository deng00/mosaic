package main

import (
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/deng00/mosaic/pkg/agent"
	"github.com/deng00/mosaic/pkg/dispatch"
	"github.com/deng00/mosaic/pkg/web"
	"github.com/deng00/mosaic/pkg/workspace"
)

// ----- web -----

// webProvider adapts AgentRuntime → web.Provider so the task-board
// HTTP layer can read live project + agent snapshots without depending
// on main / config packages.
type webProvider struct{ rt *AgentRuntime }

func (p *webProvider) Projects() []web.Project {
	in := p.rt.Projects()
	out := make([]web.Project, 0, len(in))
	for _, pi := range in {
		out = append(out, web.Project{
			SpaceID: pi.SpaceID,
			Name:    pi.Name,
			Prefix:  pi.TaskPrefix,
		})
	}
	return out
}

func (p *webProvider) Agents() []web.Agent {
	in := p.rt.List()
	out := make([]web.Agent, 0, len(in))
	for _, a := range in {
		out = append(out, web.Agent{
			ID:          a.ID,
			UserID:      a.UserID,
			DisplayName: displayNameFor(p.rt, a.ID),
			Online:      a.Online,
		})
	}
	return out
}

func displayNameFor(rt *AgentRuntime, agentID string) string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, bc := range rt.cfg.AllAgents() {
		if bc.ID == agentID {
			if bc.DisplayName != "" {
				return bc.DisplayName
			}
			return bc.User
		}
	}
	return agentID
}

var _ web.Provider = (*webProvider)(nil)

func newWebProvider(rt *AgentRuntime) web.Provider { return &webProvider{rt: rt} }

// ----- dispatch -----

// dispatchSink adapts AgentRuntime + FileConfig → dispatch.AgentSink.
// All snapshots are read live so config edits + bridge online/offline
// changes are picked up without restart.
type dispatchSink struct {
	rt *AgentRuntime
	fc *FileConfig
}

func newDispatchSink(rt *AgentRuntime, fc *FileConfig) dispatch.AgentSink {
	return &dispatchSink{rt: rt, fc: fc}
}

func (s *dispatchSink) BridgeForAgent(agentID string) dispatch.AgentBridge {
	br := s.rt.BridgeForAgent(agentID)
	if br == nil {
		return nil
	}
	// *agent.Bridge already satisfies the interface — return as-is.
	return br
}

func (s *dispatchSink) AgentByUserID(userID string) string {
	server := serverNameFromHomeserver(s.fc.Homeserver)
	for _, bc := range s.fc.AllAgents() {
		if "@"+bc.User+":"+server == userID {
			return bc.ID
		}
	}
	return ""
}

func (s *dispatchSink) ConfiguredAgentIDs() []string {
	out := make([]string, 0, len(s.fc.AllAgents()))
	for _, bc := range s.fc.AllAgents() {
		out = append(out, bc.ID)
	}
	return out
}

func (s *dispatchSink) ProjectByID(spaceID string) (dispatch.ProjectMeta, bool) {
	pc, ok := s.fc.Projects[spaceID]
	if !ok {
		return dispatch.ProjectMeta{}, false
	}
	return dispatch.ProjectMeta{
		SpaceID:       spaceID,
		Name:          pc.Name,
		Prefix:        pc.TaskPrefix,
		WorkspaceRoot: pc.WorkspaceRoot,
		Cwd:           pc.Cwd,
		Hooks: workspace.Hooks{
			AfterCreate:  pc.WorkspaceHooks.AfterCreate,
			BeforeRun:    pc.WorkspaceHooks.BeforeRun,
			AfterRun:     pc.WorkspaceHooks.AfterRun,
			BeforeRemove: pc.WorkspaceHooks.BeforeRemove,
			Timeout:      time.Duration(pc.WorkspaceHooks.TimeoutMS) * time.Millisecond,
		},
	}, true
}

var _ dispatch.AgentSink = (*dispatchSink)(nil)

// dispatchMemory wraps an *agent.Memory so it satisfies
// dispatch.MemoryWriter. The interface uses id.RoomID for both keys
// to keep call sites tight.
type dispatchMemory struct{ m *agent.Memory }

func newDispatchMemory(fc *FileConfig) dispatch.MemoryWriter {
	return &dispatchMemory{m: agent.NewMemory("", projectsDataDir(fc.DataDir))}
}

func (d *dispatchMemory) WriteTaskBrief(spaceID, roomID id.RoomID, body string) error {
	return d.m.WriteTaskBrief(spaceID, roomID, body)
}

var _ dispatch.MemoryWriter = (*dispatchMemory)(nil)
