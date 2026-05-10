package main

import (
	"github.com/deng00/mosaic/pkg/web"
)

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

// displayNameFor reaches into the FileConfig for the configured
// DisplayName (the AgentInfo struct exposed via the manager interface
// only carries DeviceName, which is "machine name" not "user name").
func displayNameFor(rt *AgentRuntime, id string) string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, bc := range rt.cfg.AllAgents() {
		if bc.ID == id {
			if bc.DisplayName != "" {
				return bc.DisplayName
			}
			return bc.User
		}
	}
	return id
}

// compile-time interface check
var _ web.Provider = (*webProvider)(nil)

func newWebProvider(rt *AgentRuntime) web.Provider {
	return &webProvider{rt: rt}
}
