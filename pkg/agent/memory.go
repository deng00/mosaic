// Package agent — memory.go: layered, file-based memory for each
// claude session. The system prompt fed to a fresh / resumed claude
// is the concatenation (in this order) of:
//
//	<agentDir>/MEMORY.md                                ← agent identity / role / persona (per agent)
//	<projectsDir>/<spaceID>/PROJECT.md                  ← project facts (architecture, deps)  — SHARED across agents
//	<projectsDir>/<spaceID>/DECISIONS.md                ← decisions log                         — SHARED
//	<projectsDir>/<spaceID>/rooms/<roomID>/SUMMARY.md   ← /compact output for this conversation — SHARED across agents in the same room
//
// MEMORY.md lives next to each agent (one per Cindy / Alice / …);
// the rest live under a global `data/projects/` tree so multiple
// agents collaborating in one Space share the same project memory
// and the same room SUMMARY (one /compact updates everyone's view).
//
// MEMORY.md is the slock-style identity file:
//
//	# Cindy
//	## Role
//	You are Cindy, the onboarding lead.
//
// Files are markdown; missing files are skipped silently. SUMMARY.md
// is the only file the agent writes itself — produced when /compact
// runs. The other files are user-curated; the agent never overwrites
// them.
package agent

import (
	"os"
	"path/filepath"
	"strings"

	"maunium.net/go/mautrix/id"
)

type Memory struct {
	agentDir    string // <data>/agents/<botID>/  — agent-private
	projectsDir string // <data>/projects/        — shared across all agents
}

// NewMemory wires the agent-private and shared-projects roots.
// projectsDir may be "" to disable shared project memory entirely
// (then only the agent's own MEMORY.md contributes).
func NewMemory(agentDir, projectsDir string) *Memory {
	return &Memory{agentDir: agentDir, projectsDir: projectsDir}
}

// SystemPrompt returns the layered context string, ready for
// claude --append-system-prompt. Empty string when no files exist.
func (m *Memory) SystemPrompt(spaceID, roomID id.RoomID) string {
	if m == nil {
		return ""
	}
	var sb strings.Builder

	add := func(path, label string) {
		b, err := os.ReadFile(path)
		if err != nil || len(b) == 0 {
			return
		}
		sb.WriteString("\n\n# ")
		sb.WriteString(label)
		sb.WriteString("\n\n")
		sb.Write(b)
	}

	add(filepath.Join(m.agentDir, "MEMORY.md"), "Agent identity & memory")
	if spaceID != "" && m.projectsDir != "" {
		spaceDir := filepath.Join(m.projectsDir, safeID(string(spaceID)))
		add(filepath.Join(spaceDir, "PROJECT.md"), "Project memory")
		add(filepath.Join(spaceDir, "DECISIONS.md"), "Project decisions log")
		if roomID != "" {
			roomDir := filepath.Join(spaceDir, "rooms", safeID(string(roomID)))
			add(filepath.Join(roomDir, "SUMMARY.md"), "Earlier conversation summary (this room)")
		}
	}

	out := strings.TrimSpace(sb.String())
	if out == "" {
		return ""
	}
	return "The following memory files were prepared by previous sessions or by the operator. Treat them as authoritative context for this conversation.\n" + out
}

// WriteSummary saves the latest /compact output as the room's
// SUMMARY.md, in the SHARED projects tree (so other agents in the
// same room see it on their next session). Atomic via tmp+rename.
func (m *Memory) WriteSummary(spaceID, roomID id.RoomID, body string) error {
	if m == nil || m.projectsDir == "" {
		return nil
	}
	if spaceID == "" || roomID == "" {
		// Without a project we have nowhere stable to write; skip
		// rather than scattering files for ad-hoc rooms.
		return nil
	}
	dir := filepath.Join(m.projectsDir, safeID(string(spaceID)), "rooms", safeID(string(roomID)))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "SUMMARY.md")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// safeID escapes a Matrix ID for use as a filesystem path component.
// Matrix IDs use "!" prefix and ":" separator; ":" is fine on POSIX
// but breaks on Windows, so we encode it here for portability.
func safeID(s string) string {
	return strings.ReplaceAll(s, ":", "_")
}
