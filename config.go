package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// FileConfig is what config.yaml looks like. Single-bot setups can
// omit this entirely and use env vars (see envBotConfig) — that path
// is preserved for backwards compat with the original .env layout.
//
// Example:
//
//	homeserver: http://127.0.0.1:8008
//	data_dir: ./data
//	bots:
//	  - id: claude-bot2
//	    user: claude-bot2
//	    password: bot1234
//	    device_name: mosaic
//	    claude:
//	      binary: claude
//	      cwd: /Users/danny0
//	      permission_mode: bypassPermissions
//	  - id: code-reviewer
//	    user: code-reviewer
//	    password: bot1234
//	    claude:
//	      cwd: /Users/danny0/Code
//	      append_system_prompt: "You are a code review assistant."
//	rooms:
//	  "!projABC:localhost":
//	    cwd: /Users/danny0/Code/projA
//	    model: claude-sonnet-4-7
type FileConfig struct {
	Homeserver string `yaml:"homeserver"`
	// ServerName is the Matrix server_name baked into every room/user
	// ID (e.g. "localhost", "matrix.danny.dev"). Distinct from the
	// homeserver URL host: with `http://127.0.0.1:8008` the URL host
	// is 127.0.0.1 but Synapse may carry `server_name: localhost` in
	// homeserver.yaml. Used for display folding (hide :server when
	// it equals our own). If empty, falls back to URL host.
	ServerName string `yaml:"server_name,omitempty"`
	DataDir    string `yaml:"data_dir"`

	// Agents are the Matrix-side identities the daemon brings online.
	// Each one logs in as its own user, has its own crypto state, and
	// can be invited into rooms. We call them "agents" in user-facing
	// language (config / slash commands), distinct from the underlying
	// CodingAgent (Claude Code) that each one spawns per turn.
	Agents []BotConfig `yaml:"agents"`

	// Admins are the full Matrix user IDs allowed to run management
	// slash commands like /agent new. Always implicitly allowed to
	// drive agents.
	Admins []string `yaml:"admins,omitempty"`

	// Members are non-admin Matrix user IDs allowed to chat with /
	// drive agents. Empty = admin-only (paranoid default — random
	// users from the same homeserver who somehow end up in a room
	// with the agent get a polite refusal). Use /agent allow @user
	// from chat to extend.
	Members []string `yaml:"members,omitempty"`

	// SharedSecret is Synapse's registration_shared_secret (copied
	// verbatim from homeserver.yaml). Used by /agent new to register
	// new Matrix users without an admin token. If empty, /agent new
	// is disabled.
	SharedSecret string `yaml:"registration_shared_secret,omitempty"`

	// Projects are keyed by Matrix Space ID. Rooms inherit a project's
	// cwd / model when their m.space.parent state event points at one
	// of these spaces. Optional.
	Projects map[string]ProjectConfigYAML `yaml:"projects,omitempty"`
	// Rooms are keyed by Matrix Room ID. Overrides the inherited
	// project values for that single room. Use sparingly — a one-off
	// experiment room or sandbox.
	Rooms map[string]RoomConfigYAML `yaml:"rooms,omitempty"`

	// Web exposes the task board over HTTP. Opt-in: when disabled the
	// board / REST API don't bind at all. The bearer token persists at
	// <data_dir>/web.token across restarts.
	Web WebConfigYAML `yaml:"web,omitempty"`
}

// AllAgents returns the configured agents.
func (c *FileConfig) AllAgents() []BotConfig {
	return c.Agents
}

type BotConfig struct {
	ID       string `yaml:"id"`       // unique per-deployment, used as data subdir name
	User     string `yaml:"user"`     // localpart (immutable; the Matrix account user_id)
	Password string `yaml:"password"`
	// DeviceName tells *which machine* this agent's claude subprocess
	// runs on. Visible in the user's "active sessions" page in Element
	// (think "Cindy on danny's MacBook" vs "Cindy on the office Mac
	// mini"). Defaults to os.Hostname() when empty — useful when the
	// same agent identity has multiple `mosaic` instances live across
	// machines.
	DeviceName string `yaml:"device_name,omitempty"`
	// DisplayName is the agent's user-visible profile name (what other
	// room members see in the member list and as message sender).
	// Pushed to Matrix profile on every startup. Empty keeps whatever
	// Matrix has stored.
	DisplayName string       `yaml:"display_name,omitempty"`
	Claude      ClaudeConfig `yaml:"claude"`

	// AutoJoinSpaceChildren: when true and the bot is a member of any
	// Space, every newly added m.space.child triggers a JoinRoomByID
	// for that child. Works for `restricted`-rule rooms (the bot is
	// auto-authorised by Space membership). Private rooms still need
	// an explicit invite. Default: false — opt in per bot.
	AutoJoinSpaceChildren bool `yaml:"auto_join_space_children,omitempty"`
}

type ClaudeConfig struct {
	Binary             string `yaml:"binary"`              // default "claude"
	Cwd                string `yaml:"cwd"`                 // fallback cwd when no project / room override matches
	Model              string `yaml:"model"`
	PermissionMode     string `yaml:"permission_mode"`     // default "bypassPermissions"
	AppendSystemPrompt string `yaml:"append_system_prompt,omitempty"`
	Env                map[string]string `yaml:"env,omitempty"`
}

type ProjectConfigYAML struct {
	Name  string `yaml:"name,omitempty"`
	Cwd   string `yaml:"cwd,omitempty"`
	Model string `yaml:"model,omitempty"`
	// TaskPrefix is the per-project ticket-id prefix, e.g. "MOS" makes
	// task ids look like MOS-1, MOS-2. Required when the web task
	// board is enabled and tasks are created against this project.
	TaskPrefix string `yaml:"task_prefix,omitempty"`
}

// WebConfigYAML controls the task-board HTTP server. Enabled is opt-in;
// the rest have sane defaults (bind 127.0.0.1, port 24527).
type WebConfigYAML struct {
	Enabled bool   `yaml:"enabled"`
	Bind    string `yaml:"bind,omitempty"`
	Port    int    `yaml:"port,omitempty"`
}

type RoomConfigYAML struct {
	Cwd   string `yaml:"cwd,omitempty"`
	Model string `yaml:"model,omitempty"`
}

// LoadFile parses config.yaml (returns nil if path doesn't exist; the
// caller falls back to env vars).
func LoadFile(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var c FileConfig
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if c.Homeserver == "" {
		return nil, fmt.Errorf("%s: homeserver required", path)
	}
	if len(c.Agents) == 0 {
		return nil, fmt.Errorf("%s: at least one agent required", path)
	}
	if c.DataDir == "" {
		c.DataDir = "data"
	}
	// Resolve DataDir relative to the config file's directory so the
	// daemon works regardless of where the user runs `mosaic` from.
	// Absolute paths pass through untouched.
	if !filepath.IsAbs(c.DataDir) {
		c.DataDir = filepath.Join(filepath.Dir(path), c.DataDir)
	}
	if c.ServerName == "" {
		c.ServerName = serverNameFromHomeserver(c.Homeserver)
	}
	return &c, nil
}

// pickleKeyFor returns a 32-byte key for one bot, persisted next to
// its crypto db so restarts don't rotate it.
func pickleKeyFor(dataDir string) ([]byte, error) {
	keyPath := filepath.Join(dataDir, "pickle.key")
	if data, err := os.ReadFile(keyPath); err == nil && len(data) >= 32 {
		return data, nil
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, buf, 0o600); err != nil {
		return nil, err
	}
	return buf, nil
}
