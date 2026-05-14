package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/deng00/mosaic/pkg/agent"
	"github.com/deng00/mosaic/pkg/matrix"
	"github.com/deng00/mosaic/pkg/registrar"
)

// AgentRuntime tracks all currently configured agents, supports hot
// adding new ones via /agent new, and writes config.yaml back to
// disk so changes survive a restart.
//
// Implements agent.AgentManager — bridges call into it from slash
// command handlers.
type AgentRuntime struct {
	cfg     *FileConfig
	cfgPath string
	rootCtx context.Context

	mu      sync.Mutex
	running map[string]*runningAgent
	bridges []*agent.Bridge // pumped on /project mutations
	wg      *sync.WaitGroup
}

type runningAgent struct {
	bc     BotConfig
	cancel context.CancelFunc
	mx     *matrix.Client // populated by attachClient after login succeeds
}

func NewAgentRuntime(ctx context.Context, fc *FileConfig, path string, wg *sync.WaitGroup) *AgentRuntime {
	return &AgentRuntime{
		cfg:     fc,
		cfgPath: path,
		rootCtx: ctx,
		running: map[string]*runningAgent{},
		wg:      wg,
	}
}

// trackStart and trackStop are called by runBot at start/end of an
// agent's lifecycle to maintain the online set.
func (r *AgentRuntime) trackStart(bc BotConfig, cancel context.CancelFunc) {
	r.mu.Lock()
	r.running[bc.ID] = &runningAgent{bc: bc, cancel: cancel}
	r.mu.Unlock()
}

func (r *AgentRuntime) trackStop(id string) {
	r.mu.Lock()
	delete(r.running, id)
	r.mu.Unlock()
}

// attachClient associates the just-logged-in matrix.Client with the
// running entry for bc.ID. Called from runBot after matrix.Login
// succeeds so /export and other fleet-wide ops can reach every agent's
// crypto store.
func (r *AgentRuntime) attachClient(id string, mx *matrix.Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ra, ok := r.running[id]; ok {
		ra.mx = mx
	}
}

// Clients implements agent.AgentManager.Clients. Only online agents
// (those that completed login and got attachClient'd) appear.
func (r *AgentRuntime) Clients() map[string]*matrix.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]*matrix.Client, len(r.running))
	for id, ra := range r.running {
		if ra.mx != nil {
			out[id] = ra.mx
		}
	}
	return out
}

// List implements agent.AgentManager.List.
func (r *AgentRuntime) List() []agent.AgentInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	server := serverNameFromHomeserver(r.cfg.Homeserver)
	out := []agent.AgentInfo{}
	for _, bc := range r.cfg.AllAgents() {
		_, online := r.running[bc.ID]
		out = append(out, agent.AgentInfo{
			ID:         bc.ID,
			UserID:     "@" + bc.User + ":" + server,
			DeviceName: bc.DeviceName,
			Online:     online,
		})
	}
	return out
}

// Create implements agent.AgentManager.Create. Registers a new
// Matrix user via shared-secret, appends an entry to config.yaml,
// pre-populates a MEMORY.md template, and spawns the new agent's
// goroutine in this process.
func (r *AgentRuntime) Create(req agent.CreateRequest) (agent.AgentInfo, error) {
	if r.cfg.SharedSecret == "" {
		return agent.AgentInfo{}, fmt.Errorf("registration_shared_secret is empty in config.yaml — copy it from data/homeserver.yaml")
	}
	localpart := req.Localpart
	displayName := req.DisplayName
	if displayName == "" {
		displayName = localpart
	}
	r.mu.Lock()
	for _, bc := range r.cfg.AllAgents() {
		if bc.ID == localpart || bc.User == localpart {
			r.mu.Unlock()
			return agent.AgentInfo{}, fmt.Errorf("agent %q already configured", localpart)
		}
	}
	r.mu.Unlock()

	pw, err := genPassword()
	if err != nil {
		return agent.AgentInfo{}, fmt.Errorf("gen password: %w", err)
	}

	reg := &registrar.Registrar{BaseURL: r.cfg.Homeserver, SharedSecret: r.cfg.SharedSecret}
	userID, err := reg.Register(localpart, pw, displayName, false)
	if err != nil {
		return agent.AgentInfo{}, fmt.Errorf("register on synapse: %w", err)
	}
	log.Printf("[runtime] registered %s on Synapse", userID)

	bc := BotConfig{
		ID:                    localpart,
		User:                  localpart,
		Password:              pw,
		DeviceName:            displayName,
		AutoJoinSpaceChildren: true,
		Model:                 req.Model,
		Claude: ClaudeRuntimeConfig{
			PermissionMode: "bypassPermissions",
		},
	}

	dataDir := agentDataDir(r.cfg.DataDir, bc.ID)
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return agent.AgentInfo{}, fmt.Errorf("mkdir data dir: %w", err)
	}
	memPath := filepath.Join(dataDir, "MEMORY.md")
	if _, err := os.Stat(memPath); os.IsNotExist(err) {
		if err := os.WriteFile(memPath, []byte(memoryTemplate(displayName, localpart, req.Description)), 0o600); err != nil {
			log.Printf("[runtime] write MEMORY.md template failed: %v", err)
		}
	}

	r.mu.Lock()
	r.cfg.Agents = append(r.cfg.Agents, bc)
	if err := writeConfig(r.cfgPath, r.cfg); err != nil {
		r.mu.Unlock()
		return agent.AgentInfo{}, fmt.Errorf("write config.yaml: %w", err)
	}
	r.mu.Unlock()

	r.wg.Add(1)
	botCtx, botCancel := context.WithCancel(r.rootCtx)
	r.trackStart(bc, botCancel)
	go func() {
		defer r.wg.Done()
		defer r.trackStop(bc.ID)
		runBot(botCtx, r.cfg, bc, r)
	}()

	return agent.AgentInfo{
		ID:         bc.ID,
		UserID:     userID,
		DeviceName: displayName,
		Online:     true,
	}, nil
}

// serverNameFromHomeserver extracts "host" from "http://host:port"
// by stripping scheme and port. For our local dev it returns
// "localhost"; for a real domain it returns that domain.
func serverNameFromHomeserver(hs string) string {
	s := hs
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	if i := strings.IndexByte(s, '/'); i >= 0 {
		s = s[:i]
	}
	return s
}

func genPassword() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func memoryTemplate(displayName, localpart, description string) string {
	role := description
	if role == "" {
		role = fmt.Sprintf("You are %s, an agent in this self-hosted Matrix workspace.", displayName)
	}
	return fmt.Sprintf(`# %s

## Role
%s

(Edit this file to refine the persona / mission. It is injected as
system prompt at every fresh claude session.)

## Core Goals
- (replace me) what is this agent here to do?

## Style
- 中文回答；技术术语保留英文
- 简洁直接；代码用例不啰嗦

## Constraints
- (replace me) what should this agent never do?

## Multi-agent collaboration

This room may contain other agents (Cindy, Alice, …). Routing is by
@-mention:

- A room with only one agent + the user: broadcast — you reply to
  everything.
- A room with multiple agents: you only reply when the message
  explicitly mentions you (` + "`@%s`" + ` or a Matrix mention pill).
  Messages addressed to another agent, or messages with no mention,
  are silently ignored.

To address a peer agent yourself, write ` + "`@<their-localpart>`" + ` in your
reply (e.g. ` + "`@alice 帮忙 review 一下`" + `). Mosaic detects the token
in your output and injects the protocol-level ` + "`m.mentions.user_ids`" + `
field — that is the only signal a peer's router trusts for bot-to-bot
routing (it ignores plain-text echoes to avoid loops).

Practical rule: put the ` + "`@<peer>`" + ` token **near the start** of your
message. The streaming pipeline only carries the m.mentions field on
the initial Matrix send; if the @ appears late in a long message the
peer may miss the ping.

## Reference
- agent id: %s
- data dir: data/%s/
`, displayName, role, localpart, localpart, localpart)
}

func writeConfig(path string, fc *FileConfig) error {
	data, err := yaml.Marshal(fc)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// trackBridge records a Bridge so /project mutations can fan out
// resolution-cache invalidations to every running agent's bridge.
func (r *AgentRuntime) trackBridge(b *agent.Bridge) {
	r.mu.Lock()
	r.bridges = append(r.bridges, b)
	r.mu.Unlock()
}

// Projects implements agent.AgentManager.Projects.
func (r *AgentRuntime) Projects() []agent.ProjectInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]agent.ProjectInfo, 0, len(r.cfg.Projects))
	for sid, pc := range r.cfg.Projects {
		out = append(out, agent.ProjectInfo{
			SpaceID: sid,
			Name:    pc.Name,
			Cwd:     pc.Cwd,
			Model:   pc.Model,
		})
	}
	return out
}

// EnsureProject implements agent.AgentManager.EnsureProject. Atomic
// "insert if absent": returns created=true only when no entry existed
// for spaceID before this call, so concurrent observers of the same
// new Space can pick a single winner for one-shot side effects like
// creating the default rooms.
func (r *AgentRuntime) EnsureProject(spaceID, name string) (bool, error) {
	if spaceID == "" {
		return false, fmt.Errorf("spaceID required")
	}
	r.mu.Lock()
	if r.cfg.Projects == nil {
		r.cfg.Projects = map[string]ProjectConfigYAML{}
	}
	if _, exists := r.cfg.Projects[spaceID]; exists {
		r.mu.Unlock()
		return false, nil
	}
	r.cfg.Projects[spaceID] = ProjectConfigYAML{Name: name}
	if err := writeConfig(r.cfgPath, r.cfg); err != nil {
		// Best-effort rollback so a failed write doesn't leave a
		// phantom entry visible to in-process callers.
		delete(r.cfg.Projects, spaceID)
		r.mu.Unlock()
		return false, err
	}
	pjs := convertProjectsConfig(r.cfg.Projects)
	bridges := append([]*agent.Bridge(nil), r.bridges...)
	r.mu.Unlock()
	for _, b := range bridges {
		b.InvalidateResolutions(pjs, nil)
	}
	return true, nil
}

// SetProject implements agent.AgentManager.SetProject. Empty fields
// preserve existing values. Persists config to disk and invalidates
// all bridges' resolution caches so the next claude spawn picks up
// the change.
func (r *AgentRuntime) SetProject(spaceID, name, cwd, model string) error {
	if spaceID == "" {
		return fmt.Errorf("spaceID required")
	}
	r.mu.Lock()
	if r.cfg.Projects == nil {
		r.cfg.Projects = map[string]ProjectConfigYAML{}
	}
	cur := r.cfg.Projects[spaceID]
	if name != "" {
		cur.Name = name
	}
	if cwd != "" {
		cur.Cwd = cwd
	}
	if model != "" {
		cur.Model = model
	}
	r.cfg.Projects[spaceID] = cur
	if err := writeConfig(r.cfgPath, r.cfg); err != nil {
		r.mu.Unlock()
		return err
	}
	// Build the agent-package shaped Projects map and broadcast.
	pjs := convertProjectsConfig(r.cfg.Projects)
	bridges := append([]*agent.Bridge(nil), r.bridges...)
	r.mu.Unlock()
	for _, b := range bridges {
		b.InvalidateResolutions(pjs, nil)
	}
	return nil
}
