// mosaic: a Matrix bot that runs Claude Code per room and
// streams output back via message edits. Built for self-hosted Synapse.
//
// Two configuration paths:
//
//  1. config.yaml (preferred; supports multiple bots, per-room cwd):
//
//	   homeserver: http://127.0.0.1:8008
//	   data_dir: ./data
//	   bots:
//	     - id: claude-bot2
//	       user: claude-bot2
//	       password: bot1234
//	       claude:
//	         cwd: /Users/danny0
//	   rooms:
//	     "!abc:localhost":
//	       cwd: /Users/danny0/Code/projectA
//
//  2. Env vars (legacy; one bot only):
//
//	   MX_HOMESERVER, MX_USER, MX_PASSWORD, MX_DEVICE_NAME,
//	   MX_CRYPTO_DB, MX_PICKLE_KEY[_FILE],
//	   CLAUDE_BIN, CLAUDE_CWD, CLAUDE_MODEL, CLAUDE_PERMISSION_MODE
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/deng00/mosaic/pkg/agent"
	// Register all runtime drivers (init() side-effect). Add a new
	// line here per new runtime; the agent.Options.Runtime field is
	// resolved against the registry built up by these imports.
	"github.com/deng00/mosaic/pkg/matrix"
	_ "github.com/deng00/mosaic/pkg/runtime/claude"
	_ "github.com/deng00/mosaic/pkg/runtime/codex"
)

func main() {
	defaultConfig := defaultConfigPath()
	configPath := flag.String("config", defaultConfig, "path to YAML config (default: ~/.mosaic/config.yaml)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fc, err := LoadFile(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	var wg sync.WaitGroup
	var runtime *AgentRuntime
	if fc != nil {
		all := fc.AllAgents()
		log.Printf("loaded %s — %d agent(s)", *configPath, len(all))
		runtime = NewAgentRuntime(ctx, fc, *configPath, &wg)
		for _, bc := range all {
			wg.Add(1)
			botCtx, botCancel := context.WithCancel(ctx)
			runtime.trackStart(bc, botCancel)
			go func(bc BotConfig) {
				defer wg.Done()
				defer runtime.trackStop(bc.ID)
				runBot(botCtx, fc, bc, runtime)
			}(bc)
		}
	} else {
		log.Printf("no %s found — falling back to env vars (single bot)", *configPath)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runEnvBot(ctx)
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("shutdown requested")
		cancel()
	}()

	wg.Wait()
	log.Printf("bye")
}

// runBot drives one agent's lifecycle: login, build bridge, sync
// loop. Each agent gets its own crypto subdir, session store, and
// matrix.Client. mgr is the daemon-level fleet manager passed into
// the bridge so /agent slash commands can list/create.
func runBot(ctx context.Context, fc *FileConfig, bc BotConfig, mgr *AgentRuntime) {
	dataDir := agentDataDir(fc.DataDir, bc.ID)
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		log.Printf("[%s] mkdir data: %v", bc.ID, err)
		return
	}
	pickle, err := pickleKeyFor(dataDir)
	if err != nil {
		log.Printf("[%s] pickle key: %v", bc.ID, err)
		return
	}

	deviceName := bc.DeviceName
	if deviceName == "" {
		// Default device name to the host this mosaic process runs on
		// — that's what the field actually represents (which machine
		// the agent's claude subprocess executes on).
		if h, err := os.Hostname(); err == nil && h != "" {
			deviceName = h
		} else {
			deviceName = bc.ID
		}
	}

	autoJoinSpaceChildren := true
	if bc.AutoJoinSpaceChildren != nil {
		autoJoinSpaceChildren = *bc.AutoJoinSpaceChildren
	}
	mx, err := matrix.Login(ctx, matrix.Config{
		Homeserver:            fc.Homeserver,
		UserID:                bc.User,
		Password:              bc.Password,
		DeviceName:            deviceName,
		CryptoDB:              filepath.Join(dataDir, "crypto.db"),
		PickleKey:             pickle,
		AutoJoinSpaceChildren: autoJoinSpaceChildren,
		MediaDir:              filepath.Join(dataDir, "attachments"),
	})
	if err != nil {
		log.Printf("[%s] login: %v", bc.ID, err)
		return
	}
	log.Printf("[%s] logged in as %s", bc.ID, mx.UserID())
	if mgr != nil {
		mgr.attachClient(bc.ID, mx)
	}
	if bc.DisplayName != "" {
		if err := mx.SetDisplayName(ctx, bc.DisplayName); err != nil {
			log.Printf("[%s] set display name failed: %v", bc.ID, err)
		} else {
			log.Printf("[%s] display name → %q", bc.ID, bc.DisplayName)
		}
	}
	// Push device name every startup — the cryptohelper reuses the
	// same device across restarts so we must PUT to refresh, not rely
	// on InitialDeviceDisplayName (only honored at first login).
	if err := mx.SetDeviceName(ctx, deviceName); err != nil {
		log.Printf("[%s] set device name failed: %v", bc.ID, err)
	} else {
		log.Printf("[%s] device name → %q", bc.ID, deviceName)
	}

	store, err := agent.NewSessionStore(filepath.Join(dataDir, "sessions.json"))
	if err != nil {
		log.Printf("[%s] session store: %v", bc.ID, err)
		return
	}

	rooms := convertRoomsConfig(fc.Rooms)
	projects := convertProjectsConfig(fc.Projects)
	binary := bc.Binary // empty → driver picks default per runtime
	pmode := bc.Claude.PermissionMode
	if pmode == "" {
		pmode = "bypassPermissions"
	}
	model := bc.Model
	if model == "" {
		model = "claude-opus-4-7"
	}
	effort := bc.Effort
	if effort == "" {
		effort = "high"
	}

	br := agent.New(mx, agent.Options{
		Runtime:        bc.Runtime,
		Cwd:            bc.Cwd,
		Model:          model,
		Effort:         effort,
		PermissionMode: pmode,
		Binary:         binary,
		Projects:       projects,
		Rooms:          rooms,
		Sessions:       store,
		Memory:         agent.NewMemory(dataDir, projectsDataDir(fc.DataDir)),
		Manager:        mgr,
		Admins:         fc.Admins,
		ServerName:     fc.ServerName,
		DataDir:        fc.DataDir,
		Env:            bc.Env,
		IgnoreToolsMsg:  resolveIgnoreToolsMsg(&bc),
		EditShowCode:    resolveEditShowCode(bc.Tools.EditShowCode),
		DisallowedTools: resolveDisallowedTools(bc.Tools),
	})
	if mgr != nil {
		mgr.trackBridge(br)
	}
	br.Start()

	log.Printf("[%s] syncing...", bc.ID)
	if err := mx.Sync(ctx); err != nil {
		log.Printf("[%s] sync: %v", bc.ID, err)
	}
	log.Printf("[%s] stopped", bc.ID)
}

// runEnvBot is the legacy single-bot env-var path. Mirrors the
// behavior of the original main.go before YAML config landed.
func runEnvBot(ctx context.Context) {
	cryptoDB := getenv("MX_CRYPTO_DB", "./data/crypto.db")
	if err := os.MkdirAll(filepath.Dir(cryptoDB), 0o755); err != nil {
		log.Fatalf("mkdir crypto db: %v", err)
	}
	pickle := loadOrCreatePickleKeyEnv()

	mx, err := matrix.Login(ctx, matrix.Config{
		Homeserver: getenv("MX_HOMESERVER", "http://127.0.0.1:8008"),
		UserID:     must("MX_USER"),
		Password:   must("MX_PASSWORD"),
		DeviceName: getenv("MX_DEVICE_NAME", "mosaic"),
		CryptoDB:   cryptoDB,
		PickleKey:  pickle,
	})
	if err != nil {
		log.Fatalf("matrix login: %v", err)
	}
	log.Printf("logged in as %s", mx.UserID())

	store, err := agent.NewSessionStore(filepath.Join(filepath.Dir(cryptoDB), "sessions.json"))
	if err != nil {
		log.Fatalf("session store: %v", err)
	}

	br := agent.New(mx, agent.Options{
		Cwd:            getenv("CLAUDE_CWD", ""),
		Model:          os.Getenv("CLAUDE_MODEL"),
		PermissionMode: getenv("CLAUDE_PERMISSION_MODE", "bypassPermissions"),
		Binary:         getenv("CLAUDE_BIN", "claude"),
		Sessions:       store,
		Memory:         agent.NewMemory(filepath.Dir(cryptoDB), filepath.Join(filepath.Dir(cryptoDB), "..", "projects")),
	})
	br.Start()

	log.Printf("syncing...")
	if err := mx.Sync(ctx); err != nil {
		log.Fatalf("sync: %v", err)
	}
}

func convertRoomsConfig(in map[string]RoomConfigYAML) map[string]agent.RoomConfig {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]agent.RoomConfig, len(in))
	for k, v := range in {
		out[k] = agent.RoomConfig{Cwd: v.Cwd, Model: v.Model}
	}
	return out
}

func convertProjectsConfig(in map[string]ProjectConfigYAML) map[string]agent.ProjectConfig {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]agent.ProjectConfig, len(in))
	for k, v := range in {
		out[k] = agent.ProjectConfig{Name: v.Name, Cwd: v.Cwd, Model: v.Model}
	}
	return out
}

func loadOrCreatePickleKeyEnv() []byte {
	if v := os.Getenv("MX_PICKLE_KEY"); v != "" {
		return []byte(v)
	}
	keyPath := getenv("MX_PICKLE_KEY_FILE", "./data/pickle.key")
	if data, err := os.ReadFile(keyPath); err == nil && len(data) >= 32 {
		return data
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		log.Fatalf("mkdir for pickle key: %v", err)
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		log.Fatalf("gen pickle key: %v", err)
	}
	if err := os.WriteFile(keyPath, buf, 0o600); err != nil {
		log.Fatalf("write pickle key: %v", err)
	}
	log.Printf("generated new pickle key at %s", keyPath)
	return buf
}

// agentDataDir returns the on-disk directory for one agent:
// <data>/agents/<id>/. Putting agents under their own subdirectory
// keeps room for sibling categories (projects, shared, etc.) at the
// data-dir root.
func agentDataDir(root, agentID string) string {
	return filepath.Join(root, "agents", agentID)
}

// projectsDataDir returns the shared projects tree:
// <data>/projects/. Multiple agents in the same Space read PROJECT.md
// / SUMMARY.md from this single tree (so /compact updates everyone's
// view, and any operator-curated PROJECT.md is shared too).
func projectsDataDir(root string) string {
	return filepath.Join(root, "projects")
}

// defaultConfigPath returns the canonical config location:
// $HOME/.mosaic/config.yaml. Independent of where the
// binary is run from, so users can `go install ./...` and run from
// anywhere. Override via `-config` flag.
func defaultConfigPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "config.yaml"
	}
	return filepath.Join(home, ".mosaic", "config.yaml")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func must(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", k)
		os.Exit(2)
	}
	return v
}

// defaultIgnoreToolsMsg is the per-agent built-in list of tool names
// whose ToolUse messages aren't relayed into the room — the noisy
// internal-housekeeping tools that don't help the user follow along.
var defaultIgnoreToolsMsg = []string{"Grep", "Read", "Write", "ToolSearch"}

// resolveIgnoreToolsMsg picks the source list in this order:
//  1. bc.Tools.IgnoreTools (new path)
//  2. bc.IgnoreToolsMsg    (legacy path, logs a deprecation note)
//  3. defaultIgnoreToolsMsg
//
// An explicit empty list at either layer means "filter nothing" and is
// honored. Returns the lower-cased lookup set the bridge consumes.
func resolveIgnoreToolsMsg(bc *BotConfig) map[string]bool {
	var src []string
	switch {
	case bc.Tools.IgnoreTools != nil:
		src = *bc.Tools.IgnoreTools
	case bc.IgnoreToolsMsg != nil:
		src = *bc.IgnoreToolsMsg
		log.Printf("[%s] config: 'ignore_tools_msg' is deprecated, use 'tools.ignore_tools' instead", bc.ID)
	default:
		src = defaultIgnoreToolsMsg
	}
	out := make(map[string]bool, len(src))
	for _, name := range src {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out[strings.ToLower(name)] = true
	}
	return out
}

// resolveEditShowCode defaults to true when unset — the diff payload
// is the most useful thing the Edit tool can show.
func resolveEditShowCode(cfg *bool) bool {
	if cfg == nil {
		return true
	}
	return *cfg
}

// resolveDisallowedTools turns the per-tool disable flags into the
// --disallowed-tools list claude understands.
func resolveDisallowedTools(t ToolsConfig) []string {
	var out []string
	if t.TodoWriteDisable {
		out = append(out, "TodoWrite")
	}
	return out
}
