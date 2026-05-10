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
	"sync"
	"syscall"

	"github.com/deng00/mosaic/pkg/agent"
	"github.com/deng00/mosaic/pkg/matrix"
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
		deviceName = "mosaic (" + bc.ID + ")"
	}

	mx, err := matrix.Login(ctx, matrix.Config{
		Homeserver:            fc.Homeserver,
		UserID:                bc.User,
		Password:              bc.Password,
		DeviceName:            deviceName,
		CryptoDB:              filepath.Join(dataDir, "crypto.db"),
		PickleKey:             pickle,
		AutoJoinSpaceChildren: bc.AutoJoinSpaceChildren,
	})
	if err != nil {
		log.Printf("[%s] login: %v", bc.ID, err)
		return
	}
	log.Printf("[%s] logged in as %s", bc.ID, mx.UserID())

	store, err := agent.NewSessionStore(filepath.Join(dataDir, "sessions.json"))
	if err != nil {
		log.Printf("[%s] session store: %v", bc.ID, err)
		return
	}

	rooms := convertRoomsConfig(fc.Rooms)
	projects := convertProjectsConfig(fc.Projects)
	binary := bc.Claude.Binary
	if binary == "" {
		binary = "claude"
	}
	pmode := bc.Claude.PermissionMode
	if pmode == "" {
		pmode = "bypassPermissions"
	}

	br := agent.New(mx, agent.Options{
		Cwd:            bc.Claude.Cwd,
		Model:          bc.Claude.Model,
		PermissionMode: pmode,
		Binary:         binary,
		Projects:       projects,
		Rooms:          rooms,
		Sessions:       store,
		Memory:         agent.NewMemory(dataDir, projectsDataDir(fc.DataDir)),
		Manager:        mgr,
		Admins:         fc.Admins,
		Members:        fc.Members,
		ServerName:     fc.ServerName,
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
