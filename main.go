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
	"github.com/deng00/mosaic/pkg/dispatch"
	"github.com/deng00/mosaic/pkg/matrix"
	"github.com/deng00/mosaic/pkg/task"
	"github.com/deng00/mosaic/pkg/web"
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
	var taskStore *task.Store
	var webSrv *web.Server
	if fc != nil {
		all := fc.AllAgents()
		log.Printf("loaded %s — %d agent(s)", *configPath, len(all))
		runtime = NewAgentRuntime(ctx, fc, *configPath, &wg)
		taskStore = task.NewStore(projectsDataDir(fc.DataDir))
		runtime.tasks = taskStore

		if fc.Web.Enabled {
			s, err := startWebServer(fc, taskStore, runtime)
			if err != nil {
				log.Printf("web: disabled — %v", err)
			} else {
				webSrv = s
			}
		}

		// Auto-dispatch: tasks moved into in_progress fan out to an
		// idle agent. APIBase / APIToken plumb the per-task callback
		// env when the web server is up; left empty otherwise (the
		// agent then has no programmatic way to flip state, and a
		// human moves it via the board).
		dCfg := dispatch.Config{
			DataDir:              fc.DataDir,
			DefaultWorkspaceRoot: filepath.Join(fc.DataDir, "workspaces"),
		}
		if webSrv != nil {
			dCfg.APIBase = "http://" + webSrv.Addr()
			dCfg.APIToken = webSrv.Token()
		}
		dispatcher := dispatch.New(dCfg, taskStore, newDispatchMemory(fc), newDispatchSink(runtime, fc))
		dispatcher.Start()
		log.Printf("[dispatch] enabled (workspaceRoot=%s, callbackAPI=%q)", dCfg.DefaultWorkspaceRoot, dCfg.APIBase)

		// Push an Element widget into every Space configured with
		// task_prefix so users get a "Tasks" entry in the Space's
		// Widgets panel without manual setup. Idempotent + retried
		// in the background until at least one agent is joined.
		startWidgetPusher(ctx, fc, webSrv, runtime)

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
	if webSrv != nil {
		webSrv.Stop()
	}
	log.Printf("bye")
}

// startWebServer brings up the task-board HTTP listener.
func startWebServer(fc *FileConfig, store *task.Store, rt *AgentRuntime) (*web.Server, error) {
	port := fc.Web.Port
	if port == 0 {
		port = 24527
	}
	bind := fc.Web.Bind
	if bind == "" {
		bind = "127.0.0.1"
	}
	srv, err := web.New(web.Options{
		Bind:      bind,
		Port:      port,
		TokenPath: filepath.Join(fc.DataDir, "web.token"),
		Store:     store,
		Provider:  newWebProvider(rt),
	})
	if err != nil {
		return nil, err
	}
	if err := srv.Start(); err != nil {
		return nil, err
	}
	log.Printf("[web] task board listening on http://%s", srv.Addr())
	log.Printf("[web] bearer token persisted at %s/web.token", fc.DataDir)
	return srv, nil
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
		Env:            bc.Claude.Env,
	})
	if mgr != nil {
		mgr.trackBridge(bc.ID, br)
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
