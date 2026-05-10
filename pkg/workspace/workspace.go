// Package workspace gives each task its own filesystem playground at
// <root>/<task-id>/. Bash hooks (after_create, before_run, after_run,
// before_remove) run with the workspace as cwd. Modeled on
// cs-symphony's workspace manager but trimmed for Mosaic's needs.
//
// Typical hook content (configured in config.yaml under
// projects.<spaceID>.workspace_hooks):
//
//	after_create: |
//	  set -e
//	  git clone https://github.com/me/repo.git .
//	before_run: |
//	  set -e
//	  git fetch origin
//	  git checkout main
//	  git pull --ff-only
//
// Hook failures in after_create / before_run abort the task and bubble
// up; failures in after_run / before_remove are logged and swallowed
// (the task is already past the point of needing them).
package workspace

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Hooks bundles the four lifecycle scripts. Empty strings are no-ops.
type Hooks struct {
	AfterCreate  string        `yaml:"after_create,omitempty"`
	BeforeRun    string        `yaml:"before_run,omitempty"`
	AfterRun     string        `yaml:"after_run,omitempty"`
	BeforeRemove string        `yaml:"before_remove,omitempty"`
	Timeout      time.Duration `yaml:"-"`
}

// Workspace is one task's per-task directory.
type Workspace struct {
	TaskID     string
	Path       string
	CreatedNow bool // true if Create just made the directory; hooks ran
}

// Manager owns workspace lifecycle for a tree of per-task dirs.
type Manager struct {
	Root  string // e.g. ~/.mosaic/workspaces
	Hooks Hooks
}

func NewManager(root string, hooks Hooks) *Manager {
	if hooks.Timeout <= 0 {
		hooks.Timeout = 2 * time.Minute
	}
	return &Manager{Root: root, Hooks: hooks}
}

var sanitizeRE = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// SanitizeID strips characters that don't belong in a path component.
// Task ids look like "MOS-4" so this is rarely a no-op, but defensive
// against future formats.
func SanitizeID(id string) string {
	return sanitizeRE.ReplaceAllString(id, "_")
}

// Create ensures <root>/<sanitized(taskID)>/ exists. If newly created,
// runs after_create with cwd set to the new directory. Returns the
// workspace handle.
func (m *Manager) Create(ctx context.Context, taskID string) (*Workspace, error) {
	if err := os.MkdirAll(m.Root, 0o755); err != nil {
		return nil, fmt.Errorf("workspace: ensure root %s: %w", m.Root, err)
	}
	rootAbs, err := filepath.Abs(m.Root)
	if err != nil {
		return nil, fmt.Errorf("workspace: abs root: %w", err)
	}
	key := SanitizeID(taskID)
	if key == "" {
		return nil, errors.New("workspace: empty sanitized task id")
	}
	pathAbs := filepath.Join(rootAbs, key)
	// Refuse paths that escape the root via "..", absolute components, etc.
	if !strings.HasPrefix(pathAbs+string(filepath.Separator), rootAbs+string(filepath.Separator)) && pathAbs != rootAbs {
		return nil, fmt.Errorf("workspace: refused path %q outside root %q", pathAbs, rootAbs)
	}
	createdNow := false
	if _, err := os.Stat(pathAbs); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(pathAbs, 0o755); err != nil {
			return nil, fmt.Errorf("workspace: mkdir: %w", err)
		}
		createdNow = true
	} else if err != nil {
		return nil, fmt.Errorf("workspace: stat: %w", err)
	}
	ws := &Workspace{TaskID: taskID, Path: pathAbs, CreatedNow: createdNow}
	if createdNow && m.Hooks.AfterCreate != "" {
		if err := m.runHook(ctx, m.Hooks.AfterCreate, ws.Path, "after_create"); err != nil {
			return ws, err
		}
	}
	return ws, nil
}

// BeforeRun runs the before_run hook (if configured); failure aborts
// the run.
func (m *Manager) BeforeRun(ctx context.Context, ws *Workspace) error {
	if m.Hooks.BeforeRun == "" {
		return nil
	}
	return m.runHook(ctx, m.Hooks.BeforeRun, ws.Path, "before_run")
}

// AfterRun runs after_run if configured; failures only logged. Used
// for cleanup that is best-effort.
func (m *Manager) AfterRun(ctx context.Context, ws *Workspace) {
	if m.Hooks.AfterRun == "" {
		return
	}
	if err := m.runHook(ctx, m.Hooks.AfterRun, ws.Path, "after_run"); err != nil {
		log.Printf("[workspace] after_run failed: %v", err)
	}
}

// Remove runs before_remove (best-effort) and rm -rf's the workspace.
// Refuses to operate on paths outside Root.
func (m *Manager) Remove(ctx context.Context, taskID string) error {
	key := SanitizeID(taskID)
	rootAbs, err := filepath.Abs(m.Root)
	if err != nil {
		return err
	}
	pathAbs := filepath.Join(rootAbs, key)
	if _, err := os.Stat(pathAbs); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if !strings.HasPrefix(pathAbs+string(filepath.Separator), rootAbs+string(filepath.Separator)) {
		return fmt.Errorf("workspace: refused to remove path outside root: %s", pathAbs)
	}
	if m.Hooks.BeforeRemove != "" {
		if err := m.runHook(ctx, m.Hooks.BeforeRemove, pathAbs, "before_remove"); err != nil {
			log.Printf("[workspace] before_remove failed (continuing with rm): %v", err)
		}
	}
	return os.RemoveAll(pathAbs)
}

// runHook runs a bash script with the workspace as cwd, capping output
// and time. Logs the (truncated) output for diagnostics.
func (m *Manager) runHook(ctx context.Context, script, cwd, name string) error {
	hookCtx, cancel := context.WithTimeout(ctx, m.Hooks.Timeout)
	defer cancel()
	cmd := exec.CommandContext(hookCtx, "bash", "-lc", script)
	cmd.Dir = cwd
	out, err := cmd.CombinedOutput()
	truncated := string(out)
	if len(truncated) > 4096 {
		truncated = truncated[:4096] + "...[truncated]"
	}
	if err != nil {
		log.Printf("[workspace] hook %s in %s failed: %v\noutput:\n%s", name, cwd, err, truncated)
		return fmt.Errorf("%s hook failed: %w", name, err)
	}
	if truncated != "" {
		log.Printf("[workspace] hook %s output:\n%s", name, truncated)
	}
	return nil
}
