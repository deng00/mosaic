// Package runtime abstracts the coding-agent CLI (claude / codex /
// future) behind a streaming I/O interface so the Matrix bridge can
// drive any of them through a single consume loop.
//
// Why an abstraction:
//   - Claude Code uses a long-lived process with stream-json over
//     stdin/stdout and rich event vocabulary (text deltas, thinking,
//     tool_use, tool_result, result).
//   - Codex's `exec --json` is per-turn (subprocess exits after each
//     prompt), narrower event vocabulary (thread / turn / item
//     lifecycle), and no token-level streaming.
//   - Future runtimes (opencode, custom SDK) will sit between these
//     two extremes.
//
// Each driver hides its native protocol behind:
//   - Process: Send(text) / Events() / Close()
//   - Event: a small normalized type the bridge consumes uniformly
//     (mirrors what FormatToolUse already renders).
package runtime

import (
	"context"
	"fmt"
)

// Options configures a driver Spawn. Common fields apply to every
// driver; runtime-specific ones are honored where they make sense
// and ignored by drivers that don't have an equivalent (e.g. codex
// has no PermissionMode flag).
type Options struct {
	// Cwd is the working directory the agent runs in. Required.
	Cwd string

	// Resume is the runtime-assigned session id to continue (claude
	// session_id, codex thread_id). Empty starts fresh.
	Resume string

	// SessionID is a caller-pre-allocated id passed via --session-id
	// (claude only). Codex assigns its own thread_id and ignores this.
	SessionID string

	// Model is the model identifier (claude/--model, codex/--model).
	// Empty = driver default.
	Model string

	// Effort is claude's --effort level. Codex ignores.
	Effort string

	// AppendSystemPrompt is extra context injected at session start.
	// Claude wires it through --append-system-prompt; codex (which
	// has no equivalent flag) prepends it to the first user message.
	AppendSystemPrompt string

	// ExtraEnv is KEY=VALUE pairs appended to the subprocess env.
	ExtraEnv []string

	// Binary overrides the executable name (default = driver's
	// preferred binary).
	Binary string

	// --- Claude-specific (other drivers ignore) ---
	PermissionMode       string
	AllowedTools         []string
	DisallowedTools      []string
	MCPConfigPath        string
	PermissionPromptTool string
	CustomSystemPrompt   string
	FallbackModel        string
}

// Attachment is one media file accompanying a user message. Path is
// an absolute filesystem path the runtime can read (matrix client
// downloaded + decrypted before forwarding). MimeType is best-effort.
type Attachment struct {
	Path     string
	MimeType string
	Kind     string // "image" / "file" / "video" / "audio"
	Filename string
}

// Message is one user turn. Text may be empty if it's media-only
// (Element typically inlines the filename into Text for media events).
type Message struct {
	Text        string
	Attachments []Attachment
}

// Process is a running agent session. Long-lived (claude) and per-
// turn-respawn (codex) drivers both satisfy this — the difference is
// hidden inside the driver.
type Process interface {
	// Send delivers a user message to the session. For long-lived
	// drivers this writes to the open stdin; for per-turn drivers it
	// dispatches a new subprocess. Serialized internally so the
	// caller can call Send freely after a previous turn's TurnDone.
	Send(msg Message) error

	// Events streams agent output. Closed when Close() is called or
	// the underlying process exits permanently. For per-turn drivers
	// the channel stays open across turns and emits one TurnDone per
	// finished turn.
	Events() <-chan Event

	// Close terminates the session and releases resources.
	Close() error
}

// Driver is the per-runtime factory. Registered via init() side-effect
// in each driver subpackage.
type Driver interface {
	// ID is the runtime identifier ("claude", "codex"). Lowercase,
	// stable — written into config.yaml and never changes once
	// shipped.
	ID() string

	// Spawn launches a session. Cancel ctx to terminate.
	Spawn(ctx context.Context, opts Options) (Process, error)
}

var drivers = map[string]Driver{}

// Register adds a driver. Called from each driver subpackage's init().
func Register(d Driver) {
	drivers[d.ID()] = d
}

// Get returns the driver for runtime. Empty runtime defaults to "claude".
func Get(runtime string) (Driver, error) {
	if runtime == "" {
		runtime = "claude"
	}
	d, ok := drivers[runtime]
	if !ok {
		return nil, fmt.Errorf("runtime: unknown runtime %q (registered: %v)", runtime, Registered())
	}
	return d, nil
}

// Registered returns the IDs of registered drivers.
func Registered() []string {
	out := make([]string, 0, len(drivers))
	for k := range drivers {
		out = append(out, k)
	}
	return out
}
