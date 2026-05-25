// Package streamjson runs `claude --print --input-format stream-json
// --output-format stream-json` as a long-lived child process and
// exposes its NDJSON I/O as Go channels.
//
// Input format (per line, written to claude stdin):
//
//	{"type":"user","parent_tool_use_id":null,
//	 "message":{"role":"user","content":"<string or content blocks>"}}
//
// Output format (per line, read from claude stdout): a stream of
// SDKMessage-equivalent events, including `system/init`, `assistant`,
// `user` (with tool_result), `stream_event` (when --include-partial-
// messages), and a terminating `result`. We pass them through as
// `json.RawMessage` so the next layer (convert package) can decode the
// shapes it cares about without forcing a giant tagged union here.
//
// EOF on stdin tells claude "end of input"; close it during shutdown
// so claude finishes the current turn cleanly. SIGINT then kills the
// process if it doesn't exit on its own.
package streamjson

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// staleSessionMarker is the substring claude writes to stderr when it
// fails to find a session id passed via --resume. Detecting it lets
// the bridge auto-recover (clear the cached sid + retry once) instead
// of bouncing the failure back to the user as error_during_execution.
const staleSessionMarker = "No conversation found with session ID"

// Options collect everything Spawn needs to launch claude. Mirrors the
// flags happy-cli's claudeRemote uses (sdk/query.ts options).
type Options struct {
	// Cwd is the working directory claude runs in. Defaults to the
	// current process cwd if empty.
	Cwd string

	// SessionID is the uuid passed via --session-id. Empty means let
	// claude mint one (we then learn it from the system/init event).
	SessionID string

	// Resume is the session-id to resume. Mutually exclusive with
	// SessionID; happy-cli prefers --resume when continuing.
	Resume string

	// Model / FallbackModel are passed via --model / --fallback-model.
	Model         string
	FallbackModel string

	// Effort maps to --effort (low / medium / high / xhigh / max).
	// Empty leaves claude's default in place.
	Effort string

	// PermissionMode maps to --permission-mode. Empty leaves the
	// default in place. Valid values: default, acceptEdits, plan,
	// bypassPermissions, dontAsk, auto.
	PermissionMode string

	// AllowedTools / DisallowedTools become --allowed-tools /
	// --disallowed-tools. Each string is a single rule like
	// "Bash(git *)" — pre-formatted by the caller.
	AllowedTools    []string
	DisallowedTools []string

	// MCPConfigPath becomes --mcp-config <path>. Used for the
	// permission-prompt MCP server.
	MCPConfigPath string

	// PermissionPromptTool is the MCP tool name claude defers to
	// for permission decisions, e.g. "mcp__warden__approve".
	PermissionPromptTool string

	// AppendSystemPrompt and CustomSystemPrompt map to claude flags.
	AppendSystemPrompt string
	CustomSystemPrompt string

	// ExtraArgs is appended verbatim — escape hatch for flags we
	// haven't named yet.
	ExtraArgs []string

	// Binary overrides the executable path; defaults to "claude".
	Binary string

	// ExtraEnv pairs (KEY=VALUE) appended to os.Environ() when claude spawns.
	// Useful for CLAUDE_CODE_OAUTH_TOKEN and other claude-side secrets.
	ExtraEnv []string
}

// Process is a running claude subprocess wired for stream-json I/O.
type Process struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	encoder *json.Encoder
	encMu   sync.Mutex

	events chan json.RawMessage // raw stdout NDJSON lines, decoded once

	doneCh chan error // set to cmd.Wait() error after exit
	doneMu sync.Mutex
	done   bool

	// staleSession latches to true if claude prints the marker that
	// signals a --resume sid we passed no longer exists on disk
	// (typically: cwd switched, .claude/sessions wiped, or the sid
	// was a phantom from a failed prior spawn). Pointer so the stderr
	// tee writer set up before Process exists can share the same
	// flag.
	staleSession *atomic.Bool
}

// StaleSession reports whether claude indicated, via stderr, that the
// --resume session id it was asked to load is missing. Latched: once
// set within a Process's lifetime it stays true.
func (p *Process) StaleSession() bool {
	if p.staleSession == nil {
		return false
	}
	return p.staleSession.Load()
}

// stderrTee wraps an io.Writer to scan each chunk for staleSessionMarker
// before forwarding to the downstream writer. Used to detect stale
// --resume failures without consuming claude's stderr (still wired to
// os.Stderr so launchd logs / operator-facing diagnostics keep
// everything).
type stderrTee struct {
	flag *atomic.Bool
	out  io.Writer
}

func (s *stderrTee) Write(p []byte) (int, error) {
	if !s.flag.Load() && bytes.Contains(p, []byte(staleSessionMarker)) {
		s.flag.Store(true)
	}
	return s.out.Write(p)
}

// UserMessage is the schema written to claude's stdin per turn.
// Content can be a plain string or a slice of content blocks; we
// expose both via constructors.
type UserMessage struct {
	Type            string  `json:"type"` // always "user"
	ParentToolUseID *string `json:"parent_tool_use_id"`
	Message         struct {
		Role    string `json:"role"`    // always "user"
		Content any    `json:"content"` // string | []block
	} `json:"message"`
}

// NewTextMessage builds the simplest UserMessage: plain text content.
func NewTextMessage(text string) UserMessage {
	var m UserMessage
	m.Type = "user"
	m.Message.Role = "user"
	m.Message.Content = text
	return m
}

// ImageBlock is one image attachment for NewMultimodalMessage. Data
// is the raw image bytes; the helper base64-encodes them inline. The
// caller owns the bytes.
type ImageBlock struct {
	MediaType string // e.g. "image/png"
	Data      []byte
}

// NewMultimodalMessage builds a UserMessage with N image content
// blocks followed by an optional trailing text block. Images go first
// because the model attends to them as context for the text caption.
// Empty text + zero images returns an error.
func NewMultimodalMessage(text string, images []ImageBlock) (UserMessage, error) {
	if text == "" && len(images) == 0 {
		return UserMessage{}, fmt.Errorf("streamjson: NewMultimodalMessage needs at least text or one image")
	}
	type imageSource struct {
		Type      string `json:"type"`       // always "base64"
		MediaType string `json:"media_type"` // e.g. "image/png"
		Data      string `json:"data"`       // base64-encoded image bytes
	}
	type block struct {
		Type   string      `json:"type"` // "image" or "text"
		Source *imageSource `json:"source,omitempty"`
		Text   string      `json:"text,omitempty"`
	}
	blocks := make([]block, 0, len(images)+1)
	for _, img := range images {
		mt := img.MediaType
		if mt == "" {
			mt = "image/png"
		}
		blocks = append(blocks, block{
			Type: "image",
			Source: &imageSource{
				Type:      "base64",
				MediaType: mt,
				Data:      base64.StdEncoding.EncodeToString(img.Data),
			},
		})
	}
	if text != "" {
		blocks = append(blocks, block{Type: "text", Text: text})
	}
	var m UserMessage
	m.Type = "user"
	m.Message.Role = "user"
	m.Message.Content = blocks
	return m, nil
}

// BuildArgs renders just the claude flag list from opts (no binary).
// Exposed so callers wrapping claude in another process (e.g. `docker
// run -i ... claude <args>`) can build the same argv tail without
// duplicating the flag-construction code.
func BuildArgs(opts Options) ([]string, error) {
	if opts.SessionID != "" && opts.Resume != "" {
		return nil, fmt.Errorf("streamjson: SessionID and Resume are mutually exclusive")
	}
	args := []string{
		"--print",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--verbose",
		"--include-partial-messages",
	}
	if opts.SessionID != "" {
		args = append(args, "--session-id", opts.SessionID)
	}
	if opts.Resume != "" {
		args = append(args, "--resume", opts.Resume)
	}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	if opts.FallbackModel != "" {
		args = append(args, "--fallback-model", opts.FallbackModel)
	}
	if opts.Effort != "" {
		args = append(args, "--effort", opts.Effort)
	}
	if opts.PermissionMode != "" {
		args = append(args, "--permission-mode", opts.PermissionMode)
	}
	for _, t := range opts.AllowedTools {
		args = append(args, "--allowed-tools", t)
	}
	for _, t := range opts.DisallowedTools {
		args = append(args, "--disallowed-tools", t)
	}
	if opts.MCPConfigPath != "" {
		args = append(args, "--mcp-config", opts.MCPConfigPath)
	}
	if opts.PermissionPromptTool != "" {
		args = append(args, "--permission-prompt-tool", opts.PermissionPromptTool)
	}
	if opts.AppendSystemPrompt != "" {
		args = append(args, "--append-system-prompt", opts.AppendSystemPrompt)
	}
	if opts.CustomSystemPrompt != "" {
		args = append(args, "--system-prompt", opts.CustomSystemPrompt)
	}
	args = append(args, opts.ExtraArgs...)
	return args, nil
}

// SpawnRaw is the underlying primitive: run any binary with NDJSON
// stdin/stdout pipes wired up to a Process value. Stderr always goes
// to os.Stderr. Use this when you need to wrap claude in another
// process (docker run -i, podman, ssh exec, …) — caller is
// responsible for assembling binary + args correctly.
//
// extraEnv pairs (KEY=VALUE) are appended to os.Environ; pass nil to
// inherit the parent env unchanged. cwd "" means "inherit warden's cwd".
func SpawnRaw(ctx context.Context, binary string, args []string, extraEnv []string, cwd string) (*Process, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	staleSession := new(atomic.Bool)
	cmd.Stderr = &stderrTee{flag: staleSession, out: os.Stderr}
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("streamjson: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("streamjson: stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("streamjson: start %s: %w", binary, err)
	}

	p := &Process{
		cmd:          cmd,
		stdin:        stdin,
		encoder:      json.NewEncoder(stdin),
		events:       make(chan json.RawMessage, 64),
		doneCh:       make(chan error, 1),
		staleSession: staleSession,
	}
	go p.readStdout(stdout)
	go p.waitLoop()
	return p, nil
}

// Spawn starts claude under ctx. Caller must Wait or Close to release
// the OS resources. Stderr is wired straight to os.Stderr so claude's
// own diagnostics surface in warden's terminal.
//
// Convenience wrapper around BuildArgs + SpawnRaw for the host case
// (no docker, no extra env).
func Spawn(ctx context.Context, opts Options) (*Process, error) {
	bin := opts.Binary
	if bin == "" {
		bin = "claude"
	}
	args, err := BuildArgs(opts)
	if err != nil {
		return nil, err
	}
	return SpawnRaw(ctx, bin, args, opts.ExtraEnv, opts.Cwd)
}


// Send writes one NDJSON line to claude's stdin. Safe for concurrent
// use; we serialize so multi-line writes never interleave.
func (p *Process) Send(msg UserMessage) error {
	p.encMu.Lock()
	defer p.encMu.Unlock()
	if err := p.encoder.Encode(&msg); err != nil {
		return fmt.Errorf("streamjson: send: %w", err)
	}
	return nil
}

// Events is the read-only channel of every NDJSON event claude emits.
// Closes when claude exits (or stdout otherwise EOFs).
func (p *Process) Events() <-chan json.RawMessage {
	return p.events
}

// Wait blocks until claude exits and returns cmd.Wait() result.
// Cancel ctx (passed to Spawn) to force termination.
func (p *Process) Wait() error {
	return <-p.doneCh
}

// Close attempts a graceful shutdown: close stdin so claude finishes
// any in-flight turn, wait up to grace, then SIGKILL via process kill.
func (p *Process) Close(grace time.Duration) error {
	_ = p.stdin.Close() // signal end-of-input
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case err := <-p.doneCh:
		return err
	case <-t.C:
		_ = p.cmd.Process.Kill()
		return <-p.doneCh
	}
}

// ----- internals -----

func (p *Process) readStdout(r io.Reader) {
	defer close(p.events)
	// Lines from claude can be quite large (full assistant content
	// blocks). bufio.Scanner with default 64K buffer would truncate;
	// switch to a manual ReadBytes loop with a generous limit.
	br := bufio.NewReaderSize(r, 1024*1024) // 1 MiB
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			// Strip trailing newline; copy because ReadBytes shares its buffer.
			trimmed := line[:len(line)-1]
			out := make(json.RawMessage, len(trimmed))
			copy(out, trimmed)
			p.events <- out
		}
		if err != nil {
			return // io.EOF or pipe closed
		}
	}
}

func (p *Process) waitLoop() {
	err := p.cmd.Wait()
	p.doneMu.Lock()
	p.done = true
	p.doneMu.Unlock()
	p.doneCh <- err
	close(p.doneCh)
}
