// Package opencode is the OpenCode CLI driver for pkg/runtime.
//
// Architectural notes (closely mirrors the codex driver):
//
//   - Like codex, opencode's `opencode run --format json` is per-turn:
//     the subprocess reads one prompt and exits. We model the session
//     as a serial loop in a background goroutine: Send enqueues, the
//     loop spawns one `opencode run` per prompt and pumps its NDJSON
//     output as Events.
//
//   - Session continuity uses opencode's `--session <id>`. The first
//     turn doesn't pass it; the sessionID embedded in every event is
//     captured into p.sid and reused on subsequent turns.
//
//   - opencode emits *completed* parts only (not token deltas) — each
//     `text` event already carries the full text block. We surface
//     them as TextFinal; the bridge falls back to one Matrix message
//     per TextFinal.
//
//   - opencode bundles tool_call + tool_result into a single
//     `tool_use` event (state.input + state.output + state.status are
//     all present). We split that into a runtime.ToolUse plus, on
//     non-completed status, a runtime.ToolResult{IsError: true}.
//
//   - No --append-system-prompt equivalent. Like codex, the driver
//     prepends opts.AppendSystemPrompt to the very first prompt as
//     a tagged block so mosaic's memory layer still flows in.
//
//   - opencode's tool names are lower-case ("bash", "edit", ...); the
//     bridge's renderer expects PascalCase ("Bash", "Edit", ...) — we
//     normalize at the driver boundary.
//
// Import for side-effect to register the driver:
//
//	import _ "github.com/deng00/mosaic/pkg/runtime/opencode"
package opencode

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/deng00/mosaic/pkg/runtime"
)

// ID is the registry key.
const ID = "opencode"

func init() { runtime.Register(driver{}) }

type driver struct{}

func (driver) ID() string { return ID }

func (driver) Spawn(ctx context.Context, opts runtime.Options) (runtime.Process, error) {
	binary := opts.Binary
	if binary == "" {
		binary = "opencode"
	}
	p := &process{
		ctx:                 ctx,
		opts:                opts,
		binary:              binary,
		events:              make(chan runtime.Event, 32),
		inbox:               make(chan runtime.Message, 1),
		done:                make(chan struct{}),
		sid:                 opts.Resume,
		pendingPrependFirst: opts.AppendSystemPrompt,
	}
	go p.loop()
	return p, nil
}

type process struct {
	ctx    context.Context
	opts   runtime.Options
	binary string
	events chan runtime.Event
	inbox  chan runtime.Message

	done      chan struct{}
	closeOnce sync.Once

	// sid is the opencode session id. Empty on a fresh launch; populated
	// from the first event's sessionID field and reused via --session
	// on every subsequent turn.
	sid string

	// sidEmittedOnce ensures we surface SessionInfo at most once per
	// driver lifetime — bridge persists it on first sighting.
	sidEmittedOnce sync.Once

	// pendingPrependFirst is AppendSystemPrompt material to inline into
	// the FIRST turn's user prompt (opencode has no equivalent flag).
	// Cleared after the first turn runs. Only relevant when sid is
	// empty at construction (no Resume) — resumed sessions already have
	// the system prompt baked into opencode's server-side state.
	pendingPrependFirst string
}

func (p *process) Send(msg runtime.Message) error {
	select {
	case p.inbox <- msg:
		return nil
	case <-p.done:
		return fmt.Errorf("opencode: process closed")
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *process) Events() <-chan runtime.Event { return p.events }

func (p *process) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	return nil
}

// StaleSession satisfies runtime.Process. opencode signals an unknown
// --session id through structured events / exit code rather than a
// stable stderr marker, so we always return false for now. If a
// reliable detection lands, hook it here.
func (p *process) StaleSession() bool { return false }

// loop dispatches one turn at a time. Strictly serial — matches the
// bridge's per-room serial inbox so two turns in the same room can't
// race each other into overlapping opencode subprocesses.
func (p *process) loop() {
	defer close(p.events)
	for {
		select {
		case <-p.done:
			return
		case <-p.ctx.Done():
			return
		case msg := <-p.inbox:
			p.runTurn(msg)
		}
	}
}

// runTurn spawns one `opencode run` invocation and pumps its NDJSON
// output. Emits exactly one TurnDone at the end regardless of success
// path or subprocess error.
func (p *process) runTurn(msg runtime.Message) {
	prompt := msg.Text
	if p.pendingPrependFirst != "" && p.sid == "" {
		prompt = "<mosaic_system_prompt>\n" + p.pendingPrependFirst + "\n</mosaic_system_prompt>\n\n" + msg.Text
		p.pendingPrependFirst = ""
	}

	args := []string{"run", "--format", "json", "--dangerously-skip-permissions"}
	if p.sid != "" {
		args = append(args, "--session", p.sid)
	}
	if p.opts.Model != "" {
		args = append(args, "--model", p.opts.Model)
	}
	if p.opts.Cwd != "" {
		args = append(args, "--dir", p.opts.Cwd)
	}
	// File attachments (images and other supported kinds). opencode's
	// -f/--file accepts repeated paths.
	for _, a := range msg.Attachments {
		if a.Path == "" {
			continue
		}
		args = append(args, "--file", a.Path)
	}
	args = append(args, prompt)

	cmd := exec.CommandContext(p.ctx, p.binary, args...)
	if p.opts.Cwd != "" {
		cmd.Dir = p.opts.Cwd
	}
	if len(p.opts.ExtraEnv) > 0 {
		cmd.Env = append(os.Environ(), p.opts.ExtraEnv...)
	}
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		p.emit(runtime.TurnDone{Err: err.Error(), Reason: "error"})
		return
	}
	if err := cmd.Start(); err != nil {
		p.emit(runtime.TurnDone{Err: err.Error(), Reason: "error"})
		return
	}

	sc := bufio.NewScanner(stdout)
	// Tool outputs (e.g. a long shell command's stdout) can be tens of
	// KB embedded in a single event line. Raise the line limit so a
	// noisy turn doesn't break parsing mid-event.
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		// opencode occasionally prints non-JSON status lines on the
		// first run ("Performing one time database migration...",
		// "Shell cwd was reset to ..."). Skip anything that isn't an
		// object literal — none of it is part of the event stream.
		if line == "" || line[0] != '{' {
			continue
		}
		for _, ev := range p.translate(line) {
			p.emit(ev)
		}
	}
	if waitErr := cmd.Wait(); waitErr != nil {
		log.Printf("[opencode] turn subprocess exit: %v", waitErr)
		p.emit(runtime.TurnDone{Err: waitErr.Error(), Reason: "error"})
		return
	}
	p.emit(runtime.TurnDone{})
}

// translate maps one opencode NDJSON event to zero or more normalized
// runtime.Events. The schema is:
//
//	{
//	  "type":      "step_start" | "text" | "tool_use" | "step_finish",
//	  "sessionID": "ses_...",
//	  "part":      { ... per-type payload ... }
//	}
//
// Mapping:
//
//	first sessionID seen  → SessionInfo (captured + persisted by bridge)
//	type=text             → TextFinal (part.text)
//	type=tool_use         → ToolUse (+ ToolResult on non-completed status)
//	type=step_start       → ignored (turn-internal housekeeping)
//	type=step_finish      → ignored (TurnDone is emitted in runTurn after Wait)
func (p *process) translate(line string) []runtime.Event {
	var head struct {
		Type      string          `json:"type"`
		SessionID string          `json:"sessionID"`
		Part      json.RawMessage `json:"part"`
	}
	if err := json.Unmarshal([]byte(line), &head); err != nil {
		return nil
	}

	var out []runtime.Event
	if head.SessionID != "" {
		if p.sid == "" {
			p.sid = head.SessionID
		}
		p.sidEmittedOnce.Do(func() {
			out = append(out, runtime.SessionInfo{SessionID: head.SessionID})
		})
	}

	switch head.Type {
	case "text":
		var part struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(head.Part, &part); err == nil && part.Text != "" {
			out = append(out, runtime.TextFinal{Body: part.Text})
		}

	case "tool_use":
		var part struct {
			Tool   string `json:"tool"`
			CallID string `json:"callID"`
			State  struct {
				Status string          `json:"status"`
				Input  json.RawMessage `json:"input"`
				Output string          `json:"output"`
			} `json:"state"`
		}
		if err := json.Unmarshal(head.Part, &part); err != nil {
			return out
		}
		name := pascalToolName(part.Tool)
		input := part.State.Input
		if len(input) == 0 {
			input = json.RawMessage("{}")
		}
		// opencode's tool schemas use camelCase keys (filePath /
		// oldString / replaceAll / ...) while pkg/agent/format.go was
		// written against claude's snake_case convention (file_path /
		// old_string / replace_all). Rewrite recursively here so the
		// renderer picks up every field uniformly without needing per-
		// tool special-casing on either side.
		input = camelKeysToSnake(input)
		out = append(out, runtime.ToolUse{
			ID:    part.CallID,
			Name:  name,
			Input: input,
		})
		// opencode bundles the result into the same event. Mosaic only
		// surfaces tool_result on error, so emit it conditionally.
		if part.State.Status != "" && part.State.Status != "completed" {
			content, _ := json.Marshal(part.State.Output)
			out = append(out, runtime.ToolResult{
				ToolUseID: part.CallID,
				ToolName:  name,
				Content:   content,
				IsError:   true,
			})
		}
	}
	return out
}

// pascalToolName converts opencode's lower-case tool ids to the
// PascalCase shape pkg/agent/format.go's renderer expects. Unknown
// names get a best-effort title-case so future tools still surface
// (just without a custom emoji until format.go gets a case for them).
func pascalToolName(s string) string {
	switch strings.ToLower(s) {
	case "bash":
		return "Bash"
	case "read":
		return "Read"
	case "write":
		return "Write"
	case "edit":
		return "Edit"
	case "grep":
		return "Grep"
	case "glob":
		return "Glob"
	case "webfetch":
		return "WebFetch"
	case "websearch":
		return "WebSearch"
	case "task":
		return "Agent"
	}
	if s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// camelKeysToSnake rewrites every object key in the given JSON
// payload from camelCase to snake_case (recursively into nested
// objects + arrays). Non-key values are passed through unchanged.
// Returns the original payload on parse failure so a malformed input
// doesn't turn into an empty {}.
func camelKeysToSnake(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	conv := convertJSONKeys(v)
	out, err := json.Marshal(conv)
	if err != nil {
		return raw
	}
	return out
}

func convertJSONKeys(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, vv := range x {
			m[camelToSnake(k)] = convertJSONKeys(vv)
		}
		return m
	case []any:
		for i, item := range x {
			x[i] = convertJSONKeys(item)
		}
		return x
	default:
		return v
	}
}

// camelToSnake lower-cases the first character and inserts an
// underscore before every subsequent uppercase ASCII letter:
//
//	"filePath"   → "file_path"
//	"oldString"  → "old_string"
//	"replaceAll" → "replace_all"
//	"command"    → "command"
//
// Edge case: consecutive caps ("HTTPClient") become "h_t_t_p_client",
// but tool-input keys don't use that style.
func camelToSnake(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		if i == 0 && r >= 'A' && r <= 'Z' {
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (p *process) emit(ev runtime.Event) {
	select {
	case p.events <- ev:
	case <-p.done:
	case <-p.ctx.Done():
	}
}
