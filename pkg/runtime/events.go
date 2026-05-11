package runtime

import "encoding/json"

// Event is one chunk of agent output. Drivers translate per-runtime
// stream protocols into these values; the bridge consumes them
// uniformly.
//
// The set is intentionally small and shaped after Mosaic's rendering
// needs (one Matrix message per content block, streamed text via
// edits). Adding a new event variant should be a deliberate UX
// decision, not a passthrough of every runtime quirk.
type Event interface{ isEvent() }

// TextDelta is one fragment of a streaming assistant text block.
// Drivers without token-level streaming (codex) skip these and emit
// TextFinal alone.
type TextDelta struct{ Text string }

// TextFinal closes the current text block. Body is the canonical
// final text — claude's accumulated deltas should match this but
// the assistant event is authoritative.
type TextFinal struct{ Body string }

// Thinking is a reasoning block. Bridge renders as quiet italic
// "💭 …".
type Thinking struct{ Text string }

// ToolUse is one tool invocation. Name is the tool identifier as
// understood by FormatToolUse (Bash / Read / Edit / …). Drivers
// that emit raw-shell calls (codex's command_execution) map to
// Name="Bash" so the existing prettyprinter handles them.
type ToolUse struct {
	ID    string
	Name  string
	Input json.RawMessage
}

// ToolResult is a tool's response. Bridge only renders it when
// IsError=true (success is implicit). ToolName is best-effort —
// claude's tool_result blocks don't carry it; codex's do.
type ToolResult struct {
	ToolUseID string
	ToolName  string
	Content   json.RawMessage
	IsError   bool
}

// SessionInfo carries the runtime-assigned session/thread id
// captured from the first event of a fresh spawn. Bridge persists
// this for later --resume.
type SessionInfo struct{ SessionID string }

// TurnDone marks the end of a turn. Reason categorizes any failure:
//   - ""              success
//   - "max_turns"     hit turn budget
//   - "max_tokens"    output exceeded token budget
//   - "rate_limit"    upstream throttled
//   - "error"         generic / unrecognized
//
// Err is the raw error string from the runtime when available.
type TurnDone struct {
	Err    string
	Reason string
}

func (TextDelta) isEvent()   {}
func (TextFinal) isEvent()   {}
func (Thinking) isEvent()    {}
func (ToolUse) isEvent()     {}
func (ToolResult) isEvent()  {}
func (SessionInfo) isEvent() {}
func (TurnDone) isEvent()    {}
