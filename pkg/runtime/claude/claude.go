// Package claude is the Claude Code driver for pkg/runtime.
// It wraps pkg/claude/streamjson (long-lived subprocess + stream-json
// over stdin/stdout) and translates the stream-json event schema to
// normalized runtime.Event values.
//
// Import for side-effect (init() registers the driver):
//
//	import _ "github.com/deng00/mosaic/pkg/runtime/claude"
package claude

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/deng00/mosaic/pkg/claude/streamjson"
	"github.com/deng00/mosaic/pkg/runtime"
)

// ID is the registry key.
const ID = "claude"

func init() { runtime.Register(driver{}) }

type driver struct{}

func (driver) ID() string { return ID }

func (driver) Spawn(ctx context.Context, opts runtime.Options) (runtime.Process, error) {
	binary := opts.Binary
	if binary == "" {
		binary = "claude"
	}
	sjOpts := streamjson.Options{
		Cwd:                  opts.Cwd,
		SessionID:            opts.SessionID,
		Resume:               opts.Resume,
		Model:                opts.Model,
		Effort:               opts.Effort,
		FallbackModel:        opts.FallbackModel,
		PermissionMode:       opts.PermissionMode,
		AllowedTools:         opts.AllowedTools,
		DisallowedTools:      opts.DisallowedTools,
		MCPConfigPath:        opts.MCPConfigPath,
		PermissionPromptTool: opts.PermissionPromptTool,
		AppendSystemPrompt:   opts.AppendSystemPrompt,
		CustomSystemPrompt:   opts.CustomSystemPrompt,
		Binary:               binary,
		ExtraEnv:             opts.ExtraEnv,
	}
	sj, err := streamjson.Spawn(ctx, sjOpts)
	if err != nil {
		return nil, err
	}
	p := &process{sj: sj, events: make(chan runtime.Event, 32)}
	go p.pump()
	return p, nil
}

type process struct {
	sj     *streamjson.Process
	events chan runtime.Event
}

func (p *process) Send(msg runtime.Message) error {
	if len(msg.Attachments) == 0 {
		return p.sj.Send(streamjson.NewTextMessage(msg.Text))
	}
	// Multimodal: build a content-block array with one image block
	// per attachment (skipping unknown kinds — claude only really
	// consumes images today), and a trailing text block for the
	// caption. Reads each image into memory and base64-encodes inline;
	// claude's stream-json doesn't have a file-reference variant.
	um, err := streamjson.NewMultimodalMessage(msg.Text, attachmentsToBlocks(msg.Attachments))
	if err != nil {
		return fmt.Errorf("build multimodal message: %w", err)
	}
	return p.sj.Send(um)
}

// attachmentsToBlocks reads each image attachment off disk and
// returns the per-block payloads streamjson.NewMultimodalMessage
// expects. Non-image attachments are skipped with a log line —
// claude's stream-json API takes images only.
func attachmentsToBlocks(atts []runtime.Attachment) []streamjson.ImageBlock {
	out := make([]streamjson.ImageBlock, 0, len(atts))
	for _, a := range atts {
		if a.Kind != "image" {
			continue
		}
		data, err := os.ReadFile(a.Path)
		if err != nil {
			continue
		}
		mt := a.MimeType
		if mt == "" {
			mt = "image/png"
		}
		out = append(out, streamjson.ImageBlock{MediaType: mt, Data: data})
	}
	return out
}

func (p *process) Events() <-chan runtime.Event { return p.events }

// Close gives the subprocess up to 2s to drain after closing stdin —
// matches what the bridge used to pass to streamjson.Process.Close
// directly.
func (p *process) Close() error { return p.sj.Close(2 * time.Second) }

func (p *process) pump() {
	defer close(p.events)
	for raw := range p.sj.Events() {
		for _, ev := range translate(raw) {
			p.events <- ev
		}
	}
}

// translate maps one claude stream-json event to zero or more
// normalized runtime.Events. The mapping is:
//
//	stream_event content_block_delta text_delta → TextDelta
//	assistant.content[].type:
//	  text     → TextFinal (canonical)
//	  thinking → Thinking
//	  tool_use → ToolUse
//	user.content[].type:
//	  tool_result with is_error → ToolResult
//	system subtype:init → SessionInfo
//	result               → TurnDone (Reason from subtype)
//
// Anything else is silently dropped — the bridge cares only about
// what should reach the chat timeline.
func translate(raw json.RawMessage) []runtime.Event {
	var head struct {
		Type    string `json:"type"`
		Subtype string `json:"subtype"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil
	}

	switch head.Type {
	case "stream_event":
		var ev struct {
			Event struct {
				Type  string `json:"type"`
				Delta struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"delta"`
			} `json:"event"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil
		}
		if ev.Event.Type == "content_block_delta" && ev.Event.Delta.Type == "text_delta" {
			return []runtime.Event{runtime.TextDelta{Text: ev.Event.Delta.Text}}
		}
		return nil

	case "assistant":
		var ev struct {
			Message struct {
				Content []struct {
					Type  string          `json:"type"`
					Text  string          `json:"text"`
					Name  string          `json:"name"`
					ID    string          `json:"id"`
					Input json.RawMessage `json:"input"`
				} `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil
		}
		out := make([]runtime.Event, 0, len(ev.Message.Content))
		for _, blk := range ev.Message.Content {
			switch blk.Type {
			case "text":
				out = append(out, runtime.TextFinal{Body: blk.Text})
			case "thinking":
				out = append(out, runtime.Thinking{Text: blk.Text})
			case "tool_use":
				out = append(out, runtime.ToolUse{
					ID:    blk.ID,
					Name:  blk.Name,
					Input: blk.Input,
				})
			}
		}
		return out

	case "user":
		var ev struct {
			Message struct {
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil
		}
		var blocks []struct {
			Type      string          `json:"type"`
			ToolUseID string          `json:"tool_use_id"`
			Content   json.RawMessage `json:"content"`
			IsError   bool            `json:"is_error"`
		}
		if err := json.Unmarshal(ev.Message.Content, &blocks); err != nil {
			return nil
		}
		out := make([]runtime.Event, 0)
		for _, blk := range blocks {
			if blk.Type == "tool_result" && blk.IsError {
				out = append(out, runtime.ToolResult{
					ToolUseID: blk.ToolUseID,
					Content:   blk.Content,
					IsError:   true,
				})
			}
		}
		return out

	case "system":
		if head.Subtype != "init" {
			return nil
		}
		var ev struct {
			SessionID string `json:"session_id"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil || ev.SessionID == "" {
			return nil
		}
		return []runtime.Event{runtime.SessionInfo{SessionID: ev.SessionID}}

	case "result":
		var ev struct {
			Subtype string `json:"subtype"`
		}
		_ = json.Unmarshal(raw, &ev)
		td := runtime.TurnDone{}
		if strings.HasPrefix(ev.Subtype, "error") {
			td.Err = ev.Subtype
			switch ev.Subtype {
			case "error_max_turns":
				td.Reason = "max_turns"
			case "error_max_tokens":
				td.Reason = "max_tokens"
			default:
				td.Reason = "error"
			}
		}
		return []runtime.Event{td}
	}
	return nil
}
