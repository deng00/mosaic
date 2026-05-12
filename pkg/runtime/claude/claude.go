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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
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
	p := &process{
		sj:            sj,
		events:        make(chan runtime.Event, 32),
		cwd:           opts.Cwd,
		pendingWrites: map[string]string{},
		emittedPaths:  map[string]bool{},
	}
	go p.pump()
	return p, nil
}

type process struct {
	sj     *streamjson.Process
	events chan runtime.Event

	// cwd is the absolute working directory of this session. Used to
	// constrain path-based image detection: only files under cwd are
	// candidates so we never surface arbitrary host files.
	cwd string

	// pendingWrites maps tool_use_id → file path for in-flight Write
	// calls that target an image extension. Cleared once the matching
	// tool_result (success) emits an ImageFinal, or replaced/dropped
	// on overwrite.
	mu            sync.Mutex
	pendingWrites map[string]string
	// emittedPaths dedupes ImageFinal per path within this process
	// lifetime — Claude often re-mentions paths across turns.
	emittedPaths map[string]bool
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
		for _, ev := range p.translate(raw) {
			p.events <- ev
		}
	}
}

// translate maps one claude stream-json event to zero or more
// normalized runtime.Events. The mapping is:
//
//	stream_event content_block_delta text_delta → TextDelta
//	assistant.content[].type:
//	  text     → TextFinal (canonical) + ImageFinal for any
//	             image paths under cwd that exist on disk
//	  thinking → Thinking
//	  tool_use → ToolUse; Write/Edit with image extension is
//	             remembered for ImageFinal-on-success below
//	user.content[].type:
//	  tool_result with is_error → ToolResult
//	  tool_result success → ImageFinal for the remembered Write
//	                        path, plus any embedded image content
//	                        blocks (base64 → temp file)
//	system subtype:init → SessionInfo
//	result               → TurnDone (Reason from subtype)
//
// Anything else is silently dropped — the bridge cares only about
// what should reach the chat timeline.
func (p *process) translate(raw json.RawMessage) []runtime.Event {
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
				for _, img := range p.scanTextForImages(blk.Text) {
					out = append(out, img)
				}
			case "thinking":
				out = append(out, runtime.Thinking{Text: blk.Text})
			case "tool_use":
				out = append(out, runtime.ToolUse{
					ID:    blk.ID,
					Name:  blk.Name,
					Input: blk.Input,
				})
				p.recordPendingWrite(blk.ID, blk.Name, blk.Input)
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
			if blk.Type != "tool_result" {
				continue
			}
			if blk.IsError {
				p.forgetPendingWrite(blk.ToolUseID)
				out = append(out, runtime.ToolResult{
					ToolUseID: blk.ToolUseID,
					Content:   blk.Content,
					IsError:   true,
				})
				continue
			}
			// Success: resolve any pending Write/Edit that targeted an
			// image path, and harvest any embedded image content blocks.
			if path := p.takePendingWrite(blk.ToolUseID); path != "" {
				if img, ok := p.maybeEmitImage(path, ""); ok {
					out = append(out, img)
				}
			}
			for _, img := range p.extractToolResultImages(blk.Content) {
				out = append(out, img)
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

// imageExt is the set of file extensions we surface as m.image.
// SVG intentionally included — Element renders it fine and it's a
// common output of charting / diagram tools.
var imageExt = map[string]string{
	".png":  "image/png",
	".jpg":  "image/jpeg",
	".jpeg": "image/jpeg",
	".gif":  "image/gif",
	".webp": "image/webp",
	".svg":  "image/svg+xml",
}

// pathPattern matches absolute or ~-prefixed paths ending in an
// image extension. Used to harvest image paths from the assistant's
// prose ("saved chart to /tmp/foo.png"). Conservative on purpose —
// only paths starting with `/` or `~/` are considered, so URLs and
// in-prose filenames without a parent component are skipped.
var pathPattern = regexp.MustCompile(`(?:~|/)[^\s'"\x60()<>]*\.(?:png|jpe?g|gif|webp|svg)\b`)

// scanTextForImages pulls image file paths out of an assistant text
// block. Each unique resolvable path that exists on disk and sits
// under the session cwd becomes one ImageFinal event. Out-of-tree
// paths are dropped — we never want to leak arbitrary host files
// just because the model mentioned one.
func (p *process) scanTextForImages(text string) []runtime.Event {
	if p.cwd == "" || text == "" {
		return nil
	}
	matches := pathPattern.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}
	var out []runtime.Event
	for _, m := range matches {
		if ev, ok := p.maybeEmitImage(m, ""); ok {
			out = append(out, ev)
		}
	}
	return out
}

// recordPendingWrite is called for every assistant tool_use block.
// When the tool is Write / Edit / NotebookEdit (the file-mutating
// builtins) and the target file_path ends in an image extension, we
// remember it so the corresponding tool_result can emit ImageFinal
// on success.
func (p *process) recordPendingWrite(toolUseID, name string, input json.RawMessage) {
	if toolUseID == "" {
		return
	}
	switch name {
	case "Write", "Edit", "NotebookEdit":
	default:
		return
	}
	var in struct {
		FilePath string `json:"file_path"`
	}
	if err := json.Unmarshal(input, &in); err != nil || in.FilePath == "" {
		return
	}
	if _, ok := imageExt[strings.ToLower(filepath.Ext(in.FilePath))]; !ok {
		return
	}
	p.mu.Lock()
	p.pendingWrites[toolUseID] = in.FilePath
	p.mu.Unlock()
}

func (p *process) takePendingWrite(toolUseID string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	path := p.pendingWrites[toolUseID]
	delete(p.pendingWrites, toolUseID)
	return path
}

func (p *process) forgetPendingWrite(toolUseID string) {
	p.mu.Lock()
	delete(p.pendingWrites, toolUseID)
	p.mu.Unlock()
}

// maybeEmitImage validates path (image extension, exists, under cwd,
// not already emitted) and returns an ImageFinal event when all
// checks pass. mimeOverride is used when the caller already knows
// the type (e.g. derived from a base64 tool_result block).
func (p *process) maybeEmitImage(path, mimeOverride string) (runtime.ImageFinal, bool) {
	abs := expandPath(path)
	if !filepath.IsAbs(abs) {
		// Resolve relative to cwd. Bare "foo.png" is rare in real
		// model output; we still accept it because Write+Edit can
		// emit relative paths.
		if p.cwd == "" {
			return runtime.ImageFinal{}, false
		}
		abs = filepath.Join(p.cwd, abs)
	}
	abs = filepath.Clean(abs)
	mime, ok := imageExt[strings.ToLower(filepath.Ext(abs))]
	if !ok {
		return runtime.ImageFinal{}, false
	}
	if mimeOverride != "" {
		mime = mimeOverride
	}
	if p.cwd != "" {
		rel, err := filepath.Rel(p.cwd, abs)
		if err != nil || strings.HasPrefix(rel, "..") {
			// Outside cwd → refuse, regardless of file existence.
			return runtime.ImageFinal{}, false
		}
	}
	info, err := os.Stat(abs)
	if err != nil || info.IsDir() || info.Size() == 0 {
		return runtime.ImageFinal{}, false
	}
	p.mu.Lock()
	if p.emittedPaths[abs] {
		p.mu.Unlock()
		return runtime.ImageFinal{}, false
	}
	p.emittedPaths[abs] = true
	p.mu.Unlock()
	return runtime.ImageFinal{Path: abs, MimeType: mime}, true
}

// expandPath rewrites a leading "~" / "~/" to the user's home dir.
// Unknown forms (~user) are left untouched.
func expandPath(p string) string {
	if !strings.HasPrefix(p, "~") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return p
	}
	if p == "~" {
		return home
	}
	if strings.HasPrefix(p, "~/") {
		return filepath.Join(home, p[2:])
	}
	return p
}

// extractToolResultImages pulls inline image blocks out of a
// tool_result content payload. Two shapes are common:
//   - the whole content is a JSON-string (Bash stdout) → no images
//   - the content is an array of {type:"text"|"image",...} blocks →
//     decode base64 image blocks to a temp file and emit ImageFinal.
//
// We write decoded bytes to os.TempDir() (not cwd) because tool-
// returned screenshots aren't workspace artefacts. The bridge takes
// it from there.
func (p *process) extractToolResultImages(content json.RawMessage) []runtime.Event {
	if len(content) == 0 || content[0] != '[' {
		return nil
	}
	var blocks []struct {
		Type   string `json:"type"`
		Source struct {
			Type      string `json:"type"`
			MediaType string `json:"media_type"`
			Data      string `json:"data"`
		} `json:"source"`
	}
	if err := json.Unmarshal(content, &blocks); err != nil {
		return nil
	}
	var out []runtime.Event
	for _, b := range blocks {
		if b.Type != "image" || b.Source.Type != "base64" || b.Source.Data == "" {
			continue
		}
		mime := b.Source.MediaType
		ext := extFromMime(mime)
		if ext == "" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(b.Source.Data)
		if err != nil || len(data) == 0 {
			continue
		}
		f, err := os.CreateTemp("", "mosaic-img-*"+ext)
		if err != nil {
			continue
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			continue
		}
		_ = f.Close()
		p.mu.Lock()
		if p.emittedPaths[f.Name()] {
			p.mu.Unlock()
			_ = os.Remove(f.Name())
			continue
		}
		p.emittedPaths[f.Name()] = true
		p.mu.Unlock()
		out = append(out, runtime.ImageFinal{Path: f.Name(), MimeType: mime})
	}
	return out
}

// extFromMime mirrors matrix/client.go but is local to keep
// dependencies one-way (driver shouldn't import matrix).
func extFromMime(mime string) string {
	switch mime {
	case "image/png":
		return ".png"
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "image/svg+xml":
		return ".svg"
	}
	return ""
}
