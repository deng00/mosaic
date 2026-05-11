// Package codex is the OpenAI Codex CLI driver for pkg/runtime.
//
// Architectural differences from claude:
//
//   - Codex's `codex exec --json` is per-turn: the subprocess reads one
//     prompt and exits. There's no long-lived stdin pipe to feed more
//     messages. We model the session as a serial loop in a background
//     goroutine: Send enqueues, the loop spawns one `codex exec` per
//     prompt and pumps its JSONL output as Events.
//
//   - Session continuity uses codex's thread_id: the first turn emits
//     thread.started which we capture; every subsequent turn becomes
//     `codex exec resume <thread> <prompt>`.
//
//   - Codex emits only completed assistant messages (no token deltas).
//     The bridge falls back to one Matrix message per TextFinal.
//
//   - No --append-system-prompt equivalent. The driver prepends the
//     AppendSystemPrompt option to the very first prompt as a tagged
//     system block so memory injection still flows through. Resume
//     turns skip it (codex already has the context server-side).
//
//   - Hard-fails at Spawn if ~/.codex/auth.json is missing, with a
//     "run codex login" hint — same pattern cs-argus-agent uses.
//
// Import for side-effect to register the driver:
//
//	import _ "github.com/deng00/mosaic/pkg/runtime/codex"
package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/deng00/mosaic/pkg/runtime"
)

// ID is the registry key.
const ID = "codex"

func init() { runtime.Register(driver{}) }

type driver struct{}

func (driver) ID() string { return ID }

func (driver) Spawn(ctx context.Context, opts runtime.Options) (runtime.Process, error) {
	binary := opts.Binary
	if binary == "" {
		binary = "codex"
	}
	// Pre-flight: codex's auth lives in ~/.codex/auth.json (one-time
	// browser login). Without it the first turn fails far from the
	// user's view inside the subprocess; surface it at Spawn time so
	// the bridge can show a useful error in chat.
	if home, err := os.UserHomeDir(); err == nil {
		authPath := filepath.Join(home, ".codex", "auth.json")
		if st, err := os.Stat(authPath); err != nil || st.IsDir() {
			return nil, fmt.Errorf("codex auth not configured: %s missing — run `codex login` on the host first", authPath)
		}
	}
	p := &process{
		ctx:                 ctx,
		opts:                opts,
		binary:              binary,
		events:              make(chan runtime.Event, 32),
		inbox:               make(chan runtime.Message, 1),
		done:                make(chan struct{}),
		thread:              opts.Resume,
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

	// thread is the codex thread_id captured from the first
	// thread.started event. Empty until first turn completes; drives
	// the resume / fresh-launch branch in runTurn.
	thread string

	// pendingPrependFirst is AppendSystemPrompt material to inline
	// into the FIRST turn's user prompt (codex has no equivalent
	// flag). Cleared after the first turn runs. Subsequent turns
	// rely on codex's server-side conversation memory.
	pendingPrependFirst string
}

func (p *process) Send(msg runtime.Message) error {
	select {
	case p.inbox <- msg:
		return nil
	case <-p.done:
		return fmt.Errorf("codex: process closed")
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *process) Events() <-chan runtime.Event { return p.events }

func (p *process) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	return nil
}

// loop dispatches one turn at a time. Strictly serial — matches
// the bridge's per-room serial inbox so two turns in the same room
// can't race each other into overlapping codex subprocesses.
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

// runTurn spawns one `codex exec` invocation and pumps its JSONL
// output. Emits exactly one TurnDone at the end regardless of
// success path or subprocess error.
func (p *process) runTurn(msg runtime.Message) {
	prompt := msg.Text
	if p.pendingPrependFirst != "" && p.thread == "" {
		// Inline AppendSystemPrompt into the first prompt as a tagged
		// block so the model sees mosaic's memory layer. Keep the
		// shape similar to claude's --append-system-prompt output.
		prompt = "<mosaic_system_prompt>\n" + p.pendingPrependFirst + "\n</mosaic_system_prompt>\n\n" + msg.Text
		p.pendingPrependFirst = ""
	}

	args := p.buildArgs()
	// Attach each image via codex's `-i <file>` repeatable flag.
	// Codex's exec / exec-resume both expose this; non-image
	// attachments are skipped (codex has no generic-file ingestion).
	for _, a := range msg.Attachments {
		if a.Kind != "image" || a.Path == "" {
			continue
		}
		args = append(args, "-i", a.Path)
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
	// Codex's aggregated_output can carry many KB of shell output; raise
	// the line limit so a noisy `ls -R` doesn't break parsing mid-event.
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] != '{' {
			// Codex occasionally prints non-JSON status lines
			// ("Shell cwd was reset to …"); they're not part of the
			// event stream and not interesting for the bridge.
			continue
		}
		for _, ev := range p.translate(line) {
			p.emit(ev)
		}
	}
	if waitErr := cmd.Wait(); waitErr != nil {
		log.Printf("[codex] turn subprocess exit: %v", waitErr)
		p.emit(runtime.TurnDone{Err: waitErr.Error(), Reason: "error"})
		return
	}
	p.emit(runtime.TurnDone{})
}

// buildArgs assembles the per-turn argv. Branches on fresh vs resume.
func (p *process) buildArgs() []string {
	common := []string{
		"--json",
		"--dangerously-bypass-approvals-and-sandbox",
		"--color", "never",
	}
	if p.opts.Model != "" {
		common = append(common, "--model", p.opts.Model)
	}
	if p.thread == "" {
		// Fresh launch. --skip-git-repo-check lets codex run in non-git
		// cwds (e.g. a freshly-cloned workspace before git config is
		// set up); harmless inside git repos.
		args := []string{"exec", "--skip-git-repo-check"}
		args = append(args, common...)
		return args
	}
	// Resume an existing thread. `codex exec resume <id> [PROMPT]`.
	args := []string{"exec", "resume", p.thread}
	args = append(args, common...)
	return args
}

// translate maps one codex JSONL event to zero or more normalized
// runtime.Events. The mapping is:
//
//	thread.started      → SessionInfo (capture thread_id)
//	item.started        command_execution → ToolUse (Name=Bash)
//	item.completed      agent_message → TextFinal
//	                    command_execution + exit_code!=0 → ToolResult(error)
//	turn.completed      → (no-op; TurnDone emitted in runTurn after Wait)
//
// Unknown types are silently dropped.
func (p *process) translate(line string) []runtime.Event {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(line), &head); err != nil {
		return nil
	}

	switch head.Type {
	case "thread.started":
		var ev struct {
			ThreadID string `json:"thread_id"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil || ev.ThreadID == "" {
			return nil
		}
		// Capture for later resume. Mutate inline; safe because
		// translate is called only from the single loop goroutine.
		p.thread = ev.ThreadID
		return []runtime.Event{runtime.SessionInfo{SessionID: ev.ThreadID}}

	case "item.started":
		var ev struct {
			Item struct {
				ID      string `json:"id"`
				Type    string `json:"type"`
				Command string `json:"command"`
			} `json:"item"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			return nil
		}
		if ev.Item.Type == "command_execution" && ev.Item.Command != "" {
			input, _ := json.Marshal(struct {
				Command string `json:"command"`
			}{Command: ev.Item.Command})
			return []runtime.Event{runtime.ToolUse{
				ID:    ev.Item.ID,
				Name:  "Bash",
				Input: input,
			}}
		}
		return nil

	case "item.completed":
		var ev struct {
			Item struct {
				ID               string `json:"id"`
				Type             string `json:"type"`
				Text             string `json:"text"`
				Command          string `json:"command"`
				AggregatedOutput string `json:"aggregated_output"`
				ExitCode         *int   `json:"exit_code"`
				Status           string `json:"status"`
			} `json:"item"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			return nil
		}
		switch ev.Item.Type {
		case "agent_message":
			return []runtime.Event{runtime.TextFinal{Body: ev.Item.Text}}
		case "command_execution":
			if ev.Item.ExitCode != nil && *ev.Item.ExitCode != 0 {
				content, _ := json.Marshal(ev.Item.AggregatedOutput)
				return []runtime.Event{runtime.ToolResult{
					ToolUseID: ev.Item.ID,
					ToolName:  "Bash",
					Content:   content,
					IsError:   true,
				}}
			}
		}
		return nil
	}
	return nil
}

func (p *process) emit(ev runtime.Event) {
	select {
	case p.events <- ev:
	case <-p.done:
	case <-p.ctx.Done():
	}
}
