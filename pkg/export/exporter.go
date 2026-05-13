// Package export dumps decrypted Matrix history to local JSONL files.
//
// Why this lives outside pkg/matrix: it composes several agents (every
// agent has its own crypto store and may hold megolm session keys the
// others don't), walks backward through the Messages endpoint, and
// writes durable on-disk state — none of that belongs in the thin
// matrix.Client wrapper.
//
// Layout produced under OutDir:
//
//	manifest.json                                  global run summary
//	rooms/<sanitized-roomID>/messages.jsonl        one JSON event per line
//	rooms/<sanitized-roomID>/state.json            resume token + counters
//	rooms/<sanitized-roomID>/failed.jsonl          events we couldn't decrypt
//
// Crypto caveat: megolm session keys flow agent-by-agent, so an agent
// that joined a room at t₀ can only decrypt events from t₀ onward.
// To maximise coverage we let every agent that's a member of the room
// take a turn at decrypting whatever the primary agent couldn't —
// they're likely to hold different sessions.
package export

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/deng00/mosaic/pkg/matrix"
)

// Options configures one Exporter run.
type Options struct {
	// OutDir is where exports/manifest.json and rooms/<id>/* land.
	// Created if missing.
	OutDir string

	// Clients maps agent ID → matrix.Client. The exporter picks the
	// first agent that has joined a given room as primary, and falls
	// back to the rest for events the primary can't decrypt.
	Clients map[string]*matrix.Client

	// Concurrency caps how many rooms are exported in parallel.
	// Defaults to 4. Per-room work is always serial.
	Concurrency int

	// PageLimit is the chunk size for /messages calls. Defaults to 200,
	// which is the spec-defined cap on most servers.
	PageLimit int

	// Progress, if non-nil, is invoked after each page flush with the
	// per-room counter. Called from worker goroutines — implementation
	// must be goroutine-safe.
	Progress func(roomID id.RoomID, eventCount int)

	// RoomDone, if non-nil, is invoked once per room when its worker
	// finishes (success or error). Goroutine-safe required.
	RoomDone func(summary RoomSummary)
}

// Summary is the structured result of one Run.
type Summary struct {
	StartedAt    time.Time     `json:"started_at"`
	FinishedAt   time.Time     `json:"finished_at"`
	Duration     time.Duration `json:"duration_ns"`
	TotalRooms   int           `json:"total_rooms"`
	TotalEvents  int           `json:"total_events"`
	TotalFailed  int           `json:"total_failed"`
	Rooms        []RoomSummary `json:"rooms"`
	AgentClients []string      `json:"agent_clients"` // IDs of agents used
}

// RoomSummary is one room's outcome.
type RoomSummary struct {
	RoomID       id.RoomID `json:"room_id"`
	Path         string    `json:"path"` // relative to OutDir
	EventCount   int       `json:"event_count"`
	FailedCount  int       `json:"failed_count"`
	Agents       []string  `json:"agents"`       // every agent that's a member
	PrimaryAgent string    `json:"primary_agent"`
	Error        string    `json:"error,omitempty"`
}

// roomState is the on-disk resume token (rooms/<id>/state.json).
type roomState struct {
	NextToken   string    `json:"next_token"`    // backward pagination cursor; "" = walked to top
	EventCount  int       `json:"event_count"`
	FailedCount int       `json:"failed_count"`
	UpdatedAt   time.Time `json:"updated_at"`
	Completed   bool      `json:"completed"`
}

// Exporter holds one configured run. Not reusable — instantiate per call.
type Exporter struct {
	opts Options
}

// New constructs an Exporter. Validates required Options fields.
func New(opts Options) (*Exporter, error) {
	if opts.OutDir == "" {
		return nil, fmt.Errorf("export: OutDir required")
	}
	if len(opts.Clients) == 0 {
		return nil, fmt.Errorf("export: at least one client required")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.PageLimit <= 0 {
		opts.PageLimit = 200
	}
	return &Exporter{opts: opts}, nil
}

// Run walks every room every supplied agent is a member of and dumps
// the timeline to JSONL. Returns a Summary; partial failures (per-room
// errors) are recorded in Summary.Rooms[i].Error rather than aborting.
//
// Run is safe to invoke repeatedly against the same OutDir — it resumes
// from state.json per room and de-dupes against the existing JSONL.
func (e *Exporter) Run(ctx context.Context) (Summary, error) {
	summary := Summary{StartedAt: time.Now()}
	for id := range e.opts.Clients {
		summary.AgentClients = append(summary.AgentClients, id)
	}

	roomMembers, err := e.gatherRooms(ctx)
	if err != nil {
		return summary, fmt.Errorf("gather rooms: %w", err)
	}
	summary.TotalRooms = len(roomMembers)

	if err := os.MkdirAll(filepath.Join(e.opts.OutDir, "rooms"), 0o700); err != nil {
		return summary, fmt.Errorf("mkdir out: %w", err)
	}

	sem := make(chan struct{}, e.opts.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex // guards summary.Rooms / totals

	for roomID, agents := range roomMembers {
		select {
		case <-ctx.Done():
			return summary, ctx.Err()
		default:
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(rid id.RoomID, agentIDs []string) {
			defer wg.Done()
			defer func() { <-sem }()
			rs := e.exportRoom(ctx, rid, agentIDs)
			mu.Lock()
			summary.Rooms = append(summary.Rooms, rs)
			summary.TotalEvents += rs.EventCount
			summary.TotalFailed += rs.FailedCount
			mu.Unlock()
			if e.opts.RoomDone != nil {
				e.opts.RoomDone(rs)
			}
		}(roomID, agents)
	}
	wg.Wait()

	summary.FinishedAt = time.Now()
	summary.Duration = summary.FinishedAt.Sub(summary.StartedAt)

	if err := e.writeManifest(summary); err != nil {
		return summary, fmt.Errorf("write manifest: %w", err)
	}
	return summary, nil
}

// gatherRooms returns roomID → list of agent IDs that have joined it.
// Agents that fail /joined_rooms (transient network etc) are logged and
// skipped — partial coverage beats refusing to export anything.
func (e *Exporter) gatherRooms(ctx context.Context) (map[id.RoomID][]string, error) {
	out := map[id.RoomID][]string{}
	// Iterate agent IDs in a stable order so the primary picked per
	// room is deterministic across runs (helps reproducibility when a
	// retry resumes from state.json).
	agentIDs := make([]string, 0, len(e.opts.Clients))
	for id := range e.opts.Clients {
		agentIDs = append(agentIDs, id)
	}
	// Lexical sort.
	for i := 1; i < len(agentIDs); i++ {
		for j := i; j > 0 && agentIDs[j-1] > agentIDs[j]; j-- {
			agentIDs[j-1], agentIDs[j] = agentIDs[j], agentIDs[j-1]
		}
	}
	for _, aid := range agentIDs {
		cli := e.opts.Clients[aid]
		rooms, err := cli.JoinedRoomIDs(ctx)
		if err != nil {
			log.Printf("[export] agent %s joined_rooms: %v (skipping)", aid, err)
			continue
		}
		for _, rid := range rooms {
			out[rid] = append(out[rid], aid)
		}
	}
	return out, nil
}

// exportRoom is the per-room worker. Idempotent + resumable.
func (e *Exporter) exportRoom(ctx context.Context, roomID id.RoomID, agentIDs []string) RoomSummary {
	rs := RoomSummary{RoomID: roomID, Agents: agentIDs, PrimaryAgent: agentIDs[0]}
	dir := filepath.Join(e.opts.OutDir, "rooms", sanitizeRoomID(roomID))
	rs.Path = filepath.Join("rooms", sanitizeRoomID(roomID), "messages.jsonl")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		rs.Error = fmt.Sprintf("mkdir: %v", err)
		return rs
	}

	// Load resume state.
	state := loadState(filepath.Join(dir, "state.json"))
	if state.Completed {
		// Already walked to top in a prior run. Nothing to do unless
		// the caller asks for incremental (not implemented yet).
		rs.EventCount = state.EventCount
		rs.FailedCount = state.FailedCount
		return rs
	}

	// Build a seen-set from existing messages.jsonl so resumes don't
	// duplicate. Cheap: streaming scan over the file, only the
	// event_id field needed.
	seen := loadSeen(filepath.Join(dir, "messages.jsonl"))

	msgPath := filepath.Join(dir, "messages.jsonl")
	failPath := filepath.Join(dir, "failed.jsonl")
	msgFile, err := os.OpenFile(msgPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		rs.Error = fmt.Sprintf("open jsonl: %v", err)
		return rs
	}
	defer msgFile.Close()
	// failFile lazily opened on first failure to avoid empty files.
	var failFile *os.File
	defer func() {
		if failFile != nil {
			_ = failFile.Close()
		}
	}()

	primary := e.opts.Clients[agentIDs[0]]
	rs.EventCount = state.EventCount
	rs.FailedCount = state.FailedCount
	from := state.NextToken

	for {
		select {
		case <-ctx.Done():
			rs.Error = ctx.Err().Error()
			_ = writeState(filepath.Join(dir, "state.json"), roomState{
				NextToken: from, EventCount: rs.EventCount, FailedCount: rs.FailedCount,
				UpdatedAt: time.Now(),
			})
			return rs
		default:
		}

		chunk, end, err := primary.RoomMessagesBackward(ctx, roomID, from, e.opts.PageLimit)
		if err != nil {
			rs.Error = fmt.Sprintf("messages page (from=%q): %v", from, err)
			_ = writeState(filepath.Join(dir, "state.json"), roomState{
				NextToken: from, EventCount: rs.EventCount, FailedCount: rs.FailedCount,
				UpdatedAt: time.Now(),
			})
			return rs
		}

		for _, evt := range chunk {
			if seen[evt.ID] {
				continue
			}
			seen[evt.ID] = true

			// Carry RoomID — Messages endpoint sometimes elides it on
			// older Synapse versions, and the decrypter keys sessions
			// by (room_id, session_id).
			if evt.RoomID == "" {
				evt.RoomID = roomID
			}

			decrypted, derr := primary.DecryptIfEncrypted(ctx, evt)
			if derr != nil {
				// Retry with every other agent that's in this room.
				// Each agent has its own crypto store and may hold
				// the session the primary's missing.
				retried := false
				for _, aid := range agentIDs[1:] {
					cli, ok := e.opts.Clients[aid]
					if !ok {
						continue
					}
					if d2, e2 := cli.DecryptIfEncrypted(ctx, evt); e2 == nil {
						decrypted = d2
						derr = nil
						retried = true
						break
					}
				}
				_ = retried // tracked implicitly via derr
			}
			if derr != nil {
				if failFile == nil {
					failFile, err = os.OpenFile(failPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
					if err != nil {
						rs.Error = fmt.Sprintf("open failed.jsonl: %v", err)
						return rs
					}
				}
				writeFailed(failFile, evt, derr)
				rs.FailedCount++
				continue
			}

			writeEvent(msgFile, decrypted, evt.Type == event.EventEncrypted)
			rs.EventCount++
		}

		// Persist state after each page so crash-resume is at most
		// one page off (≤ PageLimit events).
		_ = writeState(filepath.Join(dir, "state.json"), roomState{
			NextToken: end, EventCount: rs.EventCount, FailedCount: rs.FailedCount,
			UpdatedAt: time.Now(),
			Completed: end == "",
		})
		if e.opts.Progress != nil {
			e.opts.Progress(roomID, rs.EventCount)
		}

		if end == "" || end == from {
			break // walked to the start of the room's visible history
		}
		from = end
	}

	return rs
}

func (e *Exporter) writeManifest(s Summary) error {
	path := filepath.Join(e.opts.OutDir, "manifest.json")
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// writeEvent emits one JSONL row. We marshal the whole event.Event
// (so reactions, polls, redactions are all preserved) and tag whether
// it was decrypted from a megolm wrapper — handy for downstream tools
// that want to distinguish "originally plaintext" from "we decrypted".
func writeEvent(w *os.File, evt *event.Event, wasEncrypted bool) {
	// Build a stable row shape: top-level identifying fields plus the
	// full original JSON under "raw". Avoids leaking mautrix internal
	// fields while keeping nothing hidden.
	row := struct {
		EventID    id.EventID    `json:"event_id"`
		RoomID     id.RoomID     `json:"room_id"`
		Sender     id.UserID     `json:"sender"`
		Type       string        `json:"type"`
		TS         int64         `json:"ts"`
		Decrypted  bool          `json:"decrypted,omitempty"`
		Content    json.RawMessage `json:"content"`
	}{
		EventID:   evt.ID,
		RoomID:    evt.RoomID,
		Sender:    evt.Sender,
		Type:      evt.Type.String(),
		TS:        evt.Timestamp,
		Decrypted: wasEncrypted,
		Content:   evt.Content.VeryRaw,
	}
	b, err := json.Marshal(row)
	if err != nil {
		log.Printf("[export] marshal event %s: %v", evt.ID, err)
		return
	}
	b = append(b, '\n')
	if _, err := w.Write(b); err != nil {
		log.Printf("[export] write event %s: %v", evt.ID, err)
	}
}

// writeFailed records an event we couldn't decrypt. Keep the original
// envelope (including the encrypted content) so a future re-run with
// new keys can retry.
func writeFailed(w *os.File, evt *event.Event, reason error) {
	row := struct {
		EventID id.EventID      `json:"event_id"`
		RoomID  id.RoomID       `json:"room_id"`
		Sender  id.UserID       `json:"sender"`
		Type    string          `json:"type"`
		TS      int64           `json:"ts"`
		Reason  string          `json:"reason"`
		Content json.RawMessage `json:"content"`
	}{
		EventID: evt.ID,
		RoomID:  evt.RoomID,
		Sender:  evt.Sender,
		Type:    evt.Type.String(),
		TS:      evt.Timestamp,
		Reason:  reason.Error(),
		Content: evt.Content.VeryRaw,
	}
	b, _ := json.Marshal(row)
	b = append(b, '\n')
	_, _ = w.Write(b)
}

func writeState(path string, s roomState) error {
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func loadState(path string) roomState {
	var s roomState
	data, err := os.ReadFile(path)
	if err != nil {
		return s
	}
	_ = json.Unmarshal(data, &s)
	return s
}

// loadSeen scans an existing JSONL once and returns event_id → true.
// Used so resumed runs don't duplicate rows that were written before
// the previous run crashed. Linear scan; fine for tens of thousands.
func loadSeen(path string) map[id.EventID]bool {
	seen := map[id.EventID]bool{}
	f, err := os.Open(path)
	if err != nil {
		return seen
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for {
		var row struct {
			EventID id.EventID `json:"event_id"`
		}
		if err := dec.Decode(&row); err != nil {
			break // io.EOF or first malformed line ends the scan
		}
		if row.EventID != "" {
			seen[row.EventID] = true
		}
	}
	return seen
}

// sanitizeRoomID makes a roomID safe-and-pretty for a directory name.
// Matrix room IDs look like "!HubAKxod...:localhost"; we drop the
// leading "!" and swap ":" for "_". The full roomID is always present
// in manifest.json so this lossy form is for filesystem ergonomics
// only.
func sanitizeRoomID(rid id.RoomID) string {
	s := string(rid)
	s = strings.TrimPrefix(s, "!")
	s = strings.ReplaceAll(s, ":", "_")
	s = strings.ReplaceAll(s, "/", "_")
	return s
}
