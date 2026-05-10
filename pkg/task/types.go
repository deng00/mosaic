// Package task implements per-Project task tracking: a small kanban-
// style state machine with a JSON file per project as the system of
// record.
//
// Storage layout (see CLAUDE.md "Core data model"):
//
//	data/projects/<spaceID>/tasks.json   ← shared across agents in the project
//
// The file is the single source of truth — there is no in-memory cache
// that lives longer than a single API call. Reads parse from disk every
// time; writes are atomic (tmp+rename) and serialized by a per-project
// mutex so concurrent PATCHes from the board UI don't clobber each
// other.
package task

import (
	"fmt"
	"strings"
	"time"
)

// State is the kanban column a task is in.
type State string

const (
	StateBacklog    State = "backlog"
	StateTodo       State = "todo"
	StateInProgress State = "in_progress"
	StateInReview   State = "in_review"
	StateDone       State = "done"
	StateCancelled  State = "cancelled"
)

// AllStates lists every valid state in the order the board renders.
// Cancelled is intentionally last (and hidden by default in the UI).
var AllStates = []State{
	StateBacklog,
	StateTodo,
	StateInProgress,
	StateInReview,
	StateDone,
	StateCancelled,
}

// Valid reports whether s is a known state value.
func (s State) Valid() bool {
	for _, v := range AllStates {
		if s == v {
			return true
		}
	}
	return false
}

// Task is one card on the board.
type Task struct {
	ID            string    `json:"id"` // e.g. "MOS-4" — per-project prefix + sequence
	Title         string    `json:"title"`
	Description   string    `json:"description,omitempty"` // markdown
	State         State     `json:"state"`
	Assignee      string    `json:"assignee,omitempty"` // full Matrix user ID
	Labels        []string  `json:"labels,omitempty"`
	TopicRoom     string    `json:"topic_room,omitempty"`     // populated when dispatcher creates one
	WorkspacePath string    `json:"workspace_path,omitempty"` // populated when dispatcher checks out
	Branch        string    `json:"branch,omitempty"`         // git branch the agent should push to
	CreatedBy     string    `json:"created_by,omitempty"`     // full Matrix user ID
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// File is the on-disk shape of tasks.json.
type File struct {
	NextID int    `json:"next_id"` // next sequence number to hand out (1-based)
	Tasks  []Task `json:"tasks"`
}

// MakeID returns "<PREFIX>-<n>". Prefix is uppercased; an empty prefix
// is rejected — task_prefix is mandatory in project config.
func MakeID(prefix string, n int) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", fmt.Errorf("task_prefix is required (set it in projects.<spaceID>.task_prefix)")
	}
	return fmt.Sprintf("%s-%d", strings.ToUpper(prefix), n), nil
}
