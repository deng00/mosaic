package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ErrNotFound is returned by Get / Update / Delete when no task with
// the given id exists in the project.
var ErrNotFound = errors.New("task not found")

// ErrPrefixRequired indicates the project doesn't have task_prefix
// set in config. Surface this verbatim to API callers so the user
// knows what to fix.
var ErrPrefixRequired = errors.New("task_prefix is required (set projects.<spaceID>.task_prefix in config.yaml)")

// Hook is invoked after a successful mutation (Create / Update /
// Delete). Receives the new state of the task; for Delete with
// purge=true the hook is invoked with the OLD task state and a `purge`
// flag so dispatchers can clean up workspaces.
//
// Hooks run synchronously while the project lock is held — keep them
// short. For long work (spawning agents, git clone), spawn a goroutine.
type Hook func(spaceID string, before, after *Task)

// Store owns per-project tasks.json files. Safe for concurrent use:
// each project has its own mutex; different projects don't block each
// other. The store does NOT cache file contents — every read parses
// from disk. This is fine: tasks.json is small (≪1MB) and reads are
// rare (board UI polls + dispatcher hooks).
type Store struct {
	root string // <data>/projects/

	mu    sync.Mutex
	locks map[string]*sync.Mutex // spaceID → mutex
	hooks []Hook
}

// NewStore creates a store rooted at the shared projects tree (e.g.
// "<data>/projects"). The directory does not need to exist; per-project
// subdirs are created on demand when a task is first written.
func NewStore(root string) *Store {
	return &Store{
		root:  root,
		locks: map[string]*sync.Mutex{},
	}
}

// OnChange registers a hook fired after every successful mutation.
// Hooks are invoked in registration order while the per-project lock
// is held — long work belongs in a goroutine inside the hook.
func (s *Store) OnChange(h Hook) {
	s.mu.Lock()
	s.hooks = append(s.hooks, h)
	s.mu.Unlock()
}

func (s *Store) lockFor(spaceID string) *sync.Mutex {
	s.mu.Lock()
	m, ok := s.locks[spaceID]
	if !ok {
		m = &sync.Mutex{}
		s.locks[spaceID] = m
	}
	s.mu.Unlock()
	return m
}

func (s *Store) path(spaceID string) string {
	return filepath.Join(s.root, safeID(spaceID), "tasks.json")
}

// safeID escapes Matrix IDs (which may contain ":") for filesystem use.
// Matches the convention used by pkg/agent/memory.go.
func safeID(s string) string {
	out := make([]byte, 0, len(s))
	for _, r := range s {
		if r == ':' {
			out = append(out, '_')
		} else {
			out = append(out, byte(r))
		}
	}
	return string(out)
}

// loadLocked parses tasks.json. Missing file returns an empty File
// (NextID=1). Caller must hold the project lock.
func (s *Store) loadLocked(spaceID string) (*File, error) {
	path := s.path(spaceID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &File{NextID: 1}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if f.NextID < 1 {
		f.NextID = 1
	}
	return &f, nil
}

// saveLocked writes the file atomically. Caller must hold the project
// lock.
func (s *Store) saveLocked(spaceID string, f *File) error {
	path := s.path(spaceID)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// List returns all tasks for a project, sorted by created_at ascending
// (older first). Empty file → empty slice (no error).
func (s *Store) List(spaceID string) ([]Task, error) {
	mu := s.lockFor(spaceID)
	mu.Lock()
	defer mu.Unlock()
	f, err := s.loadLocked(spaceID)
	if err != nil {
		return nil, err
	}
	out := append([]Task(nil), f.Tasks...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

// Get returns a single task by id. ErrNotFound when absent.
func (s *Store) Get(spaceID, id string) (Task, error) {
	mu := s.lockFor(spaceID)
	mu.Lock()
	defer mu.Unlock()
	f, err := s.loadLocked(spaceID)
	if err != nil {
		return Task{}, err
	}
	for _, t := range f.Tasks {
		if t.ID == id {
			return t, nil
		}
	}
	return Task{}, ErrNotFound
}

// CreateInput is the payload accepted by Create. State defaults to
// StateBacklog when empty.
type CreateInput struct {
	Title       string
	Description string
	State       State
	Assignee    string
	Labels      []string
	CreatedBy   string
}

// Create allocates a new id from NextID, persists, fires hooks.
// prefix must be non-empty; the resulting id is "<PREFIX>-<n>".
func (s *Store) Create(spaceID, prefix string, in CreateInput) (Task, error) {
	if prefix == "" {
		return Task{}, ErrPrefixRequired
	}
	if in.Title == "" {
		return Task{}, fmt.Errorf("title is required")
	}
	st := in.State
	if st == "" {
		st = StateBacklog
	}
	if !st.Valid() {
		return Task{}, fmt.Errorf("invalid state %q", in.State)
	}

	mu := s.lockFor(spaceID)
	mu.Lock()
	defer mu.Unlock()

	f, err := s.loadLocked(spaceID)
	if err != nil {
		return Task{}, err
	}
	id, err := MakeID(prefix, f.NextID)
	if err != nil {
		return Task{}, err
	}
	now := time.Now().UTC()
	t := Task{
		ID:          id,
		Title:       in.Title,
		Description: in.Description,
		State:       st,
		Assignee:    in.Assignee,
		Labels:      in.Labels,
		CreatedBy:   in.CreatedBy,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	f.Tasks = append(f.Tasks, t)
	f.NextID++
	if err := s.saveLocked(spaceID, f); err != nil {
		return Task{}, err
	}
	s.fire(spaceID, nil, &t)
	return t, nil
}

// UpdateInput is a sparse patch — only non-nil fields are applied.
// State changes through this path so hooks see the transition.
type UpdateInput struct {
	Title         *string
	Description   *string
	State         *State
	Assignee      *string
	Labels        *[]string
	TopicRoom     *string
	WorkspacePath *string
	Branch        *string
}

// Update applies a sparse patch by id. ErrNotFound when absent.
func (s *Store) Update(spaceID, id string, in UpdateInput) (Task, error) {
	mu := s.lockFor(spaceID)
	mu.Lock()
	defer mu.Unlock()

	f, err := s.loadLocked(spaceID)
	if err != nil {
		return Task{}, err
	}
	idx := -1
	for i, t := range f.Tasks {
		if t.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return Task{}, ErrNotFound
	}
	before := f.Tasks[idx]
	t := before

	if in.Title != nil {
		t.Title = *in.Title
	}
	if in.Description != nil {
		t.Description = *in.Description
	}
	if in.State != nil {
		if !in.State.Valid() {
			return Task{}, fmt.Errorf("invalid state %q", *in.State)
		}
		t.State = *in.State
	}
	if in.Assignee != nil {
		t.Assignee = *in.Assignee
	}
	if in.Labels != nil {
		t.Labels = *in.Labels
	}
	if in.TopicRoom != nil {
		t.TopicRoom = *in.TopicRoom
	}
	if in.WorkspacePath != nil {
		t.WorkspacePath = *in.WorkspacePath
	}
	if in.Branch != nil {
		t.Branch = *in.Branch
	}
	t.UpdatedAt = time.Now().UTC()
	f.Tasks[idx] = t
	if err := s.saveLocked(spaceID, f); err != nil {
		return Task{}, err
	}
	s.fire(spaceID, &before, &t)
	return t, nil
}

// Delete soft-deletes (state=cancelled) by default. When purge=true
// the row is removed from the file entirely.
func (s *Store) Delete(spaceID, id string, purge bool) error {
	mu := s.lockFor(spaceID)
	mu.Lock()
	defer mu.Unlock()

	f, err := s.loadLocked(spaceID)
	if err != nil {
		return err
	}
	idx := -1
	for i, t := range f.Tasks {
		if t.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ErrNotFound
	}
	before := f.Tasks[idx]
	if purge {
		f.Tasks = append(f.Tasks[:idx], f.Tasks[idx+1:]...)
		if err := s.saveLocked(spaceID, f); err != nil {
			return err
		}
		s.fire(spaceID, &before, nil)
		return nil
	}
	t := before
	t.State = StateCancelled
	t.UpdatedAt = time.Now().UTC()
	f.Tasks[idx] = t
	if err := s.saveLocked(spaceID, f); err != nil {
		return err
	}
	s.fire(spaceID, &before, &t)
	return nil
}

func (s *Store) fire(spaceID string, before, after *Task) {
	s.mu.Lock()
	hooks := append([]Hook(nil), s.hooks...)
	s.mu.Unlock()
	for _, h := range hooks {
		h(spaceID, before, after)
	}
}
