package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SessionStore persists the (room → claude session_id) map and the
// per-room "archived" flag to disk so agent restarts retain both
// (resumable claude session memory + soft archive state). File is a
// small JSON object, written atomically via rename so a crash mid-
// write doesn't corrupt prior state.
type SessionStore struct {
	path string

	mu       sync.Mutex
	sessions map[string]string // roomID → claude session_id
	archived map[string]bool   // roomID → true if /archive'd
}

// On disk:
//
//	{
//	  "sessions": {"!room:host": "uuid", ...},
//	  "archived": {"!room:host": true,    ...}
//	}
//
// Older versions wrote just the sessions map at the top level; we
// read both shapes for backward compat.
type sessionStoreFile struct {
	Sessions map[string]string `json:"sessions"`
	Archived map[string]bool   `json:"archived"`
}

func NewSessionStore(path string) (*SessionStore, error) {
	s := &SessionStore{
		path:     path,
		sessions: map[string]string{},
		archived: map[string]bool{},
	}
	if err := s.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}

func (s *SessionStore) Get(roomID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[roomID]
}

func (s *SessionStore) Set(roomID, sessionID string) error {
	s.mu.Lock()
	if s.sessions[roomID] == sessionID {
		s.mu.Unlock()
		return nil
	}
	s.sessions[roomID] = sessionID
	s.mu.Unlock()
	return s.save()
}

// IsArchived reports whether /archive marked this room.
func (s *SessionStore) IsArchived(roomID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.archived[roomID]
}

// SetArchived flips the room's archived state. Persists immediately.
func (s *SessionStore) SetArchived(roomID string, archived bool) error {
	s.mu.Lock()
	if archived {
		s.archived[roomID] = true
	} else {
		delete(s.archived, roomID)
	}
	s.mu.Unlock()
	return s.save()
}

func (s *SessionStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	// Try the new shape first (object with "sessions" / "archived").
	var f sessionStoreFile
	if err := json.Unmarshal(data, &f); err == nil && (f.Sessions != nil || f.Archived != nil) {
		if f.Sessions != nil {
			s.sessions = f.Sessions
		}
		if f.Archived != nil {
			s.archived = f.Archived
		}
		return nil
	}
	// Fallback: the old shape was a flat roomID→sessionID map.
	return json.Unmarshal(data, &s.sessions)
}

func (s *SessionStore) save() error {
	s.mu.Lock()
	snap := sessionStoreFile{
		Sessions: make(map[string]string, len(s.sessions)),
		Archived: make(map[string]bool, len(s.archived)),
	}
	for k, v := range s.sessions {
		snap.Sessions[k] = v
	}
	for k, v := range s.archived {
		snap.Archived[k] = v
	}
	s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("sessionstore: mkdir: %w", err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("sessionstore: marshal: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("sessionstore: write tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("sessionstore: rename: %w", err)
	}
	return nil
}
