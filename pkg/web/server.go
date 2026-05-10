// Package web exposes Mosaic's task board over HTTP: a small REST API
// (JSON) plus a single-page HTML board served from embedded assets.
//
// Designed to be Element-widget-friendly: a user can drop the
// /ui/board?space=...&token=... URL into a Matrix Space's widget and
// see the kanban inline. The token is generated on first startup and
// persisted at <data>/web.token; bearer auth on /api/, query auth on
// /ui/.
//
// Opt-in. main.go starts this only when config has web.enabled = true.
package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/deng00/mosaic/pkg/task"
)

// Project is the snapshot a UI needs to render the project picker.
type Project struct {
	SpaceID string `json:"space_id"`
	Name    string `json:"name"`
	Prefix  string `json:"prefix"`
}

// Agent is the snapshot used to populate the assignee dropdown.
type Agent struct {
	ID          string `json:"id"`
	UserID      string `json:"user_id"`
	DisplayName string `json:"display_name"`
	Online      bool   `json:"online"`
}

// Provider hands the web server live snapshots of project + agent
// state. main wires its AgentRuntime here. Methods are called per-
// request; implementations should return cheap copies.
type Provider interface {
	Projects() []Project
	Agents() []Agent
}

// Options configures Server.
type Options struct {
	// Bind is the listen address ("127.0.0.1" recommended). Empty
	// defaults to 127.0.0.1.
	Bind string
	// Port to listen on. 0 picks a free port; the actual port is
	// available via Server.Addr() after Start.
	Port int
	// TokenPath is where the bearer token is loaded/saved (e.g.
	// "<data>/web.token"). Required.
	TokenPath string
	// Store backs the REST API.
	Store *task.Store
	// Provider supplies project + agent snapshots.
	Provider Provider
}

// Server is one HTTP listener serving /api/v1 + /ui/board.
type Server struct {
	opts  Options
	token string
	srv   *http.Server
	addr  string
}

// New constructs the server, loading or generating the bearer token.
// Doesn't bind a port — call Start.
func New(opts Options) (*Server, error) {
	if opts.TokenPath == "" {
		return nil, fmt.Errorf("web: TokenPath required")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("web: Store required")
	}
	if opts.Provider == nil {
		return nil, fmt.Errorf("web: Provider required")
	}
	if opts.Bind == "" {
		opts.Bind = "127.0.0.1"
	}
	tok, err := loadOrCreateToken(opts.TokenPath)
	if err != nil {
		return nil, fmt.Errorf("web: token: %w", err)
	}
	s := &Server{opts: opts, token: tok}
	return s, nil
}

// Token returns the bearer token (also persisted at TokenPath).
func (s *Server) Token() string { return s.token }

// Addr returns the listen address (host:port). Empty until Start.
func (s *Server) Addr() string { return s.addr }

// BoardURL returns a ready-to-paste Element-widget URL for the given
// Space, including the auth token. Public so main.go can log it on
// startup.
func (s *Server) BoardURL(spaceID string) string {
	return fmt.Sprintf("http://%s/ui/board?space=%s&token=%s",
		s.addr, spaceID, s.token)
}

// Start binds the configured port and runs the server in a background
// goroutine. Returns once bound (so Addr() is meaningful) or fails
// fast on bind error.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	s.routes(mux)

	addr := fmt.Sprintf("%s:%d", s.opts.Bind, s.opts.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("web: listen %s: %w", addr, err)
	}
	s.addr = ln.Addr().String()
	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := s.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[web] serve: %v", err)
		}
	}()
	return nil
}

// Stop gracefully shuts down. Safe to call multiple times.
func (s *Server) Stop() {
	if s.srv == nil {
		return
	}
	_ = s.srv.Close()
}

// loadOrCreateToken reads an existing token file or generates a new
// 32-byte hex token. Mode 0o600. Stable across restarts so a widget
// URL the user pasted into Element keeps working.
func loadOrCreateToken(path string) (string, error) {
	if data, err := os.ReadFile(path); err == nil {
		t := strings.TrimSpace(string(data))
		if len(t) >= 16 {
			return t, nil
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	tok := hex.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(tok+"\n"), 0o600); err != nil {
		return "", err
	}
	return tok, nil
}

// constantTimeEqual avoids timing-channel leaks on token compare.
func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
