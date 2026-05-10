package web

import (
	_ "embed"
	"net/http"
)

//go:embed assets/board.html
var boardHTML []byte

// serveBoard returns the kanban single-page app. The token is allowed
// in ?token= so a user can paste a one-shot URL into Element's widget
// dialog without setting cookies.
func (s *Server) serveBoard(w http.ResponseWriter, r *http.Request) {
	tok := r.URL.Query().Get("token")
	if !constantTimeEqual(tok, s.token) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("invalid or missing ?token= — see ~/.mosaic/data/web.token\n"))
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Frame Element / Matrix widgets from any origin. The token in the
	// URL is the auth boundary; framing protection isn't useful here.
	w.Header().Set("Content-Security-Policy", "frame-ancestors *;")
	_, _ = w.Write(boardHTML)
}
