package web

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/deng00/mosaic/pkg/task"
)

// routes wires the mux. Go 1.22+ pattern syntax with method + path
// keeps the handler set readable without an external router.
func (s *Server) routes(mux *http.ServeMux) {
	// API
	mux.HandleFunc("GET /api/v1/projects", s.requireToken(s.listProjects))
	mux.HandleFunc("GET /api/v1/projects/{spaceID}/agents", s.requireToken(s.listAgents))
	mux.HandleFunc("GET /api/v1/projects/{spaceID}/tasks", s.requireToken(s.listTasks))
	mux.HandleFunc("POST /api/v1/projects/{spaceID}/tasks", s.requireToken(s.createTask))
	mux.HandleFunc("GET /api/v1/projects/{spaceID}/tasks/{id}", s.requireToken(s.getTask))
	mux.HandleFunc("PATCH /api/v1/projects/{spaceID}/tasks/{id}", s.requireToken(s.updateTask))
	mux.HandleFunc("DELETE /api/v1/projects/{spaceID}/tasks/{id}", s.requireToken(s.deleteTask))

	// UI
	mux.HandleFunc("GET /ui/board", s.serveBoard)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
}

// requireToken validates the bearer token. Accepts either Authorization
// header or ?token=... query (the latter so the board.html that lives
// inside an iframe can pass the token without configuring CORS).
func (s *Server) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := ""
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			got = strings.TrimPrefix(h, "Bearer ")
		} else if q := r.URL.Query().Get("token"); q != "" {
			got = q
		}
		if !constantTimeEqual(got, s.token) {
			writeErr(w, http.StatusUnauthorized, "invalid or missing token")
			return
		}
		next(w, r)
	}
}

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.opts.Provider.Projects())
}

func (s *Server) listAgents(w http.ResponseWriter, r *http.Request) {
	// Same global agent list for every project — the dispatcher
	// (Phase 2) is responsible for filtering to "agents in this Space".
	writeJSON(w, http.StatusOK, s.opts.Provider.Agents())
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	spaceID := r.PathValue("spaceID")
	tasks, err := s.opts.Store.List(spaceID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tasks == nil {
		tasks = []task.Task{}
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	spaceID := r.PathValue("spaceID")
	id := r.PathValue("id")
	t, err := s.opts.Store.Get(spaceID, id)
	if errors.Is(err, task.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// createBody is the POST payload. Mirrors task.CreateInput but with
// JSON tags + the project prefix lookup happens server-side.
type createBody struct {
	Title       string     `json:"title"`
	Description string     `json:"description"`
	State       task.State `json:"state"`
	Assignee    string     `json:"assignee"`
	Labels      []string   `json:"labels"`
	CreatedBy   string     `json:"created_by"`
}

func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	spaceID := r.PathValue("spaceID")
	prefix := s.lookupPrefix(spaceID)
	if prefix == "" {
		writeErr(w, http.StatusBadRequest,
			"project has no task_prefix — set projects."+spaceID+".task_prefix in config.yaml")
		return
	}
	var body createBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	t, err := s.opts.Store.Create(spaceID, prefix, task.CreateInput{
		Title:       body.Title,
		Description: body.Description,
		State:       body.State,
		Assignee:    body.Assignee,
		Labels:      body.Labels,
		CreatedBy:   body.CreatedBy,
	})
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Printf("[web] created task %s in %s", t.ID, spaceID)
	writeJSON(w, http.StatusCreated, t)
}

// updateBody mirrors task.UpdateInput but with JSON tags. Pointer
// fields preserve the "absent vs zero-value" distinction.
type updateBody struct {
	Title         *string     `json:"title,omitempty"`
	Description   *string     `json:"description,omitempty"`
	State         *task.State `json:"state,omitempty"`
	Assignee      *string     `json:"assignee,omitempty"`
	Labels        *[]string   `json:"labels,omitempty"`
	TopicRoom     *string     `json:"topic_room,omitempty"`
	WorkspacePath *string     `json:"workspace_path,omitempty"`
	Branch        *string     `json:"branch,omitempty"`
}

func (s *Server) updateTask(w http.ResponseWriter, r *http.Request) {
	spaceID := r.PathValue("spaceID")
	id := r.PathValue("id")
	var body updateBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	t, err := s.opts.Store.Update(spaceID, id, task.UpdateInput{
		Title:         body.Title,
		Description:   body.Description,
		State:         body.State,
		Assignee:      body.Assignee,
		Labels:        body.Labels,
		TopicRoom:     body.TopicRoom,
		WorkspacePath: body.WorkspacePath,
		Branch:        body.Branch,
	})
	if errors.Is(err, task.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) deleteTask(w http.ResponseWriter, r *http.Request) {
	spaceID := r.PathValue("spaceID")
	id := r.PathValue("id")
	purge := r.URL.Query().Get("purge") == "true"
	if err := s.opts.Store.Delete(spaceID, id, purge); err != nil {
		if errors.Is(err, task.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "task not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// lookupPrefix resolves a Space → task_prefix via the Provider's
// Projects() snapshot.
func (s *Server) lookupPrefix(spaceID string) string {
	for _, p := range s.opts.Provider.Projects() {
		if p.SpaceID == spaceID {
			return p.Prefix
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
