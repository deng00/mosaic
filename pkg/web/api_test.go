package web

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/deng00/mosaic/pkg/task"
)

type fakeProvider struct {
	projects []Project
	agents   []Agent
}

func (f *fakeProvider) Projects() []Project { return f.projects }
func (f *fakeProvider) Agents() []Agent     { return f.agents }

func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	store := task.NewStore(filepath.Join(dir, "projects"))
	prov := &fakeProvider{
		projects: []Project{
			{SpaceID: "!space:localhost", Name: "Mosaic", Prefix: "MOS"},
		},
		agents: []Agent{
			{ID: "cindy", UserID: "@cindy:localhost", DisplayName: "Cindy", Online: true},
		},
	}
	s, err := New(Options{
		Bind:      "127.0.0.1",
		TokenPath: filepath.Join(dir, "web.token"),
		Store:     store,
		Provider:  prov,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	mux := http.NewServeMux()
	s.routes(mux)
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	return s, hs
}

func req(t *testing.T, hs *httptest.Server, tok, method, path string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	r, err := http.NewRequest(method, hs.URL+path, rdr)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func TestAuthRequired(t *testing.T) {
	_, hs := newTestServer(t)
	code, _ := req(t, hs, "", "GET", "/api/v1/projects", nil)
	if code != http.StatusUnauthorized {
		t.Errorf("want 401, got %d", code)
	}
}

func TestCreateAndListTasks(t *testing.T) {
	s, hs := newTestServer(t)
	tok := s.Token()
	code, body := req(t, hs, tok, "POST", "/api/v1/projects/!space:localhost/tasks", map[string]any{
		"title":       "first task",
		"description": "do the thing",
	})
	if code != http.StatusCreated {
		t.Fatalf("create: got %d body=%s", code, body)
	}
	var created task.Task
	_ = json.Unmarshal(body, &created)
	if created.ID != "MOS-1" {
		t.Errorf("want MOS-1, got %s", created.ID)
	}
	code, body = req(t, hs, tok, "GET", "/api/v1/projects/!space:localhost/tasks", nil)
	if code != http.StatusOK {
		t.Fatalf("list: got %d body=%s", code, body)
	}
	var tasks []task.Task
	_ = json.Unmarshal(body, &tasks)
	if len(tasks) != 1 || tasks[0].ID != "MOS-1" {
		t.Errorf("unexpected list: %+v", tasks)
	}
}

func TestUpdateState(t *testing.T) {
	s, hs := newTestServer(t)
	tok := s.Token()
	_, _ = req(t, hs, tok, "POST", "/api/v1/projects/!space:localhost/tasks", map[string]any{"title": "x"})
	code, body := req(t, hs, tok, "PATCH", "/api/v1/projects/!space:localhost/tasks/MOS-1",
		map[string]any{"state": "in_progress"})
	if code != http.StatusOK {
		t.Fatalf("update: %d %s", code, body)
	}
	var got task.Task
	_ = json.Unmarshal(body, &got)
	if got.State != task.StateInProgress {
		t.Errorf("want in_progress, got %s", got.State)
	}
}

func TestDeleteSoftThenPurge(t *testing.T) {
	s, hs := newTestServer(t)
	tok := s.Token()
	_, _ = req(t, hs, tok, "POST", "/api/v1/projects/!space:localhost/tasks", map[string]any{"title": "x"})
	code, _ := req(t, hs, tok, "DELETE", "/api/v1/projects/!space:localhost/tasks/MOS-1", nil)
	if code != http.StatusNoContent {
		t.Errorf("soft delete: %d", code)
	}
	code, body := req(t, hs, tok, "GET", "/api/v1/projects/!space:localhost/tasks/MOS-1", nil)
	if code != http.StatusOK {
		t.Fatalf("get after soft delete: %d %s", code, body)
	}
	var got task.Task
	_ = json.Unmarshal(body, &got)
	if got.State != task.StateCancelled {
		t.Errorf("soft delete should set state=cancelled, got %s", got.State)
	}
	code, _ = req(t, hs, tok, "DELETE", "/api/v1/projects/!space:localhost/tasks/MOS-1?purge=true", nil)
	if code != http.StatusNoContent {
		t.Errorf("purge: %d", code)
	}
	code, _ = req(t, hs, tok, "GET", "/api/v1/projects/!space:localhost/tasks/MOS-1", nil)
	if code != http.StatusNotFound {
		t.Errorf("after purge expect 404, got %d", code)
	}
}

func TestCreateRequiresKnownProject(t *testing.T) {
	s, hs := newTestServer(t)
	tok := s.Token()
	code, body := req(t, hs, tok, "POST", "/api/v1/projects/!unknown:localhost/tasks",
		map[string]any{"title": "x"})
	if code != http.StatusBadRequest {
		t.Errorf("want 400 for unknown project, got %d body=%s", code, body)
	}
}

func TestListProjectsAndAgents(t *testing.T) {
	s, hs := newTestServer(t)
	tok := s.Token()
	code, body := req(t, hs, tok, "GET", "/api/v1/projects", nil)
	if code != http.StatusOK {
		t.Fatalf("projects: %d %s", code, body)
	}
	var ps []Project
	_ = json.Unmarshal(body, &ps)
	if len(ps) != 1 || ps[0].Prefix != "MOS" {
		t.Errorf("projects: %+v", ps)
	}
	code, body = req(t, hs, tok, "GET", "/api/v1/projects/!space:localhost/agents", nil)
	if code != http.StatusOK {
		t.Fatalf("agents: %d %s", code, body)
	}
	var as []Agent
	_ = json.Unmarshal(body, &as)
	if len(as) != 1 || as[0].UserID != "@cindy:localhost" {
		t.Errorf("agents: %+v", as)
	}
}
