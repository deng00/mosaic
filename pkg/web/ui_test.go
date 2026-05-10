package web

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestBoardServesHTMLWithToken(t *testing.T) {
	s, hs := newTestServer(t)
	tok := s.Token()
	resp, err := http.Get(hs.URL + "/ui/board?token=" + tok)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Mosaic") || !strings.Contains(string(body), "kanban") && !strings.Contains(string(body), "Tasks") {
		t.Errorf("unexpected body, first 200B: %q", string(body[:min(200, len(body))]))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("content-type: %q", ct)
	}
}

func TestBoardRejectsBadToken(t *testing.T) {
	_, hs := newTestServer(t)
	resp, err := http.Get(hs.URL + "/ui/board?token=wrong")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
