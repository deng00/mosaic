package workspace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateMakesDirAndRunsAfterCreate(t *testing.T) {
	root := t.TempDir()
	m := NewManager(root, Hooks{
		AfterCreate: `echo hello > marker.txt`,
		Timeout:     5 * time.Second,
	})
	ws, err := m.Create(context.Background(), "MOS-1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !ws.CreatedNow {
		t.Errorf("expected CreatedNow=true on first create")
	}
	if _, err := os.Stat(filepath.Join(ws.Path, "marker.txt")); err != nil {
		t.Errorf("after_create did not run: %v", err)
	}
	// Second call should not re-run after_create.
	ws2, err := m.Create(context.Background(), "MOS-1")
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if ws2.CreatedNow {
		t.Errorf("expected CreatedNow=false on existing ws")
	}
}

func TestAfterCreateFailureSurfaces(t *testing.T) {
	root := t.TempDir()
	m := NewManager(root, Hooks{
		AfterCreate: `exit 1`,
		Timeout:     5 * time.Second,
	})
	_, err := m.Create(context.Background(), "MOS-2")
	if err == nil {
		t.Errorf("expected after_create failure to surface")
	}
}

func TestRemoveRunsBeforeRemoveAndDeletes(t *testing.T) {
	root := t.TempDir()
	probePath := filepath.Join(root, "removed-marker")
	m := NewManager(root, Hooks{
		BeforeRemove: `touch ` + probePath,
		Timeout:      5 * time.Second,
	})
	_, err := m.Create(context.Background(), "MOS-3")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.Remove(context.Background(), "MOS-3"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := os.Stat(probePath); err != nil {
		t.Errorf("before_remove hook did not run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "MOS-3")); !os.IsNotExist(err) {
		t.Errorf("workspace dir not removed: err=%v", err)
	}
}

func TestSanitizeStripsBadChars(t *testing.T) {
	got := SanitizeID("../escape/MOS-1?x")
	for _, ch := range got {
		switch ch {
		case '.', '_', '-':
		default:
			if (ch < 'A' || ch > 'Z') && (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') {
				t.Errorf("unsanitized char %q in %q", ch, got)
			}
		}
	}
}
