package task

import (
	"path/filepath"
	"sync"
	"testing"
)

const space = "!HubAKxod:localhost"
const prefix = "MOS"

func newTestStore(t *testing.T) *Store {
	t.Helper()
	root := filepath.Join(t.TempDir(), "projects")
	return NewStore(root)
}

func TestCreateAssignsSequentialIDs(t *testing.T) {
	s := newTestStore(t)
	t1, err := s.Create(space, prefix, CreateInput{Title: "first"})
	if err != nil {
		t.Fatalf("create 1: %v", err)
	}
	if t1.ID != "MOS-1" {
		t.Errorf("want MOS-1, got %s", t1.ID)
	}
	if t1.State != StateBacklog {
		t.Errorf("default state should be backlog, got %s", t1.State)
	}
	t2, err := s.Create(space, prefix, CreateInput{Title: "second"})
	if err != nil {
		t.Fatalf("create 2: %v", err)
	}
	if t2.ID != "MOS-2" {
		t.Errorf("want MOS-2, got %s", t2.ID)
	}
}

func TestCreateRequiresPrefix(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Create(space, "", CreateInput{Title: "x"})
	if err != ErrPrefixRequired {
		t.Errorf("want ErrPrefixRequired, got %v", err)
	}
}

func TestCreateRequiresTitle(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Create(space, prefix, CreateInput{})
	if err == nil {
		t.Errorf("expected error for missing title")
	}
}

func TestUpdateAppliesSparsePatch(t *testing.T) {
	s := newTestStore(t)
	tk, _ := s.Create(space, prefix, CreateInput{Title: "x"})
	newTitle := "y"
	newState := StateInProgress
	out, err := s.Update(space, tk.ID, UpdateInput{Title: &newTitle, State: &newState})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if out.Title != "y" {
		t.Errorf("title not patched: %s", out.Title)
	}
	if out.State != StateInProgress {
		t.Errorf("state not patched: %s", out.State)
	}
	if !out.UpdatedAt.After(tk.UpdatedAt) {
		t.Errorf("UpdatedAt should advance")
	}
}

func TestUpdateRejectsInvalidState(t *testing.T) {
	s := newTestStore(t)
	tk, _ := s.Create(space, prefix, CreateInput{Title: "x"})
	bad := State("nope")
	_, err := s.Update(space, tk.ID, UpdateInput{State: &bad})
	if err == nil {
		t.Errorf("expected error for invalid state")
	}
}

func TestDeleteSoftMarksCancelled(t *testing.T) {
	s := newTestStore(t)
	tk, _ := s.Create(space, prefix, CreateInput{Title: "x"})
	if err := s.Delete(space, tk.ID, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := s.Get(space, tk.ID)
	if err != nil {
		t.Fatalf("get after soft-delete: %v", err)
	}
	if got.State != StateCancelled {
		t.Errorf("want cancelled, got %s", got.State)
	}
}

func TestDeletePurgeRemovesRow(t *testing.T) {
	s := newTestStore(t)
	tk, _ := s.Create(space, prefix, CreateInput{Title: "x"})
	if err := s.Delete(space, tk.ID, true); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, err := s.Get(space, tk.ID); err != ErrNotFound {
		t.Errorf("want ErrNotFound, got %v", err)
	}
}

func TestPersistenceSurvivesNewStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "projects")
	s1 := NewStore(root)
	if _, err := s1.Create(space, prefix, CreateInput{Title: "persist"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	s2 := NewStore(root)
	got, err := s2.List(space)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 || got[0].Title != "persist" {
		t.Errorf("did not persist: %+v", got)
	}
	// Next id should continue from 2.
	t2, _ := s2.Create(space, prefix, CreateInput{Title: "after-restart"})
	if t2.ID != "MOS-2" {
		t.Errorf("want MOS-2 after restart, got %s", t2.ID)
	}
}

func TestHookFiresOnCreateUpdateDelete(t *testing.T) {
	s := newTestStore(t)
	var got []string
	s.OnChange(func(spaceID string, before, after *Task) {
		switch {
		case before == nil && after != nil:
			got = append(got, "create:"+after.ID)
		case before != nil && after != nil:
			got = append(got, "update:"+after.ID)
		case before != nil && after == nil:
			got = append(got, "purge:"+before.ID)
		}
	})
	tk, _ := s.Create(space, prefix, CreateInput{Title: "x"})
	st := StateTodo
	_, _ = s.Update(space, tk.ID, UpdateInput{State: &st})
	_ = s.Delete(space, tk.ID, true)
	want := []string{"create:MOS-1", "update:MOS-1", "purge:MOS-1"}
	if len(got) != len(want) {
		t.Fatalf("hook calls: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("hook[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestConcurrentCreateProducesUniqueIDs(t *testing.T) {
	s := newTestStore(t)
	const N = 20
	var wg sync.WaitGroup
	ids := make(chan string, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk, err := s.Create(space, prefix, CreateInput{Title: "x"})
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ids <- tk.ID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate id: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != N {
		t.Errorf("want %d unique ids, got %d", N, len(seen))
	}
}

func TestDifferentProjectsIndependent(t *testing.T) {
	s := newTestStore(t)
	const otherSpace = "!Other:localhost"
	a, _ := s.Create(space, "MOS", CreateInput{Title: "a"})
	b, _ := s.Create(otherSpace, "OTH", CreateInput{Title: "b"})
	if a.ID != "MOS-1" || b.ID != "OTH-1" {
		t.Errorf("ids should be per-project, got %s and %s", a.ID, b.ID)
	}
}
