package workflow

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestParseFrontMatterAndBody(t *testing.T) {
	src := []byte(`---
branch_template: "feat/{{.ID}}-x"
---
hello {{.Title}}`)
	f, err := Parse("WORKFLOW.md", src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if f.Front.BranchTemplate != "feat/{{.ID}}-x" {
		t.Errorf("front: %q", f.Front.BranchTemplate)
	}
	if !strings.HasPrefix(f.Body, "hello") {
		t.Errorf("body: %q", f.Body)
	}
}

func TestParseNoFrontMatterAllBody(t *testing.T) {
	src := []byte("Just body.\nNo front.")
	f, err := Parse("X", src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if f.Front.BranchTemplate != "" {
		t.Errorf("expected empty front")
	}
	if !strings.Contains(f.Body, "Just body") {
		t.Errorf("body lost: %q", f.Body)
	}
}

func TestRenderWithVars(t *testing.T) {
	src := []byte(`---
---
Task {{.ID}} — {{.Title}}. Branch={{.Branch}}.`)
	f, _ := Parse("X", src)
	out, err := f.Render(Vars{ID: "MOS-1", Title: "fix it", Branch: "task/MOS-1"})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := "Task MOS-1 — fix it. Branch=task/MOS-1."
	if !strings.Contains(out, want) {
		t.Errorf("rendered: %q (want substring %q)", out, want)
	}
}

func TestRenderBranchDefault(t *testing.T) {
	f, _ := Parse("X", []byte("body"))
	got, err := f.RenderBranch(Vars{ID: "MOS-9"})
	if err != nil {
		t.Fatalf("render branch: %v", err)
	}
	if got != "task/MOS-9" {
		t.Errorf("branch: %q", got)
	}
}

func TestRenderBranchCustom(t *testing.T) {
	f, _ := Parse("X", []byte(`---
branch_template: "{{.ID}}-thing"
---
body`))
	got, _ := f.RenderBranch(Vars{ID: "MOS-9"})
	if got != "MOS-9-thing" {
		t.Errorf("branch: %q", got)
	}
}

func TestLoadMissingReturnsNil(t *testing.T) {
	f, err := Load(filepath.Join(t.TempDir(), "nope.md"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if f != nil {
		t.Errorf("missing file should return nil, got %+v", f)
	}
}

func TestDefaultTemplateRenders(t *testing.T) {
	f, _ := Parse("X", []byte(DefaultTemplate))
	out, err := f.Render(Vars{
		ID: "MOS-1", Title: "do thing", Description: "the desc",
		Labels: []string{"feat", "p1"},
		Branch: "task/MOS-1", WorkspaceDir: "/tmp/ws",
	})
	if err != nil {
		t.Fatalf("render default: %v", err)
	}
	for _, want := range []string{"MOS-1", "do thing", "the desc", "feat, p1", "task/MOS-1", "/tmp/ws"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
