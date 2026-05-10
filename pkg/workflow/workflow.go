// Package workflow loads and renders WORKFLOW.md — a per-project file
// that controls how Mosaic dispatches in-progress tasks to agents.
//
// Format (YAML front-matter + text/template body):
//
//	---
//	branch_template: "task/{{.ID}}"
//	---
//	You are working on task {{.ID}}: {{.Title}}.
//
//	{{.Description}}
//
//	When done:
//	  curl -X PATCH -H "Authorization: Bearer $MOSAIC_TOKEN" \
//	    -d '{"state":"in_review"}' "$MOSAIC_API_URL/...{{.ID}}"
//
// The template variables come from a Task plus a few injected env-vars
// the agent will use to call back. Missing WORKFLOW.md → callers fall
// back to DefaultTemplate.
package workflow

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

// Front is the parsed YAML front-matter. All fields optional.
type Front struct {
	// BranchTemplate overrides the default git branch name. Variables
	// are the same as the body template (e.g. "task/{{.ID}}").
	BranchTemplate string `yaml:"branch_template,omitempty"`
}

// File is one parsed WORKFLOW.md.
type File struct {
	Path  string
	Front Front
	Body  string // raw template text
}

// Vars are the template variables exposed to WORKFLOW.md.
type Vars struct {
	ID           string
	Title        string
	Description  string
	Labels       []string
	Assignee     string // full Matrix user ID
	Branch       string // git branch (rendered from BranchTemplate)
	WorkspaceDir string
	SpaceID      string
	// MosaicAPI / MosaicToken let the agent PATCH the task state when
	// done. Empty when web server is disabled — agent should leave
	// the state to a human.
	MosaicAPI   string
	MosaicToken string
}

// Load parses a WORKFLOW.md from disk. ENOENT returns (nil, nil) so
// callers can fall back to DefaultTemplate without distinguishing
// missing-file from parse-error.
func Load(path string) (*File, error) {
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("workflow: read %s: %w", path, err)
	}
	return Parse(path, contents)
}

// Parse splits a WORKFLOW.md byte stream into front-matter + body.
func Parse(path string, contents []byte) (*File, error) {
	front, body := splitFrontMatter(contents)
	f := &File{Path: path, Body: string(body)}
	if len(front) > 0 {
		if err := yaml.Unmarshal(front, &f.Front); err != nil {
			return nil, fmt.Errorf("workflow: invalid YAML front-matter in %s: %w", path, err)
		}
	}
	return f, nil
}

// Render runs the body template against vars. Returns the rendered
// prompt string the agent should see.
func (f *File) Render(vars Vars) (string, error) {
	tmpl, err := template.New("workflow").Option("missingkey=zero").Parse(f.Body)
	if err != nil {
		return "", fmt.Errorf("workflow: parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("workflow: render: %w", err)
	}
	return buf.String(), nil
}

// RenderBranch resolves a branch name from BranchTemplate (or default).
func (f *File) RenderBranch(vars Vars) (string, error) {
	src := f.Front.BranchTemplate
	if src == "" {
		src = "task/{{.ID}}"
	}
	tmpl, err := template.New("branch").Option("missingkey=zero").Parse(src)
	if err != nil {
		return "", fmt.Errorf("workflow: parse branch template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("workflow: render branch: %w", err)
	}
	return strings.TrimSpace(buf.String()), nil
}

// splitFrontMatter returns (frontBytes, bodyBytes). When the file
// doesn't start with "---\n" the entire content is body.
func splitFrontMatter(b []byte) ([]byte, []byte) {
	r := bufio.NewReader(bytes.NewReader(b))
	first, err := r.ReadString('\n')
	if err != nil && first == "" {
		return nil, b
	}
	if strings.TrimRight(first, "\r\n") != "---" {
		return nil, b
	}
	var fm bytes.Buffer
	for {
		line, err := r.ReadString('\n')
		if strings.TrimRight(line, "\r\n") == "---" {
			body, _ := readAll(r)
			return fm.Bytes(), body
		}
		fm.WriteString(line)
		if err != nil {
			// Unterminated front-matter — treat the whole thing as body
			// rather than rejecting; principle of least surprise.
			return nil, b
		}
	}
}

func readAll(r *bufio.Reader) ([]byte, error) {
	var out bytes.Buffer
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	return out.Bytes(), nil
}

// DefaultTemplate is the prompt used when the project has no
// WORKFLOW.md. Aimed at the common "open a PR with gh" flow; if the
// project doesn't fit, override with a project-specific WORKFLOW.md.
const DefaultTemplate = `You are working on task **{{.ID}}** ({{.Title}}).
{{if .Assignee}}Assigned to {{.Assignee}}.{{end}}

# Task

{{.Description}}
{{if .Labels}}
Labels: {{range $i, $l := .Labels}}{{if $i}}, {{end}}{{$l}}{{end}}{{end}}

# Workspace

You are in a fresh workspace at ` + "`{{.WorkspaceDir}}`" + `. The
project owner has prepared it via the ` + "`workspace.hooks.after_create`" + ` hook
(typically a ` + "`git clone`" + `).

# Workflow

1. Read the relevant code to understand context. If something is genuinely
   ambiguous, stop and explain rather than guessing.
2. Create a branch named ` + "`{{.Branch}}`" + ` (or a sensible derivative if
   it already exists).
3. Implement the change. Keep the diff focused. Run existing tests / linters;
   do not introduce new failures.
4. Commit with a clear message that includes ` + "`{{.ID}}`" + ` in subject or body.
5. Push the branch:
   ` + "```bash" + `
   git push -u origin {{.Branch}}
   ` + "```" + `
6. **Open a pull request** using ` + "`gh pr create`" + `. Include ` + "`{{.ID}}`" + `
   in the title or body. If ` + "`gh`" + ` is missing or unauthenticated, **STOP**
   and report — do not invent another path.
7. Capture the PR URL.
8. **Mark the task ready for review** by PATCHing the Mosaic API:
   ` + "```bash" + `
   curl -sS -X PATCH \
     -H "Authorization: Bearer $MOSAIC_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"state":"in_review"}' \
     "$MOSAIC_API_URL/api/v1/projects/$MOSAIC_SPACE_ID/tasks/$MOSAIC_TASK_ID"
   ` + "```" + `
9. Stop. Do not do further work in this session.

If anything fails, **stop and explain** instead of pushing through.
`
