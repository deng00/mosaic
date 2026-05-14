// Package agent — format.go: pretty-printers for stream-json events
// (one matrix message per content block). Avoids dumping raw JSON
// into the chat timeline; selects the few interesting fields per
// tool and elides the rest.
package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FormatToolUse renders one tool_use block as a single short markdown
// line suitable for sending as its own Matrix message. Falls back to
// "🔧 ToolName" if the input doesn't deserialize.
//
// House style: the tool name (bold + emoji) sits on its own line; the
// arguments / payload go on the line below. goldmark has HardWraps
// off, so the break is encoded as two trailing spaces + newline
// (markdown hard break) — see pkg/matrix/client.go:38.
func FormatToolUse(name string, input json.RawMessage) string {
	switch name {
	case "Bash":
		// Tool_use blocks default to m.emote (see Bridge.consume), so
		// Element shows "* <agent> 🛠️ <description>". We surface only
		// the description here — the actual command is internal
		// housekeeping and the user doesn't need to read it. Falls
		// back to "Bash" when the description field is empty (rare;
		// Claude Code normally fills it in).
		var v struct {
			Description string `json:"description"`
		}
		_ = json.Unmarshal(input, &v)
		if v.Description != "" {
			return "🛠️ " + truncate(oneLine(v.Description), 200)
		}
		return "🛠️ Bash"

	case "Read":
		var v struct {
			FilePath string `json:"file_path"`
			Offset   int    `json:"offset"`
			Limit    int    `json:"limit"`
		}
		_ = json.Unmarshal(input, &v)
		extra := ""
		if v.Limit > 0 {
			extra = fmt.Sprintf(" (lines %d–%d)", v.Offset+1, v.Offset+v.Limit)
		}
		return "📖 **Read**  \n`" + relPath(v.FilePath) + "`" + extra

	case "Write":
		var v struct {
			FilePath string `json:"file_path"`
			Content  string `json:"content"`
		}
		_ = json.Unmarshal(input, &v)
		return fmt.Sprintf("📝 **Write**  \n`%s` (%d B)", relPath(v.FilePath), len(v.Content))

	case "Edit":
		var v struct {
			FilePath  string `json:"file_path"`
			OldString string `json:"old_string"`
			NewString string `json:"new_string"`
			ReplaceAll bool  `json:"replace_all"`
		}
		_ = json.Unmarshal(input, &v)
		marker := ""
		if v.ReplaceAll {
			marker = " (replace_all)"
		}
		return fmt.Sprintf("✏️ **Edit**  \n`%s`%s\n```diff\n%s%s```",
			relPath(v.FilePath), marker,
			diffLines("-", v.OldString, 20, 200),
			diffLines("+", v.NewString, 20, 200))

	case "Grep":
		var v struct {
			Pattern    string `json:"pattern"`
			Path       string `json:"path"`
			Glob       string `json:"glob"`
			OutputMode string `json:"output_mode"`
		}
		_ = json.Unmarshal(input, &v)
		where := ""
		if v.Path != "" {
			where = " in `" + relPath(v.Path) + "`"
		}
		if v.Glob != "" {
			where += " glob=`" + v.Glob + "`"
		}
		return fmt.Sprintf("🔎 **Grep**  \n`%s`%s", truncate(v.Pattern, 80), where)

	case "Glob":
		var v struct {
			Pattern string `json:"pattern"`
			Path    string `json:"path"`
		}
		_ = json.Unmarshal(input, &v)
		where := ""
		if v.Path != "" {
			where = " in `" + relPath(v.Path) + "`"
		}
		return "🔭 **Glob**  \n`" + v.Pattern + "`" + where

	case "Agent":
		var v struct {
			Description  string `json:"description"`
			SubagentType string `json:"subagent_type"`
		}
		_ = json.Unmarshal(input, &v)
		return fmt.Sprintf("🤖 **Agent**  \n_%s_ — %s",
			defaultStr(v.SubagentType, "general"),
			truncate(v.Description, 120))

	case "TodoWrite":
		// Render the actual todo list — Element collapsed the older
		// "(N items)" summary into a single line that didn't reveal
		// which todo was in progress, so the user couldn't follow the
		// agent's plan. One markdown bullet per todo, status icon up
		// front so it scans at a glance.
		var v struct {
			Todos []struct {
				Content    string `json:"content"`
				ActiveForm string `json:"activeForm"`
				Status     string `json:"status"`
			} `json:"todos"`
		}
		_ = json.Unmarshal(input, &v)
		if len(v.Todos) == 0 {
			return "📋 **TodoWrite**  \n_(empty)_"
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "📋 **TodoWrite** (%d)\n", len(v.Todos))
		for _, t := range v.Todos {
			icon := "⬜"
			text := t.Content
			switch t.Status {
			case "completed":
				icon = "✅"
			case "in_progress":
				icon = "🔄"
				if t.ActiveForm != "" {
					text = t.ActiveForm
				}
			}
			fmt.Fprintf(&sb, "- %s %s\n", icon, truncate(oneLine(text), 200))
		}
		return strings.TrimRight(sb.String(), "\n")

	case "WebFetch":
		var v struct {
			URL string `json:"url"`
		}
		_ = json.Unmarshal(input, &v)
		return "🌐 **WebFetch**  \n" + v.URL

	case "WebSearch":
		var v struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(input, &v)
		return "🔍 **WebSearch**  \n`" + truncate(v.Query, 120) + "`"

	case "Skill":
		var v struct {
			Skill string `json:"skill"`
		}
		_ = json.Unmarshal(input, &v)
		return "🧩 **Skill**  \n`" + v.Skill + "`"

	case "ToolSearch":
		// Claude Code's deferred-tools mechanism: model calls this to
		// pull a tool's schema into context before invoking it. Query
		// is either `select:Foo,Bar` (exact names) or keywords.
		var v struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(input, &v)
		return "📚 **ToolSearch**  \n`" + truncate(v.Query, 120) + "`"

	case "EnterPlanMode":
		// No input parameters — agent is asking to switch into plan
		// mode before a non-trivial implementation task.
		return "📐 **EnterPlanMode**"

	case "ExitPlanMode":
		// Agent finished its plan and is requesting user approval.
		// AllowedPrompts (optional) lists the categories of actions
		// it'll need permission for.
		var v struct {
			AllowedPrompts []struct {
				Tool   string `json:"tool"`
				Prompt string `json:"prompt"`
			} `json:"allowedPrompts"`
		}
		_ = json.Unmarshal(input, &v)
		if len(v.AllowedPrompts) == 0 {
			return "📐 **ExitPlanMode**"
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "📐 **ExitPlanMode** (%d permission(s))", len(v.AllowedPrompts))
		for _, p := range v.AllowedPrompts {
			fmt.Fprintf(&sb, "\n- `%s` — %s", p.Tool, truncate(oneLine(p.Prompt), 120))
		}
		return sb.String()

	case "Monitor":
		// Background log/event watcher. The description shows up in
		// every emitted notification, so it's the most useful field
		// to surface; command is housekeeping.
		var v struct {
			Description string `json:"description"`
			Command     string `json:"command"`
			Persistent  bool   `json:"persistent"`
		}
		_ = json.Unmarshal(input, &v)
		tail := ""
		if v.Persistent {
			tail = " (persistent)"
		}
		label := v.Description
		if label == "" {
			label = v.Command
		}
		return fmt.Sprintf("👁️ **Monitor**%s  \n%s", tail, truncate(oneLine(label), 200))

	default:
		// Unknown tool: show name and a tiny preview of input.
		preview := truncate(oneLine(string(input)), 100)
		return fmt.Sprintf("🔧 **%s**  \n`%s`", name, preview)
	}
}

// FormatEditBrief renders an Edit tool_use as a single emote-friendly
// line, without the diff payload. Used when ToolsConfig.EditShowCode
// is false — the user has opted out of seeing diffs inline.
func FormatEditBrief(input json.RawMessage) string {
	var v struct {
		FilePath   string `json:"file_path"`
		ReplaceAll bool   `json:"replace_all"`
	}
	_ = json.Unmarshal(input, &v)
	marker := ""
	if v.ReplaceAll {
		marker = " (replace_all)"
	}
	return "✏️ Edit `" + relPath(v.FilePath) + "`" + marker
}

// relPath shortens absolute paths under $HOME to ~/foo style and
// trims the rest. Bare filenames pass through.
func relPath(p string) string {
	if p == "" {
		return ""
	}
	if home, _ := os.UserHomeDir(); home != "" {
		if rel, err := filepath.Rel(home, p); err == nil && !strings.HasPrefix(rel, "..") {
			return "~/" + rel
		}
	}
	return p
}

// diffLines renders s as a block of diff lines, each prefixed with
// "- " or "+ ". Long files are capped at maxLines (extra lines folded
// into a "… N more lines" tail), and any single line longer than
// maxLineLen is truncated with an ellipsis. Trailing empty line is
// dropped so the caller's surrounding fence sits tight.
func diffLines(prefix, s string, maxLines, maxLineLen int) string {
	if s == "" {
		return prefix + "\n"
	}
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	var sb strings.Builder
	n := len(lines)
	shown := n
	if shown > maxLines {
		shown = maxLines
	}
	for i := 0; i < shown; i++ {
		sb.WriteString(prefix)
		sb.WriteByte(' ')
		sb.WriteString(truncate(lines[i], maxLineLen))
		sb.WriteByte('\n')
	}
	if n > shown {
		fmt.Fprintf(&sb, "%s … %d more line(s)\n", prefix, n-shown)
	}
	return sb.String()
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ⏎ ")
	s = strings.ReplaceAll(s, "\t", " ")
	return s
}

func defaultStr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// FoldHomeServer hides the `:<homeServer>` suffix on a Matrix ID
// when it matches our own server — display sweetener so long IDs
// like `!HubAKxodLIciDIDrhF:localhost` show as `!HubAKxodLIciDIDrhF`.
// IDs from other servers (federation) pass through untouched because
// their server part is meaningful.
func FoldHomeServer(id, homeServer string) string {
	if homeServer == "" || id == "" {
		return id
	}
	suffix := ":" + homeServer
	if strings.HasSuffix(id, suffix) {
		return id[:len(id)-len(suffix)]
	}
	return id
}

// ExpandHomeServer is the inverse: when the user types a bare MXID
// without the :server part, append our own home server so downstream
// Matrix API calls have a complete ID.
func ExpandHomeServer(id, homeServer string) string {
	if id == "" || homeServer == "" {
		return id
	}
	if !strings.HasPrefix(id, "!") && !strings.HasPrefix(id, "@") && !strings.HasPrefix(id, "#") {
		return id
	}
	if strings.Contains(id, ":") {
		return id
	}
	return id + ":" + homeServer
}
