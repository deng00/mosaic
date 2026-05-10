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
func FormatToolUse(name string, input json.RawMessage) string {
	switch name {
	case "Bash":
		var v struct {
			Command     string `json:"command"`
			Description string `json:"description"`
		}
		_ = json.Unmarshal(input, &v)
		body := "🛠️ **Bash** `" + truncate(oneLine(v.Command), 200) + "`"
		if v.Description != "" {
			body += "  _" + truncate(v.Description, 80) + "_"
		}
		return body

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
		return "📖 **Read** `" + relPath(v.FilePath) + "`" + extra

	case "Write":
		var v struct {
			FilePath string `json:"file_path"`
			Content  string `json:"content"`
		}
		_ = json.Unmarshal(input, &v)
		return fmt.Sprintf("📝 **Write** `%s` (%d B)", relPath(v.FilePath), len(v.Content))

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
		return fmt.Sprintf("✏️ **Edit** `%s`%s\n```diff\n- %s\n+ %s\n```",
			relPath(v.FilePath), marker,
			truncate(oneLine(v.OldString), 100),
			truncate(oneLine(v.NewString), 100))

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
		return fmt.Sprintf("🔎 **Grep** `%s`%s", truncate(v.Pattern, 80), where)

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
		return "🔭 **Glob** `" + v.Pattern + "`" + where

	case "Agent":
		var v struct {
			Description  string `json:"description"`
			SubagentType string `json:"subagent_type"`
		}
		_ = json.Unmarshal(input, &v)
		return fmt.Sprintf("🤖 **Agent** _%s_ — %s",
			defaultStr(v.SubagentType, "general"),
			truncate(v.Description, 120))

	case "TodoWrite":
		// Too noisy to surface — claude uses it for internal book-keeping.
		var v struct {
			Todos []struct {
				Content string `json:"content"`
				Status  string `json:"status"`
			} `json:"todos"`
		}
		_ = json.Unmarshal(input, &v)
		if len(v.Todos) == 0 {
			return "📋 **TodoWrite** _(empty)_"
		}
		return fmt.Sprintf("📋 **TodoWrite** (%d items)", len(v.Todos))

	case "WebFetch":
		var v struct {
			URL    string `json:"url"`
			Prompt string `json:"prompt"`
		}
		_ = json.Unmarshal(input, &v)
		return "🌐 **WebFetch** " + v.URL + "  _" + truncate(v.Prompt, 80) + "_"

	case "WebSearch":
		var v struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(input, &v)
		return "🔍 **WebSearch** `" + truncate(v.Query, 120) + "`"

	default:
		// Unknown tool: show name and a tiny preview of input.
		preview := truncate(oneLine(string(input)), 100)
		return fmt.Sprintf("🔧 **%s** `%s`", name, preview)
	}
}

// FormatToolResult turns the tool_result content into a brief
// confirmation line. Most are not worth surfacing — return "" to
// suppress. We do surface errors so the user sees failures.
func FormatToolResult(toolName string, content json.RawMessage, isError bool) string {
	if !isError {
		return "" // success is implicit; don't spam
	}
	out := stringifyToolResult(content)
	if out == "" {
		return "⚠️ tool error"
	}
	return "⚠️ **" + toolName + " error**\n```\n" + truncate(out, 800) + "\n```"
}

func stringifyToolResult(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err == nil {
		var sb strings.Builder
		for _, b := range blocks {
			if b.Type == "text" {
				sb.WriteString(b.Text)
			}
		}
		return sb.String()
	}
	return string(raw)
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
