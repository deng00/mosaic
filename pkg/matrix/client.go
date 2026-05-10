// Package matrix wraps mautrix-go to give the agent a focused API:
// login (with E2EE via cryptohelper), auto-join on invite, dispatch
// inbound text messages to a handler, and post / edit messages back.
//
// The crypto store lives in a SQLite file (one per bot account) so
// device keys / megolm sessions persist across restarts. Without
// persistence, every restart creates a new device and breaks history
// decryption.
package matrix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer/html"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// markdownRenderer is configured once and reused — goldmark instances
// are safe for concurrent use. Tables come from extension.GFM; we keep
// HardWraps off so newline-only paragraphs read like in source.
var markdownRenderer = goldmark.New(
	goldmark.WithExtensions(
		extension.GFM, // tables, strikethrough, task lists, autolink
	),
	goldmark.WithRendererOptions(
		html.WithUnsafe(), // we trust our own input; let <table> through
	),
)

// renderMarkdown returns (htmlBody, didRender). When the input is
// "obviously plaintext" (no markdown markers), didRender=false and the
// caller should skip setting formatted_body to avoid noisy round-trips.
func renderMarkdown(src string) (string, bool) {
	if !looksMarkdown(src) {
		return "", false
	}
	var buf bytes.Buffer
	if err := markdownRenderer.Convert([]byte(src), &buf); err != nil {
		return "", false
	}
	out := strings.TrimSpace(buf.String())
	if out == "" {
		return "", false
	}
	return out, true
}

func looksMarkdown(s string) bool {
	// Cheap heuristic: any of these characters in any reasonable
	// position triggers a render. Goldmark is fast enough that we
	// could just always render, but the extra HTML body is wasted
	// bytes on the wire when the message is single-line plaintext.
	if strings.ContainsAny(s, "*_`#~|<>") {
		return true
	}
	if strings.Contains(s, "\n- ") || strings.Contains(s, "\n* ") || strings.Contains(s, "\n1. ") {
		return true
	}
	if strings.HasPrefix(s, "- ") || strings.HasPrefix(s, "* ") || strings.HasPrefix(s, "1. ") {
		return true
	}
	if strings.Contains(s, "\n\n") {
		return true
	}
	return false
}

// MessageHandler is invoked for every inbound text message in any room
// the bot is in, except messages sent by the bot itself. The handler
// runs in the sync goroutine; long work should be punted to its own
// goroutine.
type MessageHandler func(ctx context.Context, roomID id.RoomID, eventID id.EventID, sender id.UserID, text string)

// Config bundles everything Login needs.
type Config struct {
	Homeserver string // e.g. http://127.0.0.1:8008
	UserID     string // localpart only (e.g. "claude-coder") OR full "@user:domain"
	Password   string
	DeviceName string // shows up in Element's session list

	// CryptoDB is the SQLite path for the olm/megolm store. One per bot.
	CryptoDB string

	// PickleKey encrypts crypto material at rest in CryptoDB.
	// Use a stable random per deployment; losing it = losing E2E history.
	PickleKey []byte

	// AutoJoinSpaceChildren — when true, install a sync handler that
	// attempts to JoinRoomByID for every new m.space.child state event
	// the bot observes in Spaces it belongs to. See BotConfig docs.
	AutoJoinSpaceChildren bool
}

// Client is the high-level wrapper. Construct via Login.
type Client struct {
	mx     *mautrix.Client
	helper *cryptohelper.CryptoHelper

	autoJoinSpaceChildren bool

	mu      sync.Mutex
	handler MessageHandler
}

// Login logs in (creating a new device if needed), starts the crypto
// machinery, and returns a Client ready to Sync.
func Login(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.PickleKey == nil {
		return nil, fmt.Errorf("matrix: PickleKey must be set")
	}
	if cfg.DeviceName == "" {
		cfg.DeviceName = "mosaic"
	}

	// mautrix-go accepts homeserver URL + empty userID/token; login
	// will fill those in.
	mx, err := mautrix.NewClient(cfg.Homeserver, "", "")
	if err != nil {
		return nil, fmt.Errorf("matrix: new client: %w", err)
	}

	helper, err := cryptohelper.NewCryptoHelper(mx, cfg.PickleKey, cfg.CryptoDB)
	if err != nil {
		return nil, fmt.Errorf("matrix: crypto helper: %w", err)
	}

	user := normalizeUserID(cfg.UserID, cfg.Homeserver)
	helper.LoginAs = &mautrix.ReqLogin{
		Type:                     mautrix.AuthTypePassword,
		Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: user},
		Password:                 cfg.Password,
		InitialDeviceDisplayName: cfg.DeviceName,
		StoreCredentials:         true,
	}

	if err := helper.Init(ctx); err != nil {
		return nil, fmt.Errorf("matrix: crypto init / login: %w", err)
	}
	mx.Crypto = helper

	c := &Client{mx: mx, helper: helper, autoJoinSpaceChildren: cfg.AutoJoinSpaceChildren}
	if err := c.bootstrapCrossSigning(ctx, cfg.Password); err != nil {
		// Non-fatal: bot still works, just shows up as "unverified" in
		// other clients. Worth logging loudly so the operator notices.
		log.Printf("[matrix] cross-signing bootstrap skipped: %v\n", err)
	}
	c.installHandlers()
	return c, nil
}

// bootstrapCrossSigning makes the bot's device self-verified: on first
// run it generates master / self-signing / user-signing keys, uploads
// them via UIA (password), then self-signs its own device. Element and
// other clients then see the bot as a verified user.
//
// Idempotent: skips when the server already has a master key for this
// user. Cross-signing keys, once published, are sticky — there's no
// safe way to re-bootstrap without losing trust from other devices.
func (c *Client) bootstrapCrossSigning(ctx context.Context, password string) error {
	mach := c.helper.Machine()
	pubKeys, err := mach.GetOwnCrossSigningPublicKeys(ctx)
	if err == nil && pubKeys != nil && pubKeys.MasterKey != "" {
		// Already bootstrapped server-side. We can't re-sign our own
		// device without the master/self-signing seed, but the
		// existing setup is fine for trust-on-first-use UX.
		return nil
	}
	if password == "" {
		return fmt.Errorf("password not provided; cannot complete UIA")
	}
	recoveryKey, _, err := mach.GenerateAndUploadCrossSigningKeysWithPassword(ctx, password, "")
	if err != nil {
		return fmt.Errorf("generate / upload cross-signing keys: %w", err)
	}
	// GenerateAndUploadCrossSigningKeys uploads the public keys, but
	// doesn't itself sign the current device with the self-signing
	// key or sign the master key with the device key. Without those
	// the verification chain is incomplete (Element shows the bot as
	// "verified by no one"). Do them explicitly.
	if err := mach.SignOwnDevice(ctx, mach.OwnIdentity()); err != nil {
		return fmt.Errorf("sign own device: %w", err)
	}
	if err := mach.SignOwnMasterKey(ctx); err != nil {
		return fmt.Errorf("sign own master key: %w", err)
	}
	log.Printf("[matrix] cross-signing bootstrap complete; recovery key: %s", recoveryKey)
	log.Printf("[matrix]   (save this if you ever want to log this bot in elsewhere; otherwise discard)")
	return nil
}

// OnMessage registers the inbound text handler. Must be called before Sync.
func (c *Client) OnMessage(h MessageHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = h
}

// UserID returns the bot's full Matrix ID (@bot:domain).
func (c *Client) UserID() id.UserID { return c.mx.UserID }

// SetUserRoomTag adds a per-user m.tag account-data entry on the
// room. Element treats `m.lowpriority` and `m.favourite` natively;
// custom tags (`u.archived`, etc.) appear under "Other". Removing a
// tag = setting empty body via DeleteUserRoomTag.
//
// Note: m.tag is per-user (the calling account's view), not room
// state — other members are unaffected.
func (c *Client) SetUserRoomTag(ctx context.Context, roomID id.RoomID, tag string) error {
	url := c.mx.BuildURL(mautrix.ClientURLPath{
		"v3", "user", c.mx.UserID.String(), "rooms", roomID.String(), "tags", tag,
	})
	_, err := c.mx.MakeRequest(ctx, "PUT", url, map[string]any{"order": 0.5}, nil)
	return err
}

// DeleteUserRoomTag removes a tag previously set via SetUserRoomTag.
func (c *Client) DeleteUserRoomTag(ctx context.Context, roomID id.RoomID, tag string) error {
	url := c.mx.BuildURL(mautrix.ClientURLPath{
		"v3", "user", c.mx.UserID.String(), "rooms", roomID.String(), "tags", tag,
	})
	_, err := c.mx.MakeRequest(ctx, "DELETE", url, nil, nil)
	return err
}

// ParentSpaces returns the Matrix Space room IDs this room is a child
// of, by reading the room's m.space.parent state events. Returns an
// empty slice when the room is not in any space, or on error reading
// state. Order is unspecified (rooms may belong to multiple spaces).
func (c *Client) ParentSpaces(ctx context.Context, roomID id.RoomID) ([]id.RoomID, error) {
	state, err := c.mx.State(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("matrix: read state of %s: %w", roomID, err)
	}
	parents := state[event.StateSpaceParent]
	out := make([]id.RoomID, 0, len(parents))
	for stateKey := range parents {
		if stateKey != "" {
			out = append(out, id.RoomID(stateKey))
		}
	}
	return out, nil
}

// Sync runs the long-poll sync loop until ctx is cancelled.
// Crypto helper drains shutdown via the same context.
func (c *Client) Sync(ctx context.Context) error {
	defer c.helper.Close()
	return c.mx.SyncWithContext(ctx)
}

// Typing toggles the typing notification for this bot in the room.
// Element shows "<bot> is typing..."; auto-expires after timeout if
// not refreshed. Pass on=false to clear immediately.
func (c *Client) Typing(ctx context.Context, roomID id.RoomID, on bool, timeoutMs int) error {
	if !on {
		_, err := c.mx.UserTyping(ctx, roomID, false, 0)
		return err
	}
	_, err := c.mx.UserTyping(ctx, roomID, true, time.Duration(timeoutMs)*time.Millisecond)
	return err
}

// SendText posts a fresh m.room.message (text). When the body looks
// like markdown (tables, headings, code, lists, …), we also fill the
// HTML formatted_body so Element renders it richly. Returns the event
// ID so the caller can edit it later for streaming.
func (c *Client) SendText(ctx context.Context, roomID id.RoomID, body string) (id.EventID, error) {
	content := buildTextContent(body)
	resp, err := c.mx.SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

// EditText replaces the body of an earlier message. Per Matrix spec,
// edits are sent as a new event with m.relates_to.rel_type=m.replace.
// Element renders this as the original event with "(edited)" tag and
// the new body inline. Markdown handling matches SendText.
func (c *Client) EditText(ctx context.Context, roomID id.RoomID, origEventID id.EventID, body string) error {
	inner := buildTextContent(body)
	outer := buildTextContent("* " + body) // fallback for clients that don't render edits
	outer.NewContent = inner
	outer.RelatesTo = &event.RelatesTo{
		Type:    event.RelReplace,
		EventID: origEventID,
	}
	_, err := c.mx.SendMessageEvent(ctx, roomID, event.EventMessage, outer)
	return err
}

// buildTextContent fills body + (optional) formatted_body for an
// m.room.message. Plaintext fallback always set so non-HTML clients
// still see something readable.
func buildTextContent(body string) *event.MessageEventContent {
	c := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    body,
	}
	if html, ok := renderMarkdown(body); ok {
		c.Format = event.FormatHTML
		c.FormattedBody = html
	}
	return c
}

// ----- internals -----

func (c *Client) installHandlers() {
	syncer := c.mx.Syncer.(*mautrix.DefaultSyncer)

	// Auto-join when we're invited to a room. Necessary so users
	// can DM the bot or invite it into a session room.
	syncer.OnEventType(event.StateMember, func(ctx context.Context, evt *event.Event) {
		if evt.GetStateKey() != c.mx.UserID.String() {
			return
		}
		membership := evt.Content.AsMember().Membership
		if membership != event.MembershipInvite {
			return
		}
		if _, err := c.mx.JoinRoomByID(ctx, evt.RoomID); err != nil {
			log.Printf("[matrix] join %s after invite from %s failed: %v", evt.RoomID, evt.Sender, err)
		} else {
			log.Printf("[matrix] joined %s after invite from %s", evt.RoomID, evt.Sender)
		}
	})

	// Auto-join newly added child rooms of any Space the bot is a
	// member of, only when AutoJoinSpaceChildren is enabled. See
	// Config.AutoJoinSpaceChildren docs for the caveat about
	// `restricted` rooms — Synapse may reject auto-auth in some
	// configurations, in which case fall back to manual invite.
	if c.autoJoinSpaceChildren {
		syncer.OnEventType(event.StateSpaceChild, func(ctx context.Context, evt *event.Event) {
			childID := id.RoomID(evt.GetStateKey())
			if childID == "" {
				return
			}
			var content struct {
				Via []string `json:"via"`
			}
			_ = json.Unmarshal(evt.Content.VeryRaw, &content)
			if len(content.Via) == 0 {
				// Empty `via` = removal of the parent/child link, not an addition.
				return
			}
			req := &mautrix.ReqJoinRoom{Via: content.Via}
			if _, err := c.mx.JoinRoom(ctx, childID.String(), req); err != nil {
				log.Printf("[matrix] auto-join space child %s (parent %s) failed: %v — invite the bot manually if needed",
					childID, evt.RoomID, err)
			} else {
				log.Printf("[matrix] auto-joined space child %s (parent %s)",
					childID, evt.RoomID)
			}
		})
	}

	// m.room.message — cryptohelper has already decrypted by the
	// time the syncer calls us, so the content is plain text.
	syncer.OnEventType(event.EventMessage, func(ctx context.Context, evt *event.Event) {
		if evt.Sender == c.mx.UserID {
			return // ignore our own echoes
		}
		msg := evt.Content.AsMessage()
		if msg == nil || msg.MsgType != event.MsgText {
			return
		}
		// Skip edits — we only react to the first send. Edits show up
		// with msg.NewContent != nil; ignore.
		if msg.NewContent != nil {
			return
		}
		text := strings.TrimSpace(msg.Body)
		if text == "" {
			return
		}

		c.mu.Lock()
		h := c.handler
		c.mu.Unlock()
		if h == nil {
			return
		}
		h(ctx, evt.RoomID, evt.ID, evt.Sender, text)
	})
}

// normalizeUserID turns "claude-coder" into "claude-coder" for the
// login API (which wants the localpart). If the caller already passed
// the full @user:domain, we strip the prefix.
func normalizeUserID(userOrFull, homeserver string) string {
	s := strings.TrimPrefix(userOrFull, "@")
	if i := strings.Index(s, ":"); i >= 0 {
		return s[:i]
	}
	_ = homeserver
	return s
}
