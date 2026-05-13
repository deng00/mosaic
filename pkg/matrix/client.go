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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer/html"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/attachment"
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

// Attachment is one inbound media file that the matrix client has
// already downloaded (and, for E2E rooms, decrypted) to local disk.
// Path is an absolute filesystem path the bridge can hand directly to
// a coding-agent runtime.
type Attachment struct {
	Path     string // absolute local path
	MimeType string // e.g. "image/png"
	Kind     string // "image" / "file" / "video" / "audio"
	Filename string // original filename from the event (best-effort)
}

// IncomingMessage bundles a user message + any attachments. Text may
// be empty for media-only messages (Element typically sets Body to
// the filename for media events — we surface that as Text so the
// agent at least sees the caption).
type IncomingMessage struct {
	RoomID      id.RoomID
	EventID     id.EventID
	Sender      id.UserID
	Text        string
	Attachments []Attachment

	// Mentions is the explicit `m.mentions.user_ids` list, when present
	// on the event. Element / mautrix populate it when the sender uses
	// the autocomplete pill; plain-text `@localpart` typed by hand is
	// NOT in here — callers that care must also parse the body.
	Mentions []id.UserID
}

// MessageHandler is invoked for every inbound user message (text or
// media) in any room the bot is in, except echoes of its own sends.
// Runs in the sync goroutine; long work should be punted to its own
// goroutine.
type MessageHandler func(ctx context.Context, msg IncomingMessage)

// SpaceJoinedHandler is invoked after the bot successfully auto-joins
// an m.space.child whose target room is itself a Matrix Space (i.e. a
// "sub-Space" used as a project). parentSpace is the existing Space
// that announced the new child; newSpace is the freshly joined Space.
// spaceName is the new Space's m.room.name (may be empty if the user
// hasn't named it yet). Runs in the sync goroutine — punt long work.
type SpaceJoinedHandler func(ctx context.Context, parentSpace, newSpace id.RoomID, spaceName string)

// PollResponseHandler fires for every inbound org.matrix.msc3381.
// poll.response event not sent by the bot. pollStartID is the poll
// start event being voted on; answerIDs is the list of selected
// option IDs (the bridge sets max_selections=1 today so this is
// typically one element). Used by the bridge for the ask_user flow.
type PollResponseHandler func(ctx context.Context, roomID id.RoomID, pollStartID id.EventID, sender id.UserID, answerIDs []string)

// PollAnswer is one option in a poll the bot creates. ID must be
// unique within the poll; Text is what Element renders on the button.
type PollAnswer struct {
	ID   string
	Text string
}

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

	// MediaDir is where inbound attachments land after download +
	// E2E decrypt. One file per inbound media event, named
	// "<eventID>_<sanitized-filename>.<ext>". Empty disables image
	// support (events are dropped at the syncer). The bridge sets
	// this to data/agents/<id>/attachments/ at startup.
	MediaDir string
}

// Client is the high-level wrapper. Construct via Login.
type Client struct {
	mx     *mautrix.Client
	helper *cryptohelper.CryptoHelper

	autoJoinSpaceChildren bool
	mediaDir              string

	mu          sync.Mutex
	handler      MessageHandler
	spaceJoined  SpaceJoinedHandler
	pollResponse PollResponseHandler
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

	c := &Client{mx: mx, helper: helper, autoJoinSpaceChildren: cfg.AutoJoinSpaceChildren, mediaDir: cfg.MediaDir}
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

// OnSpaceJoined registers a handler invoked after the bot auto-joins a
// child Space (m.space.child whose target is itself a Space). Used to
// auto-initialise per-project state when a new sub-Space appears under
// a Space the bot is in. Requires AutoJoinSpaceChildren=true.
func (c *Client) OnSpaceJoined(h SpaceJoinedHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spaceJoined = h
}

// OnPollResponse registers the inbound poll-response handler. The
// bot's own responses are filtered out before reaching h. Must be
// called before Sync.
func (c *Client) OnPollResponse(h PollResponseHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollResponse = h
}

// SendPollStart posts an org.matrix.msc3381.poll.start event (MSC3381
// unstable namespace — what Element actually consumes today). Element
// renders this natively as a poll card with click-to-vote buttons.
// max_selections is hard-coded to 1; ask_user is single-choice today.
// Returns the new event ID so the caller can later close the poll
// via SendPollEnd and key state lookups on it.
func (c *Client) SendPollStart(ctx context.Context, roomID id.RoomID, question string, answers []PollAnswer) (id.EventID, error) {
	// We assemble the content as map[string]any rather than mautrix's
	// PollStartEventContent because the latter uses anonymous structs
	// for the answers list, which makes idiomatic construction painful.
	// JSON shape is identical either way.
	answersBlock := make([]map[string]any, 0, len(answers))
	for _, a := range answers {
		answersBlock = append(answersBlock, map[string]any{
			"id":                      a.ID,
			"org.matrix.msc1767.text": a.Text,
		})
	}
	content := map[string]any{
		"org.matrix.msc3381.poll.start": map[string]any{
			"kind":           "org.matrix.msc3381.poll.disclosed",
			"max_selections": 1,
			"question":       map[string]any{"org.matrix.msc1767.text": question},
			"answers":        answersBlock,
		},
		// MSC1767 fallback for clients that don't render polls yet —
		// they see the question text and at least know what was asked.
		"org.matrix.msc1767.text": question,
	}
	resp, err := c.mx.SendMessageEvent(ctx, roomID, event.EventUnstablePollStart, content)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

// SendPollEnd posts an org.matrix.msc3381.poll.end event referencing
// pollStartID. Element greys out the poll card and shows summary
// counts. summary is the m.text fallback for non-poll-aware clients
// (e.g. "Poll closed: 火锅").
func (c *Client) SendPollEnd(ctx context.Context, roomID id.RoomID, pollStartID id.EventID, summary string) error {
	content := map[string]any{
		"m.relates_to": map[string]any{
			"rel_type": "m.reference",
			"event_id": pollStartID,
		},
		"org.matrix.msc3381.poll.end": map[string]any{},
		"org.matrix.msc1767.text":     summary,
	}
	_, err := c.mx.SendMessageEvent(ctx, roomID, event.EventUnstablePollEnd, content)
	return err
}

// IsSpace reports whether roomID is a Matrix Space — its m.room.create
// state event has type = "m.space" (RoomTypeSpace). False on read
// error, which is intentional: callers treat unknowns as plain rooms.
func (c *Client) IsSpace(ctx context.Context, roomID id.RoomID) (bool, error) {
	var content event.CreateEventContent
	if err := c.mx.StateEvent(ctx, roomID, event.StateCreate, "", &content); err != nil {
		return false, err
	}
	return content.Type == event.RoomTypeSpace, nil
}

// RoomName reads the m.room.name state event for roomID. Returns ""
// (no error) when the room has no name set — that's the common case
// for freshly-created rooms before the user types one.
func (c *Client) RoomName(ctx context.Context, roomID id.RoomID) (string, error) {
	var content event.RoomNameEventContent
	if err := c.mx.StateEvent(ctx, roomID, event.StateRoomName, "", &content); err != nil {
		// 404 (M_NOT_FOUND) → no name set; surface as empty.
		return "", nil
	}
	return content.Name, nil
}

// UserID returns the bot's full Matrix ID (@bot:domain).
func (c *Client) UserID() id.UserID { return c.mx.UserID }

// SetDisplayName updates the bot's user-profile display name (shows
// in member lists and as the sender label on messages). Empty string
// clears it. Push via Matrix profile API; sticks across restarts.
func (c *Client) SetDisplayName(ctx context.Context, name string) error {
	return c.mx.SetDisplayName(ctx, name)
}

// SetDeviceName updates this device's display label (what shows up
// in the user's "active sessions" page). cryptohelper persists the
// device across restarts but doesn't re-push the display name on
// subsequent inits, so we PUT it explicitly each startup.
func (c *Client) SetDeviceName(ctx context.Context, name string) error {
	if c.mx.DeviceID == "" {
		return nil
	}
	url := c.mx.BuildClientURL("v3", "devices", c.mx.DeviceID)
	_, err := c.mx.MakeRequest(ctx, "PUT", url, &mautrix.ReqPutDevice{DisplayName: name}, nil)
	return err
}

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

// JoinedMemberSet returns the set of currently-joined user ids in
// roomID. Wraps the `/joined_members` API. Used by the bridge to
// decide if a room is "single-agent" (only one of our fleet present →
// broadcast mode) vs "multi-agent" (require explicit @-mention to
// respond).
func (c *Client) JoinedMemberSet(ctx context.Context, roomID id.RoomID) (map[id.UserID]bool, error) {
	resp, err := c.mx.JoinedMembers(ctx, roomID)
	if err != nil {
		return nil, err
	}
	out := make(map[id.UserID]bool, len(resp.Joined))
	for uid := range resp.Joined {
		out[uid] = true
	}
	return out, nil
}

// MyPowerLevel reports the bot's PL in roomID by reading m.room.power_levels.
// Returns the per-user value if listed; otherwise users_default; 0 on error
// (treat unknown as no privilege — callers gate behaviour on >= 50).
func (c *Client) MyPowerLevel(ctx context.Context, roomID id.RoomID) (int, error) {
	var pl event.PowerLevelsEventContent
	if err := c.mx.StateEvent(ctx, roomID, event.StatePowerLevels, "", &pl); err != nil {
		return 0, err
	}
	if v, ok := pl.Users[c.mx.UserID]; ok {
		return v, nil
	}
	return pl.UsersDefault, nil
}

// CreateRoomOpts configures one CreateRoom call.
type CreateRoomOpts struct {
	Name        string      // visible name in Element
	Topic       string      // visible topic / description
	ParentSpace id.RoomID   // optional Space to attach as a child
	Invite      []id.UserID // users to invite
	Preset      string      // e.g. "private_chat" or "trusted_private_chat" (default: "private_chat")

	// StrictParentLink — when true and ParentSpace is set, a failure to
	// publish m.space.child in the parent (typically: bot lacks PL 50)
	// is fatal: CreateRoom leaves the just-created orphan room and
	// returns an error so callers don't accumulate invisible rooms.
	// Default false preserves "best-effort" semantics: the room is
	// created and returned even if the Space link fails.
	StrictParentLink bool
}

// CreateRoom creates a new Matrix room owned by this client. When
// ParentSpace is set, also publishes the m.space.child + m.space.parent
// state events so it shows up in Element under the Space tree. Returns
// the new room id.
func (c *Client) CreateRoom(ctx context.Context, opts CreateRoomOpts) (id.RoomID, error) {
	preset := opts.Preset
	if preset == "" {
		preset = "private_chat"
	}
	req := &mautrix.ReqCreateRoom{
		Name:     opts.Name,
		Topic:    opts.Topic,
		Invite:   opts.Invite,
		Preset:   preset,
		IsDirect: false,
	}
	resp, err := c.mx.CreateRoom(ctx, req)
	if err != nil {
		return "", fmt.Errorf("matrix: create room: %w", err)
	}
	roomID := resp.RoomID
	if opts.ParentSpace != "" {
		// Best-effort wire into the Space. Failures here don't void
		// the room (unless StrictParentLink is set).
		via := serverPart(string(c.mx.UserID))
		if _, err := c.mx.SendStateEvent(ctx, opts.ParentSpace, event.StateSpaceChild, string(roomID),
			&event.SpaceChildEventContent{Via: []string{via}}); err != nil {
			log.Printf("[matrix] m.space.child failed (room created OK): %v", err)
			if opts.StrictParentLink {
				// Clean up the orphan so we don't leave invisible
				// rooms in the bot's joined-set. Best-effort.
				if _, leaveErr := c.mx.LeaveRoom(ctx, roomID); leaveErr != nil {
					log.Printf("[matrix] orphan cleanup leave %s failed: %v", roomID, leaveErr)
				}
				return "", fmt.Errorf("matrix: link child to parent space %s: %w", opts.ParentSpace, err)
			}
		}
		if _, err := c.mx.SendStateEvent(ctx, roomID, event.StateSpaceParent, string(opts.ParentSpace),
			&event.SpaceParentEventContent{Via: []string{via}, Canonical: true}); err != nil {
			log.Printf("[matrix] m.space.parent failed (room created OK): %v", err)
		}
	}
	return roomID, nil
}

// CreateSpace creates a new Matrix Space owned by this client. Mosaic
// uses this for `/project new`: the bot is the creator (PL 100) so it
// can immediately set state events, link child rooms, etc. — the
// user-typed-in-Element flow always leaves the bot at PL 0 and forces
// a manual promotion.
//
// inviteUser, when non-empty, is invited and granted PL 100 as a
// joint owner. Pass "" to skip.
//
// parentSpace, when non-empty, is best-effort linked: m.space.child
// in parent (needs PL ≥ 50 in parent — surfaced via parentLinkErr if
// it fails) and m.space.parent in the new Space (bot is creator so
// this succeeds barring transient errors). The new Space is returned
// regardless of parent-link success.
func (c *Client) CreateSpace(ctx context.Context, name string, parentSpace id.RoomID, inviteUser id.UserID) (newSpace id.RoomID, parentLinkErr error, err error) {
	plUsers := map[id.UserID]int{c.mx.UserID: 100}
	var invites []id.UserID
	if inviteUser != "" && inviteUser != c.mx.UserID {
		invites = []id.UserID{inviteUser}
		plUsers[inviteUser] = 100
	}
	req := &mautrix.ReqCreateRoom{
		Name:   name,
		Invite: invites,
		Preset: "private_chat",
		CreationContent: map[string]interface{}{
			"type": string(event.RoomTypeSpace),
		},
		PowerLevelOverride: &event.PowerLevelsEventContent{
			Users: plUsers,
		},
	}
	resp, err := c.mx.CreateRoom(ctx, req)
	if err != nil {
		return "", nil, fmt.Errorf("matrix: create space: %w", err)
	}
	newSpace = resp.RoomID
	if parentSpace == "" {
		return newSpace, nil, nil
	}
	via := serverPart(string(c.mx.UserID))
	if _, e := c.mx.SendStateEvent(ctx, parentSpace, event.StateSpaceChild, string(newSpace),
		&event.SpaceChildEventContent{Via: []string{via}}); e != nil {
		parentLinkErr = fmt.Errorf("link m.space.child in parent %s: %w", parentSpace, e)
	}
	if _, e := c.mx.SendStateEvent(ctx, newSpace, event.StateSpaceParent, string(parentSpace),
		&event.SpaceParentEventContent{Via: []string{via}, Canonical: true}); e != nil {
		log.Printf("[matrix] m.space.parent in new space %s failed: %v", newSpace, e)
	}
	return newSpace, parentLinkErr, nil
}

// serverPart returns the homeserver portion of "@user:server" or
// "!room:server". Used to compute the "via" hint for space child/parent
// state events.
func serverPart(matrixID string) string {
	if i := strings.IndexByte(matrixID, ':'); i >= 0 {
		return matrixID[i+1:]
	}
	return ""
}

// ParentSpaces returns the Matrix Space room IDs this room is a child
// of, by reading the room's m.space.parent state events. Returns an
// empty slice when the room is not in any space, or on error reading
// state. Order is unspecified (rooms may belong to multiple spaces).
//
// Caveat: many clients (incl. Element) only set m.space.child in the
// parent and NOT m.space.parent in the child, so this can return [] for
// rooms that are nonetheless visibly nested. For walking up the tree
// reliably, use SpaceHierarchy which is bidirectional.
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

// SpaceHierarchy scans every Space the bot is joined to and reads its
// m.space.child events, returning a map child → parent Spaces. This is
// the inverse of m.space.parent and works around clients that publish
// only m.space.child (Element being the canonical case): walking up
// purely via ParentSpaces stops at the first Space the user added via
// Element's "Add to Space" UI without echoing m.space.parent in the
// child. Empty-via entries (i.e. removed links) are skipped.
//
// Cost: one /joined_rooms call + one State call per joined room.
// Acceptable for slash-command use; cache caller-side if hot.
func (c *Client) SpaceHierarchy(ctx context.Context) (map[id.RoomID][]id.RoomID, error) {
	joined, err := c.mx.JoinedRooms(ctx)
	if err != nil {
		return nil, fmt.Errorf("matrix: joined_rooms: %w", err)
	}
	childToParents := map[id.RoomID][]id.RoomID{}
	for _, r := range joined.JoinedRooms {
		state, err := c.mx.State(ctx, r)
		if err != nil {
			continue
		}
		children := state[event.StateSpaceChild]
		if len(children) == 0 {
			continue // not a Space (or a Space with no children)
		}
		for stateKey, evt := range children {
			if stateKey == "" || evt == nil {
				continue
			}
			var content struct {
				Via []string `json:"via"`
			}
			_ = json.Unmarshal(evt.Content.VeryRaw, &content)
			if len(content.Via) == 0 {
				continue // empty via = link removed
			}
			child := id.RoomID(stateKey)
			childToParents[child] = append(childToParents[child], r)
		}
	}
	return childToParents, nil
}

// JoinedRoomIDs is a thin wrapper over /joined_rooms used by export
// and other fleet-wide enumerators. Returned in homeserver order — no
// sorting, callers shouldn't assume one.
func (c *Client) JoinedRoomIDs(ctx context.Context) ([]id.RoomID, error) {
	resp, err := c.mx.JoinedRooms(ctx)
	if err != nil {
		return nil, fmt.Errorf("matrix: joined_rooms: %w", err)
	}
	return resp.JoinedRooms, nil
}

// RoomMessagesBackward fetches one page of history for roomID, walking
// backwards from `from` (empty = latest). Returns the raw event chunk
// and the pagination token to use for the next call. When `end` comes
// back empty the room has been fully walked.
//
// Events are NOT decrypted here — Messages chunks come from the server
// as-is. Run DecryptIfEncrypted on each event afterwards.
func (c *Client) RoomMessagesBackward(ctx context.Context, roomID id.RoomID, from string, limit int) (chunk []*event.Event, end string, err error) {
	if limit <= 0 {
		limit = 100
	}
	resp, err := c.mx.Messages(ctx, roomID, from, "", mautrix.DirectionBackward, nil, limit)
	if err != nil {
		return nil, "", fmt.Errorf("matrix: messages %s: %w", roomID, err)
	}
	return resp.Chunk, resp.End, nil
}

// DecryptIfEncrypted decrypts evt in place when it's an m.room.encrypted
// megolm event. For plaintext events it's a no-op (returns evt unchanged).
// The returned event has Type / Content rewritten to the cleartext form
// when decryption succeeds.
//
// Failure modes (returned as error): no megolm session for this event
// (the agent joined the room after the key rotated and never received
// it), corrupted ciphertext, or the helper's olm machine isn't ready.
// Callers should record the failure but keep walking — one missing
// session shouldn't abort a room export.
func (c *Client) DecryptIfEncrypted(ctx context.Context, evt *event.Event) (*event.Event, error) {
	if evt.Type != event.EventEncrypted {
		return evt, nil
	}
	// Messages-endpoint events arrive with Content.VeryRaw populated but
	// Parsed nil; DecryptMegolmEvent type-asserts on Parsed so we must
	// fill it first.
	if evt.Content.Parsed == nil {
		if err := evt.Content.ParseRaw(evt.Type); err != nil {
			return evt, fmt.Errorf("parse encrypted content: %w", err)
		}
	}
	decrypted, err := c.helper.Machine().DecryptMegolmEvent(ctx, evt)
	if err != nil {
		return evt, err
	}
	return decrypted, nil
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
	return c.SendTextMentions(ctx, roomID, body, nil)
}

// SendTextMentions is like SendText but populates the protocol-level
// m.mentions.user_ids field. Required for peer agents to recognise
// the message as an intentional ping (their router only trusts
// m.mentions for bot-to-bot dispatch). Pass nil/empty for normal
// (non-pinging) sends.
func (c *Client) SendTextMentions(ctx context.Context, roomID id.RoomID, body string, mentions []id.UserID) (id.EventID, error) {
	content := buildTextContent(body)
	if len(mentions) > 0 {
		content.Mentions = &event.Mentions{UserIDs: mentions}
	}
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
	return c.EditTextMentions(ctx, roomID, origEventID, body, nil)
}

// EditTextMentions is like EditText but propagates m.mentions on the
// new-content envelope. Note Element silently swallows edits when
// computing notifications, so this is mostly cosmetic — what matters
// for peer routing is that the INITIAL send carried the mention.
// Streaming agent output should still call this so post-merge text
// records the final mention set accurately.
func (c *Client) EditTextMentions(ctx context.Context, roomID id.RoomID, origEventID id.EventID, body string, mentions []id.UserID) error {
	inner := buildTextContent(body)
	if len(mentions) > 0 {
		inner.Mentions = &event.Mentions{UserIDs: mentions}
	}
	outer := buildTextContent("* " + body) // fallback for clients that don't render edits
	outer.NewContent = inner
	outer.RelatesTo = &event.RelatesTo{
		Type:    event.RelReplace,
		EventID: origEventID,
	}
	_, err := c.mx.SendMessageEvent(ctx, roomID, event.EventMessage, outer)
	return err
}

// SendImage uploads localPath to the homeserver's media repo and
// posts an m.image event into roomID. Detects whether the room is
// encrypted and follows the EncryptedFileInfo path when so; otherwise
// publishes a plaintext mxc:// URL. mimeType is best-effort — if
// empty, sniff from the first 512 bytes. Caption goes into the body
// (also the plaintext fallback for non-image clients); empty caption
// falls back to the file's basename.
//
// Returns the new event ID on success. Errors do NOT include
// homeserver-side rate-limit retries — callers (the bridge) should
// log + skip.
func (c *Client) SendImage(ctx context.Context, roomID id.RoomID, localPath, mimeType, caption string) (id.EventID, error) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return "", fmt.Errorf("matrix: read image %s: %w", localPath, err)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("matrix: image %s is empty", localPath)
	}
	if mimeType == "" {
		probe := data
		if len(probe) > 512 {
			probe = probe[:512]
		}
		mimeType = http.DetectContentType(probe)
	}
	filename := filepath.Base(localPath)
	body := caption
	if body == "" {
		body = filename
	}

	encrypted, _ := c.roomEncrypted(ctx, roomID)

	content := &event.MessageEventContent{
		MsgType: event.MsgImage,
		Body:    body,
		Info: &event.FileInfo{
			MimeType: mimeType,
			Size:     len(data),
		},
	}

	if encrypted {
		ef := attachment.NewEncryptedFile()
		cipher := ef.Encrypt(data)
		resp, err := c.mx.UploadBytesWithName(ctx, cipher, "application/octet-stream", filename)
		if err != nil {
			return "", fmt.Errorf("matrix: upload encrypted image: %w", err)
		}
		fi := &event.EncryptedFileInfo{EncryptedFile: *ef}
		fi.URL = resp.ContentURI.CUString()
		content.File = fi
	} else {
		resp, err := c.mx.UploadBytesWithName(ctx, data, mimeType, filename)
		if err != nil {
			return "", fmt.Errorf("matrix: upload image: %w", err)
		}
		content.URL = resp.ContentURI.CUString()
	}

	out, err := c.mx.SendMessageEvent(ctx, roomID, event.EventMessage, content)
	if err != nil {
		return "", fmt.Errorf("matrix: send m.image: %w", err)
	}
	return out.EventID, nil
}

// roomEncrypted reports whether roomID has an m.room.encryption
// state event set. Prefers the client's state store (populated by
// the sync loop) and falls back to a direct StateEvent probe so a
// freshly-joined room not yet seen in /sync still gets the right
// answer.
func (c *Client) roomEncrypted(ctx context.Context, roomID id.RoomID) (bool, error) {
	if c.mx.StateStore != nil {
		if enc, err := c.mx.StateStore.IsEncrypted(ctx, roomID); err == nil && enc {
			return true, nil
		}
	}
	var content event.EncryptionEventContent
	if err := c.mx.StateEvent(ctx, roomID, event.StateEncryption, "", &content); err != nil {
		return false, nil
	}
	return content.Algorithm != "", nil
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
				return
			}
			log.Printf("[matrix] auto-joined space child %s (parent %s)",
				childID, evt.RoomID)

			// If the child is itself a Space (sub-Space used as a
			// project), let higher layers auto-initialise project
			// state. Detached goroutine because StateEvent calls hit
			// the homeserver and we don't want to stall the syncer.
			c.mu.Lock()
			h := c.spaceJoined
			c.mu.Unlock()
			if h == nil {
				return
			}
			parentID := evt.RoomID
			go func() {
				bg := context.Background()
				isSpace, err := c.IsSpace(bg, childID)
				if err != nil || !isSpace {
					return
				}
				name, _ := c.RoomName(bg, childID)
				h(bg, parentID, childID, name)
			}()
		})
	}

	// m.room.message — cryptohelper has already decrypted by the
	// time the syncer calls us, so msg content is plaintext.
	syncer.OnEventType(event.EventMessage, func(ctx context.Context, evt *event.Event) {
		if evt.Sender == c.mx.UserID {
			return // ignore our own echoes
		}
		msg := evt.Content.AsMessage()
		if msg == nil {
			return
		}
		// Skip edits — we only react to the first send. Edits show up
		// with msg.NewContent != nil; ignore.
		if msg.NewContent != nil {
			return
		}

		im := IncomingMessage{
			RoomID:  evt.RoomID,
			EventID: evt.ID,
			Sender:  evt.Sender,
		}
		if msg.Mentions != nil {
			im.Mentions = msg.Mentions.UserIDs
		}

		switch msg.MsgType {
		case event.MsgText, event.MsgNotice:
			im.Text = strings.TrimSpace(msg.Body)
			if im.Text == "" {
				return
			}
		case event.MsgImage, event.MsgFile, event.MsgVideo, event.MsgAudio:
			if c.mediaDir == "" {
				log.Printf("[matrix] %s media event in %s dropped: MediaDir not configured", msg.MsgType, evt.RoomID)
				return
			}
			att, err := c.downloadAttachment(ctx, msg, evt.ID)
			if err != nil {
				log.Printf("[matrix] download attachment %s failed: %v", evt.ID, err)
				return
			}
			im.Attachments = []Attachment{att}
			// Caption only — GetCaption returns body when filename
			// differs (Element's "add caption" feature), "" otherwise.
			// Avoids surfacing the bare filename as a user text turn.
			im.Text = strings.TrimSpace(msg.GetCaption())
		default:
			// Unknown msgtype — ignore.
			return
		}

		c.mu.Lock()
		h := c.handler
		c.mu.Unlock()
		if h == nil {
			return
		}
		h(ctx, im)
	})

	// org.matrix.msc3381.poll.response — Element's native poll vote.
	// Filter the bot's own votes; require a reference back to the
	// poll start event. Multiple selections are allowed by spec; we
	// pass the full list through and let the bridge pick the first
	// (max_selections is 1 in our polls).
	syncer.OnEventType(event.EventUnstablePollResponse, func(ctx context.Context, evt *event.Event) {
		if evt.Sender == c.mx.UserID {
			return
		}
		var content event.PollResponseEventContent
		if err := json.Unmarshal(evt.Content.VeryRaw, &content); err != nil {
			return
		}
		rel := content.RelatesTo
		if rel.EventID == "" {
			return
		}
		c.mu.Lock()
		h := c.pollResponse
		c.mu.Unlock()
		if h == nil {
			return
		}
		h(ctx, evt.RoomID, rel.EventID, evt.Sender, content.Response.Answers)
	})
}

// downloadAttachment fetches the media payload of a media message
// event, decrypting with the embedded EncryptedFileInfo when the room
// is E2EE. The plaintext bytes are written under c.mediaDir as
// "<eventID>_<sanitized-filename>" and the resulting Attachment is
// returned. Best-effort mime detection: prefer the event's
// FileInfo.MimeType when present; otherwise sniff the first 512 bytes.
func (c *Client) downloadAttachment(ctx context.Context, msg *event.MessageEventContent, eventID id.EventID) (Attachment, error) {
	var (
		data []byte
		err  error
	)
	switch {
	case msg.File != nil:
		// Encrypted attachment: download ciphertext from the embedded
		// URL, then decrypt with the per-file key.
		mxc, perr := msg.File.URL.Parse()
		if perr != nil {
			return Attachment{}, fmt.Errorf("parse encrypted mxc: %w", perr)
		}
		cipher, derr := c.mx.DownloadBytes(ctx, mxc)
		if derr != nil {
			return Attachment{}, fmt.Errorf("download encrypted: %w", derr)
		}
		data, err = msg.File.Decrypt(cipher)
		if err != nil {
			return Attachment{}, fmt.Errorf("decrypt attachment: %w", err)
		}
	case msg.URL != "":
		mxc, perr := msg.URL.Parse()
		if perr != nil {
			return Attachment{}, fmt.Errorf("parse mxc: %w", perr)
		}
		data, err = c.mx.DownloadBytes(ctx, mxc)
		if err != nil {
			return Attachment{}, fmt.Errorf("download: %w", err)
		}
	default:
		return Attachment{}, fmt.Errorf("media event has neither url nor file")
	}

	mime := ""
	if msg.Info != nil && msg.Info.MimeType != "" {
		mime = msg.Info.MimeType
	}
	if mime == "" {
		probe := data
		if len(probe) > 512 {
			probe = probe[:512]
		}
		mime = http.DetectContentType(probe)
	}

	kind := "file"
	switch {
	case strings.HasPrefix(mime, "image/"):
		kind = "image"
	case strings.HasPrefix(mime, "video/"):
		kind = "video"
	case strings.HasPrefix(mime, "audio/"):
		kind = "audio"
	}

	if err := os.MkdirAll(c.mediaDir, 0o700); err != nil {
		return Attachment{}, fmt.Errorf("mkdir media dir: %w", err)
	}
	origName := msg.GetFileName()
	filename := sanitizeFilename(origName)
	if filename == "" {
		filename = "attachment" + extFromMime(mime)
	} else if filepath.Ext(filename) == "" {
		filename += extFromMime(mime)
	}
	// Prepend event id so a sloppy filename collision can't overwrite
	// an earlier attachment from a different event in the same room.
	stem := sanitizeFilename(string(eventID))
	path := filepath.Join(c.mediaDir, stem+"_"+filename)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return Attachment{}, fmt.Errorf("write %s: %w", path, err)
	}
	log.Printf("[matrix] downloaded %s attachment (%d B) → %s", kind, len(data), path)
	return Attachment{
		Path:     path,
		MimeType: mime,
		Kind:     kind,
		Filename: origName,
	}, nil
}

// sanitizeFilename strips path separators and characters likely to
// trip up shells / runtimes. Empty input returns "".
func sanitizeFilename(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	r := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_",
		"?", "_", "\"", "_", "<", "_", ">", "_", "|", "_",
		" ", "_", "\t", "_", "\n", "_",
	)
	out := r.Replace(s)
	if len(out) > 80 {
		out = out[:80]
	}
	return out
}

// extFromMime returns a "." extension for a common mime type, or "" if
// unknown. Keep this list short — too aggressive guessing causes more
// confusion than it solves.
func extFromMime(mime string) string {
	switch mime {
	case "image/png":
		return ".png"
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	case "audio/ogg":
		return ".ogg"
	case "audio/mpeg":
		return ".mp3"
	}
	return ""
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
