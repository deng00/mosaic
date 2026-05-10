// readroom: log in, sync long enough for cryptohelper to decrypt
// recent traffic, then print the last N text messages in plaintext.
// Used to verify what the bot actually said in an encrypted room.
//
// Usage:
//
//	readroom -user danny -pass test1234 -room '!abc:localhost' [-n 5]
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func main() {
	homeserver := flag.String("homeserver", "http://127.0.0.1:8008", "homeserver URL")
	user := flag.String("user", "", "localpart")
	pass := flag.String("pass", "", "password")
	roomID := flag.String("room", "", "room id")
	n := flag.Int("n", 8, "max messages to dump")
	dbPath := flag.String("db", "./data/readroom-crypto.db", "crypto sqlite")
	wait := flag.Duration("wait", 6*time.Second, "sync window before dump")
	flag.Parse()

	if *user == "" || *pass == "" || *roomID == "" {
		flag.Usage()
		os.Exit(2)
	}
	_ = os.MkdirAll(filepath.Dir(*dbPath), 0o700)
	pickle := loadOrGenKey(*dbPath + ".pickle")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mx, err := mautrix.NewClient(*homeserver, "", "")
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	helper, err := cryptohelper.NewCryptoHelper(mx, pickle, *dbPath)
	if err != nil {
		log.Fatalf("crypto helper: %v", err)
	}
	helper.LoginAs = &mautrix.ReqLogin{
		Type:                     mautrix.AuthTypePassword,
		Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: *user},
		Password:                 *pass,
		InitialDeviceDisplayName: "readroom",
		StoreCredentials:         true,
	}
	if err := helper.Init(ctx); err != nil {
		log.Fatalf("crypto init: %v", err)
	}
	mx.Crypto = helper

	type entry struct {
		ts     int64
		sender id.UserID
		eid    id.EventID
		body   string
		isEdit bool
		editsTo id.EventID
	}
	var (
		mu      sync.Mutex
		entries []entry
	)

	// Run a short sync first so cryptohelper picks up megolm session
	// keys via to-device events. Without this, historical encrypted
	// events from /messages can't be decrypted.
	syncCtx, syncCancel := context.WithCancel(ctx)
	go func() {
		if err := mx.SyncWithContext(syncCtx); err != nil && err != context.Canceled {
			log.Printf("sync: %v", err)
		}
	}()
	log.Printf("warming megolm sessions for %s ...", *wait)
	time.Sleep(*wait)

	// Fetch recent events from /messages and decrypt each via the
	// cryptohelper. /messages returns plaintext for unencrypted, and
	// raw m.room.encrypted for encrypted events.
	resp, err := mx.Messages(ctx, id.RoomID(*roomID), "", "", mautrix.DirectionBackward, nil, 30)
	if err != nil {
		log.Fatalf("messages: %v", err)
	}
	syncCancel()

	for _, evt := range resp.Chunk {
		if evt.Type == event.EventEncrypted {
			if perr := evt.Content.ParseRaw(event.EventEncrypted); perr != nil {
				entries = append(entries, entry{
					ts: evt.Timestamp, sender: evt.Sender, eid: evt.ID,
					body: "[parse encrypted: " + perr.Error() + "]",
				})
				continue
			}
			decrypted, derr := mx.Crypto.Decrypt(ctx, evt)
			if derr != nil {
				entries = append(entries, entry{
					ts: evt.Timestamp, sender: evt.Sender, eid: evt.ID,
					body: "[undecryptable: " + derr.Error() + "]",
				})
				continue
			}
			evt = decrypted
		}
		if evt.Type != event.EventMessage {
			continue
		}
		_ = evt.Content.ParseRaw(event.EventMessage)
		msg := evt.Content.AsMessage()
		if msg == nil {
			continue
		}
		body := msg.Body
		var editsTo id.EventID
		isEdit := msg.NewContent != nil
		if isEdit {
			body = msg.NewContent.Body
			if msg.RelatesTo != nil {
				editsTo = msg.RelatesTo.EventID
			}
		}
		entries = append(entries, entry{
			ts: evt.Timestamp, sender: evt.Sender, eid: evt.ID,
			body: body, isEdit: isEdit, editsTo: editsTo,
		})
	}
	_ = helper.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(entries) == 0 {
		fmt.Println("(no decryptable messages observed in this sync window)")
		return
	}
	// Show the latest N
	start := 0
	if len(entries) > *n {
		start = len(entries) - *n
	}
	fmt.Printf("=== last %d events in %s ===\n", len(entries)-start, *roomID)
	for _, e := range entries[start:] {
		marker := "  "
		if e.isEdit {
			marker = "✏️ "
		}
		fmt.Printf("%s [%s] %s %s\n",
			marker,
			time.UnixMilli(e.ts).Format("15:04:05.000"),
			e.sender,
			truncate(strings.ReplaceAll(e.body, "\n", " ⏎ "), 200),
		)
	}
}

func loadOrGenKey(p string) []byte {
	if data, err := os.ReadFile(p); err == nil && len(data) >= 32 {
		return data
	}
	buf := make([]byte, 32)
	_, _ = rand.Read(buf)
	_ = os.WriteFile(p, buf, 0o600)
	return buf
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
