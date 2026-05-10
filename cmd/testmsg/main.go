// testmsg: one-shot client that logs in as a Matrix user, syncs
// briefly to learn member device keys, and sends an encrypted message
// into the given room. Used to drive end-to-end smoke tests against
// the agent without needing a browser.
//
// Usage:
//
//	testmsg \
//	  -homeserver http://127.0.0.1:8008 \
//	  -user danny -pass test1234 \
//	  -room '!TFNeSknsiSRiNJADKV:localhost' \
//	  -msg 'hello bot' \
//	  [-db ./data/testmsg-crypto.db] [-wait 3s]
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"log"
	"os"
	"path/filepath"
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
	roomID := flag.String("room", "", "room id, e.g. !abc:localhost")
	msg := flag.String("msg", "hello", "message body")
	dbPath := flag.String("db", "./data/testmsg-crypto.db", "crypto sqlite path")
	wait := flag.Duration("wait", 5*time.Second, "sync window before sending (for key fetch)")
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
		InitialDeviceDisplayName: "testmsg",
		StoreCredentials:         true,
	}
	if err := helper.Init(ctx); err != nil {
		log.Fatalf("crypto init: %v", err)
	}
	mx.Crypto = helper

	// Sync briefly so cryptohelper learns about room members and
	// fetches their device keys (otherwise SendMessageEvent has
	// nothing to encrypt to).
	syncCtx, syncCancel := context.WithCancel(ctx)
	go func() {
		if err := mx.SyncWithContext(syncCtx); err != nil && err != context.Canceled {
			log.Printf("sync: %v", err)
		}
	}()
	log.Printf("syncing for %s before send...", *wait)
	time.Sleep(*wait)

	resp, err := mx.SendMessageEvent(ctx, id.RoomID(*roomID), event.EventMessage, &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    *msg,
	})
	if err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("sent event %s in %s", resp.EventID, *roomID)

	syncCancel()
	_ = helper.Close()
}

func loadOrGenKey(p string) []byte {
	if data, err := os.ReadFile(p); err == nil && len(data) >= 32 {
		return data
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		log.Fatalf("rand: %v", err)
	}
	_ = os.WriteFile(p, buf, 0o600)
	return buf
}
