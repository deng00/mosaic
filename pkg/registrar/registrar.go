// Package registrar registers new Matrix users via Synapse's
// shared-secret registration endpoint
// (`/_synapse/admin/v1/register`). The shared secret comes from
// `registration_shared_secret` in the homeserver's homeserver.yaml —
// callers pass it in. No admin token needed; this is the same
// mechanism `register_new_matrix_user` CLI uses internally.
package registrar

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Registrar struct {
	BaseURL      string // homeserver base, e.g. http://127.0.0.1:8008
	SharedSecret string // registration_shared_secret
	HTTP         *http.Client
}

// Register creates a new Matrix user. localpart is the part before
// the colon (e.g. "claude-coder" → @claude-coder:<server>). Returns
// the full user_id on success.
func (r *Registrar) Register(localpart, password, displayName string, admin bool) (userID string, err error) {
	if r.SharedSecret == "" {
		return "", fmt.Errorf("registrar: SharedSecret is empty")
	}
	client := r.HTTP
	if client == nil {
		client = http.DefaultClient
	}

	// Step 1 — fetch a one-shot nonce.
	nonceURL := r.BaseURL + "/_synapse/admin/v1/register"
	resp, err := client.Get(nonceURL)
	if err != nil {
		return "", fmt.Errorf("registrar: get nonce: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("registrar: nonce status %d: %s", resp.StatusCode, body)
	}
	var nonceResp struct {
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(body, &nonceResp); err != nil {
		return "", fmt.Errorf("registrar: parse nonce: %w", err)
	}

	// Step 2 — compute the HMAC-SHA1 mac per
	// https://element-hq.github.io/synapse/latest/admin_api/register_api.html
	mac := hmac.New(sha1.New, []byte(r.SharedSecret))
	mac.Write([]byte(nonceResp.Nonce))
	mac.Write([]byte{0})
	mac.Write([]byte(localpart))
	mac.Write([]byte{0})
	mac.Write([]byte(password))
	mac.Write([]byte{0})
	if admin {
		mac.Write([]byte("admin"))
	} else {
		mac.Write([]byte("notadmin"))
	}
	macHex := hex.EncodeToString(mac.Sum(nil))

	// Step 3 — POST the registration.
	payload, _ := json.Marshal(map[string]any{
		"nonce":        nonceResp.Nonce,
		"username":     localpart,
		"displayname":  displayName,
		"password":     password,
		"admin":        admin,
		"mac":          macHex,
	})
	resp, err = client.Post(nonceURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("registrar: post register: %w", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("registrar: register status %d: %s", resp.StatusCode, body)
	}
	var regResp struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(body, &regResp); err != nil {
		return "", fmt.Errorf("registrar: parse register response: %w", err)
	}
	return regResp.UserID, nil
}
