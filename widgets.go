package main

import (
	"context"
	"log"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/deng00/mosaic/pkg/web"
)

// startWidgetPusher walks every project that has a task_prefix +
// places an Element widget pointing at the task board into the
// corresponding Space. Runs in a goroutine because we have to wait
// until at least one of our agent bridges is online AND joined to the
// Space before SendStateEvent will succeed (M_FORBIDDEN otherwise).
//
// Idempotent: same stateKey reused on every push, so re-running the
// daemon overwrites the same state event instead of duplicating
// entries in Element's "Widgets" panel.
func startWidgetPusher(ctx context.Context, fc *FileConfig, srv *web.Server, rt *AgentRuntime) {
	if srv == nil {
		return
	}
	go widgetPusherLoop(ctx, fc, srv, rt)
}

func widgetPusherLoop(ctx context.Context, fc *FileConfig, srv *web.Server, rt *AgentRuntime) {
	// pending tracks Spaces we still need to push for. Removed once
	// successfully posted.
	pending := map[string]string{} // spaceID → human name (for log)
	for sid, pc := range fc.Projects {
		if pc.TaskPrefix == "" {
			continue
		}
		pending[sid] = pc.Name
	}
	if len(pending) == 0 {
		return
	}

	// Try every 3s for the first ~minute, then back off to every 30s.
	// Most installs have an agent online within seconds; the slow
	// fallback catches restarts where agents come up later.
	delays := []time.Duration{
		3 * time.Second, 3 * time.Second, 3 * time.Second, 3 * time.Second,
		5 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second,
	}
	for attempt := 0; ctx.Err() == nil && len(pending) > 0; attempt++ {
		d := delays[len(delays)-1]
		if attempt < len(delays) {
			d = delays[attempt]
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(d):
		}

		for sid, name := range pending {
			if pushOne(ctx, sid, name, srv, rt) {
				delete(pending, sid)
				log.Printf("[widget] task board widget pushed to space %s (%q)", sid, name)
			}
		}
	}
}

// pushOne tries to publish the widget state event in spaceID via any
// online agent that is joined to the Space. Returns true on success.
func pushOne(ctx context.Context, spaceID, name string, srv *web.Server, rt *AgentRuntime) bool {
	url := srv.BoardURL(spaceID)
	stateKey := "mosaic-tasks"
	displayName := "Tasks"
	if name != "" {
		displayName = name + " · Tasks"
	}
	for _, ai := range rt.List() {
		if !ai.Online {
			continue
		}
		br := rt.BridgeForAgent(ai.ID)
		if br == nil {
			continue
		}
		// Some agents may not have joined every Space yet — skip and
		// try the next one rather than failing the whole pass.
		if !br.HasJoinedRoom(ctx, id.RoomID(spaceID)) {
			continue
		}
		if err := br.EnsureWidget(ctx, id.RoomID(spaceID), stateKey, displayName, url); err != nil {
			log.Printf("[widget] %s via %s failed: %v (will retry)", spaceID, ai.ID, err)
			continue
		}
		return true
	}
	return false
}
