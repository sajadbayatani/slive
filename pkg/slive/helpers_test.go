package slive_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/pkg/slive"
)

// newSTUNFreeClient returns a Client with STUN-free ICE so tests stay
// offline and deterministic.
func newSTUNFreeClient(t *testing.T) *slive.Client {
	t.Helper()
	cfg := slive.DefaultSDKConfig()
	cfg.STUNServers = []string{}
	client, err := slive.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestSessionSubscribeRegistersForwarderSubscriber is the TASK-032
// regression test: a domain-only SubscribeTrack must not be required to hit
// the SFU; the signaling Session path must end with
// Snapshot().ForwarderSubscribers >= 1.
func TestSessionSubscribeRegistersForwarderSubscriber(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pub, err := client.Connect(ctx, "room-test-sfu", "publisher")
	if err != nil {
		t.Fatalf("publisher Connect: %v", err)
	}
	defer pub.Close()
	if err := pub.PublishTrack(ctx, "track-test-1", slive.TrackKindAudio, slive.TrackSourceMicrophone); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}

	sub, err := client.Connect(ctx, "room-test-sfu", "subscriber")
	if err != nil {
		t.Fatalf("subscriber Connect: %v", err)
	}
	defer sub.Close()
	if err := sub.SubscribeTrack(ctx, "track-test-1"); err != nil {
		t.Fatalf("SubscribeTrack: %v", err)
	}

	snap := client.Snapshot()
	t.Logf("forwarder_subscribers=%d forwarder_dropped_total=%d rooms_active=%d participants_active=%d",
		snap.ForwarderSubscribers, snap.ForwarderDroppedTotal, snap.RoomsActive, snap.ParticipantsActive)
	if snap.ForwarderSubscribers < 1 {
		t.Errorf("ForwarderSubscribers = %d, want >= 1", snap.ForwarderSubscribers)
	}
	if snap.RoomsActive != 1 || snap.ParticipantsActive != 2 {
		t.Errorf("rooms/participants = %d/%d, want 1/2", snap.RoomsActive, snap.ParticipantsActive)
	}
}

// TestHTTPHandlerHealthz verifies Client.HTTPHandler serves the real
// /healthz diagnostics endpoint (status ok, uptime, goroutines) instead of
// falling through to the signaling handler.
func TestHTTPHandlerHealthz(t *testing.T) {
	client := newSTUNFreeClient(t)
	if _, err := client.JoinRoom(context.Background(), "room-test-health", "alice"); err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}

	server := httptest.NewServer(client.HTTPHandler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var out struct {
		Status        string `json:"status"`
		UptimeSeconds int64  `json:"uptime_seconds"`
		Goroutines    int    `json:"goroutines"`
		RoomsActive   int    `json:"rooms_active"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode JSON: %v (body %s)", err, body)
	}
	t.Logf("status=%s uptime_seconds=%d goroutines=%d rooms_active=%d", out.Status, out.UptimeSeconds, out.Goroutines, out.RoomsActive)
	if out.Status != "ok" {
		t.Errorf("status = %q, want ok", out.Status)
	}
	if out.RoomsActive != 1 {
		t.Errorf("rooms_active = %d, want 1", out.RoomsActive)
	}
}

// TestSessionErrorMapping checks that a duplicate WS publish surfaces the
// domain error as a signaling error response that still carries the frozen
// sentinel identity (guards the round-trip error branch in awaitLocked and the
// ErrorResponse → sentinel mapping).
func TestSessionDuplicatePublishFails(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pub, err := client.Connect(ctx, "room-test-dup", "publisher")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer pub.Close()
	if err := pub.PublishTrack(ctx, "track-dup", slive.TrackKindAudio, slive.TrackSourceMicrophone); err != nil {
		t.Fatalf("first PublishTrack: %v", err)
	}
	err = pub.PublishTrack(ctx, "track-dup", slive.TrackKindAudio, slive.TrackSourceMicrophone)
	if err == nil {
		t.Fatal("second PublishTrack: want error, got nil")
	}
	t.Logf("duplicate publish error surfaced: %v", err)
	if !errors.Is(err, slive.ErrTrackAlreadyPublished) {
		t.Errorf("second PublishTrack error %v does not match ErrTrackAlreadyPublished; "+
			"Session failures must stay sentinel-matchable", err)
	}
}

// TestSessionSubscribeMissingTrackMatchesErrTrackNotFound pins the other half of
// the Session error mapping (B-2): a track miss reaches the client as the
// track_not_found wire *code*, so errors.Is must resolve it from the code
// rather than from message text.
func TestSessionSubscribeMissingTrackMatchesErrTrackNotFound(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sub, err := client.Connect(ctx, "room-track-not-found", "subscriber")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer sub.Close()

	err = sub.SubscribeTrack(ctx, "no-such-track")
	if err == nil {
		t.Fatal("SubscribeTrack on a missing track: want error, got nil")
	}
	t.Logf("missing track error surfaced: %v", err)
	if !errors.Is(err, slive.ErrTrackNotFound) {
		t.Errorf("error %v does not match ErrTrackNotFound", err)
	}
	if errors.Is(err, slive.ErrTrackAlreadyPublished) {
		t.Errorf("error %v must not match an unrelated sentinel", err)
	}
}

// TestRoomMissReportsErrRoomNotFound pins DEF-01 on the stable surface: a room
// miss from any Client method matches the package-local ErrRoomNotFound and
// must NOT match ErrParticipantNotFound. Room misses are raised inside
// pkg/slive (Client.* methods return the sentinel directly), so the wrong
// aliasing identity would surface here.
func TestRoomMissReportsErrRoomNotFound(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx := context.Background()
	const missingRoom = "def01-no-such-room"

	roomMisses := []struct {
		name string
		call func() error
	}{
		{"LeaveRoom", func() error { return client.LeaveRoom(ctx, missingRoom, "alice") }},
		{"PublishTrack", func() error {
			_, err := client.PublishTrack(ctx, missingRoom, "alice", "mic-1", slive.TrackKindAudio, slive.TrackSourceMicrophone)
			return err
		}},
		{"SubscribeTrack", func() error { return client.SubscribeTrack(ctx, missingRoom, "alice", "mic-1") }},
		{"UnsubscribeTrack", func() error { return client.UnsubscribeTrack(ctx, missingRoom, "alice", "mic-1") }},
	}
	for _, tc := range roomMisses {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatalf("%s on a missing room: want error, got nil", tc.name)
			}
			t.Logf("%s room-miss error: %v", tc.name, err)
			if !errors.Is(err, slive.ErrRoomNotFound) {
				t.Errorf("%s room-miss error %v does not match ErrRoomNotFound", tc.name, err)
			}
			if errors.Is(err, slive.ErrParticipantNotFound) {
				t.Errorf("%s room-miss error %v must NOT match ErrParticipantNotFound (DEF-01)", tc.name, err)
			}
		})
	}
}

// TestParticipantMissIsNotRoomNotFound is the DEF-01 positive control: a
// participant miss in an existing room matches ErrParticipantNotFound and not
// ErrRoomNotFound, so the two sentinels have genuinely distinct identities and
// the room fix did not collapse them in the other direction.
func TestParticipantMissIsNotRoomNotFound(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx := context.Background()
	if _, err := client.JoinRoom(ctx, "def01-room", "alice"); err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}
	err := client.LeaveRoom(ctx, "def01-room", "ghost")
	if err == nil {
		t.Fatal("LeaveRoom for a missing participant: want error, got nil")
	}
	t.Logf("participant-miss error: %v", err)
	if !errors.Is(err, slive.ErrParticipantNotFound) {
		t.Errorf("participant-miss error %v does not match ErrParticipantNotFound", err)
	}
	if errors.Is(err, slive.ErrRoomNotFound) {
		t.Errorf("participant-miss error %v must NOT match ErrRoomNotFound", err)
	}
}
