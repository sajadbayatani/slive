package slive_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/pkg/slive"
)

// client_test.go covers the stable Client surface that the SFU/session tests in
// helpers_test.go do not reach: error identity on the frozen sentinels (DEF-01),
// the documented JoinRoom idempotency under concurrency (B-4), and the
// lifecycle of the in-process signaling server, which is a plain net/http
// server rather than net/http/httptest (#5).

// TestRoomSentinelsHaveOwnIdentity pins DEF-01: the two room sentinels are
// owned by pkg/slive and must not share identity with any other exported
// sentinel, while remaining matchable through their own wrap chain.
func TestRoomSentinelsHaveOwnIdentity(t *testing.T) {
	notSameIdentity := [][2]error{
		{slive.ErrRoomNotFound, slive.ErrParticipantNotFound},
		{slive.ErrRoomNotFound, slive.ErrParticipantAlreadyExists},
		{slive.ErrRoomNotFound, slive.ErrRoomAlreadyExists},
		{slive.ErrRoomAlreadyExists, slive.ErrParticipantAlreadyExists},
		{slive.ErrRoomAlreadyExists, slive.ErrParticipantNotFound},
		{slive.ErrRoomNotFound, slive.ErrRoomClosed},
		{slive.ErrRoomAlreadyExists, slive.ErrTrackNotFound},
	}

	for _, pair := range notSameIdentity {
		a, b := pair[0], pair[1]
		if errors.Is(a, b) {
			t.Errorf("errors.Is(%v, %v) = true; these sentinels must have distinct identities", a, b)
		}
		if errors.Is(b, a) {
			t.Errorf("errors.Is(%v, %v) = true; these sentinels must have distinct identities", b, a)
		}
	}

	for _, sentinel := range []error{slive.ErrRoomNotFound, slive.ErrRoomAlreadyExists} {
		wrapped := fmt.Errorf("slive op: %w", sentinel)
		if !errors.Is(wrapped, sentinel) {
			t.Errorf("%v is not matchable through a %%w chain", sentinel)
		}
	}
}

// TestRoomManagerKeepsInternalIdentity records the deliberate limit of the
// DEF-01 fix: slive.ErrRoomNotFound is produced by Client-level methods only.
// The unstable RoomManager alias still surfaces the internal participant
// errors, which doubles as the positive control proving errors.Is keeps working
// for the sentinels that do alias internal/*.
func TestRoomManagerKeepsInternalIdentity(t *testing.T) {
	rm := slive.NewRoomManager()

	err := rm.CloseRoom("room-never-created")
	if err == nil {
		t.Fatal("CloseRoom on a missing room: want error, got nil")
	}
	if !errors.Is(err, slive.ErrParticipantNotFound) {
		t.Errorf("RoomManager.CloseRoom %v must still match ErrParticipantNotFound (internal identity)", err)
	}
	if errors.Is(err, slive.ErrRoomNotFound) {
		t.Errorf("RoomManager.CloseRoom %v must not match the pkg/slive ErrRoomNotFound", err)
	}

	if _, err := rm.CreateRoom("room-twice"); err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	err = func() error {
		_, err := rm.CreateRoom("room-twice")
		return err
	}()
	if err == nil {
		t.Fatal("duplicate CreateRoom: want error, got nil")
	}
	if !errors.Is(err, slive.ErrParticipantAlreadyExists) {
		t.Errorf("RoomManager.CreateRoom %v must still match ErrParticipantAlreadyExists (internal identity)", err)
	}
	if errors.Is(err, slive.ErrRoomAlreadyExists) {
		t.Errorf("RoomManager.CreateRoom %v must not match the pkg/slive ErrRoomAlreadyExists", err)
	}
}

// TestClientRoomMissReportsErrRoomNotFound closes gap G-1 (LeaveRoom was never
// exercised anywhere) and pins the DEF-01 consequence the review demanded:
// every room-level miss on the Client matches ErrRoomNotFound and *not*
// ErrParticipantNotFound, so a consumer can tell the two apart.
func TestClientRoomMissReportsErrRoomNotFound(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx := context.Background()

	cases := []struct {
		name string
		call func() error
	}{
		{"LeaveRoom", func() error { return client.LeaveRoom(ctx, "no-such-room", "alice") }},
		{"PublishTrack", func() error {
			_, err := client.PublishTrack(ctx, "no-such-room", "alice", "mic-1",
				slive.TrackKindAudio, slive.TrackSourceMicrophone)
			return err
		}},
		{"SubscribeTrack", func() error {
			return client.SubscribeTrack(ctx, "no-such-room", "alice", "mic-1")
		}},
		{"UnsubscribeTrack", func() error {
			return client.UnsubscribeTrack(ctx, "no-such-room", "alice", "mic-1")
		}},
	}

	for _, tc := range cases {
		err := tc.call()
		if err == nil {
			t.Fatalf("%s on a missing room: want error, got nil", tc.name)
		}
		if !errors.Is(err, slive.ErrRoomNotFound) {
			t.Errorf("%s: error %v does not match ErrRoomNotFound", tc.name, err)
		}
		if errors.Is(err, slive.ErrParticipantNotFound) {
			t.Errorf("%s: a room miss must not match ErrParticipantNotFound (DEF-01), got %v", tc.name, err)
		}
		if errors.Is(err, slive.ErrParticipantAlreadyExists) {
			t.Errorf("%s: a room miss must not match ErrParticipantAlreadyExists (DEF-01), got %v", tc.name, err)
		}
	}
}

// TestClientParticipantMissKeepsParticipantIdentity pins the other side of the
// split: a present room with an absent participant still reports
// ErrParticipantNotFound and must not be mistaken for a room miss.
func TestClientParticipantMissKeepsParticipantIdentity(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx := context.Background()

	if _, err := client.JoinRoom(ctx, "room-participant-miss", "alice"); err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}

	if err := client.LeaveRoom(ctx, "room-participant-miss", "ghost"); !errors.Is(err, slive.ErrParticipantNotFound) {
		t.Errorf("LeaveRoom unknown participant: error %v does not match ErrParticipantNotFound", err)
	}
	if err := client.SubscribeTrack(ctx, "room-participant-miss", "ghost", "mic-1"); !errors.Is(err, slive.ErrParticipantNotFound) {
		t.Errorf("SubscribeTrack unknown participant: error %v does not match ErrParticipantNotFound", err)
	}
	// The room exists, so the room sentinel must not fire.
	if err := client.LeaveRoom(ctx, "room-participant-miss", "ghost"); errors.Is(err, slive.ErrRoomNotFound) {
		t.Errorf("participant miss must not match ErrRoomNotFound, got %v", err)
	}
}

// TestClientLeaveRoomRemovesParticipant exercises the happy path of the method
// that gap G-1 found untested: after a real leave the participant is gone and
// the room survives.
func TestClientLeaveRoomRemovesParticipant(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx := context.Background()

	room, err := client.JoinRoom(ctx, "room-leave", "alice")
	if err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}
	if _, err := client.JoinRoom(ctx, "room-leave", "bob"); err != nil {
		t.Fatalf("JoinRoom: %v", err)
	}

	if err := client.LeaveRoom(ctx, "room-leave", "alice"); err != nil {
		t.Fatalf("LeaveRoom: %v", err)
	}
	if room.GetParticipant("alice") != nil {
		t.Error("alice is still in the room after LeaveRoom")
	}
	if room.GetParticipant("bob") == nil {
		t.Error("bob disappeared from the room")
	}
	// Leaving twice is a participant miss, not a room miss.
	if err := client.LeaveRoom(ctx, "room-leave", "alice"); !errors.Is(err, slive.ErrParticipantNotFound) {
		t.Errorf("second LeaveRoom: error %v does not match ErrParticipantNotFound", err)
	}
}

// TestClientJoinRoomIsIdempotent covers the sequential half of the documented
// contract: an already-present participantID returns the same room without
// re-joining.
func TestClientJoinRoomIsIdempotent(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx := context.Background()

	first, err := client.JoinRoom(ctx, "room-idempotent", "alice")
	if err != nil {
		t.Fatalf("first JoinRoom: %v", err)
	}
	second, err := client.JoinRoom(ctx, "room-idempotent", "alice")
	if err != nil {
		t.Fatalf("second JoinRoom: %v", err)
	}
	if first != second {
		t.Errorf("idempotent join returned a different *Room (%p vs %p)", first, second)
	}
	if got := len(first.Participants()); got != 1 {
		t.Errorf("participants after double join = %d, want 1", got)
	}
}

// TestClientJoinRoomConcurrent is the B-4 regression: with the check-then-act
// sequence unlocked, a losing goroutine returned
// domain.ErrParticipantAlreadyExists instead of the documented idempotent
// success. Distinct participants racing into the same room must still all land.
func TestClientJoinRoomConcurrent(t *testing.T) {
	const (
		roomID        = "room-concurrent-join"
		goroutines    = 16
		distinctBase  = "distinct-"
		distinctCount = 8
	)
	client := newSTUNFreeClient(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Phase 1: goroutines racing for the same room AND the same participant.
	rooms := make([]*slive.Room, goroutines)
	errs := make([]error, goroutines)
	// Phase 2: same room, distinct participants.
	distinctErrs := make([]error, distinctCount)

	wg.Add(goroutines + distinctCount)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			rooms[i], errs[i] = client.JoinRoom(ctx, roomID, "same-participant")
		}(i)
	}
	for i := 0; i < distinctCount; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			_, distinctErrs[i] = client.JoinRoom(ctx, roomID, fmt.Sprintf("%s%d", distinctBase, i))
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent JoinRoom[%d] (same participant): %v", i, err)
		}
		if rooms[i] == nil {
			t.Errorf("concurrent JoinRoom[%d] (same participant): nil room", i)
			continue
		}
		if rooms[i].ID() != roomID {
			t.Errorf("concurrent JoinRoom[%d]: room ID = %q, want %q", i, rooms[i].ID(), roomID)
		}
	}
	for i, err := range distinctErrs {
		if err != nil {
			t.Errorf("concurrent JoinRoom for %s%d: %v", distinctBase, i, err)
		}
	}

	room := client.RoomManager().GetRoom(roomID)
	if room == nil {
		t.Fatalf("room %q missing after concurrent joins", roomID)
	}
	if got := len(room.Participants()); got != 1+distinctCount {
		t.Errorf("participants = %d, want %d", got, 1+distinctCount)
	}
	if room.GetParticipant("same-participant") == nil {
		t.Error("participant \"same-participant\" is not in the room after concurrent joins")
	}
}

// TestSignalingURLServesLoopbackHTTPServer pins #5: the in-process endpoint is
// a plain net/http server bound to 127.0.0.1 (the SDK no longer links
// net/http/httptest), it is started once and reused, it serves the production
// router, and Close releases the listener.
func TestSignalingURLServesLoopbackHTTPServer(t *testing.T) {
	client := newSTUNFreeClient(t)

	first, err := client.SignalingURL()
	if err != nil {
		t.Fatalf("SignalingURL: %v", err)
	}
	second, err := client.SignalingURL()
	if err != nil {
		t.Fatalf("second SignalingURL: %v", err)
	}
	if first != second {
		t.Errorf("SignalingURL restarted the server: %q then %q", first, second)
	}

	base := strings.TrimPrefix(first, "http://")
	host, port, err := net.SplitHostPort(base)
	if err != nil {
		t.Fatalf("parse base URL %q: %v", first, err)
	}
	if host != "127.0.0.1" {
		t.Errorf("signaling server host = %q, want 127.0.0.1 (loopback only)", host)
	}
	if port == "0" || port == "" {
		t.Errorf("signaling server port = %q, want an allocated port", port)
	}

	resp, err := http.Get(first + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz through SignalingURL: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz status = %d, want 200", resp.StatusCode)
	}

	// Sessions dial through the same endpoint, which proves the hand-rolled
	// server replaced httptest cleanly rather than only answering health
	// checks.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := client.Connect(ctx, "room-loopback-server", "alice")
	if err != nil {
		t.Fatalf("Connect through in-process server: %v", err)
	}
	if err := session.PublishTrack(ctx, "mic-1", slive.TrackKindAudio, slive.TrackSourceMicrophone); err != nil {
		t.Errorf("PublishTrack: %v", err)
	}
	_ = session.Close()

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The listener goes away with the server.
	if conn, dialErr := net.DialTimeout("tcp", net.JoinHostPort(host, port), time.Second); dialErr == nil {
		_ = conn.Close()
		t.Error("signaling listener still accepting connections after Close")
	}
	if _, err := client.SignalingURL(); err == nil {
		t.Error("SignalingURL on a closed client: want error, got nil")
	}
}

// TestConnectAfterCloseFails pins #7 from the caller's side: a client that is
// already closed must refuse new sessions rather than resurrect bookkeeping.
func TestConnectAfterCloseFails(t *testing.T) {
	cfg := slive.DefaultSDKConfig()
	cfg.STUNServers = []string{}
	client, err := slive.NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := client.Connect(ctx, "room-after-close", "alice")
	if err == nil {
		if session != nil {
			_ = session.Close()
		}
		t.Fatal("Connect on a closed client: want error, got nil")
	}
	if session != nil {
		t.Errorf("Connect returned a session alongside error %v", err)
	}
	// Close stays idempotent after the refusal.
	if err := client.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
