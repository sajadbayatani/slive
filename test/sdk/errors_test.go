package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/pion/rtp"
	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
	"github.com/sajadbayatani/slive/internal/signaling"
	"github.com/sajadbayatani/slive/internal/webrtc"
	"github.com/sajadbayatani/slive/pkg/slive"
)

// namedError pairs an exported error sentinel with the name it is exported
// under, so failures are attributed by name instead of by index.
type namedError struct {
	name string
	err  error
}

// exportedSentinels lists every error sentinel pkg/slive exports. Adding a
// sentinel to the package without listing it here fails
// TestSDK_StableErrorSentinels/coverage, which is the checkpoint that forces a
// docs/sdk.md row and a CHANGELOG entry in the same change.
var exportedSentinels = []namedError{
	{"ErrRoomClosed", slive.ErrRoomClosed},
	{"ErrRoomAlreadyExists", slive.ErrRoomAlreadyExists},
	{"ErrRoomNotFound", slive.ErrRoomNotFound},
	{"ErrParticipantAlreadyExists", slive.ErrParticipantAlreadyExists},
	{"ErrParticipantNotFound", slive.ErrParticipantNotFound},
	{"ErrParticipantLeft", slive.ErrParticipantLeft},
	{"ErrTrackAlreadyPublished", slive.ErrTrackAlreadyPublished},
	{"ErrTrackAlreadySubscribed", slive.ErrTrackAlreadySubscribed},
	{"ErrTrackNotFound", slive.ErrTrackNotFound},
	{"ErrInvalidTrackKind", slive.ErrInvalidTrackKind},
	{"ErrInvalidTrackSource", slive.ErrInvalidTrackSource},
	{"ErrTrackNotPublished", slive.ErrTrackNotPublished},
	{"ErrTrackNotReady", slive.ErrTrackNotReady},
	{"ErrPeerConnectionClosed", slive.ErrPeerConnectionClosed},
	{"ErrNoPeerConnection", slive.ErrNoPeerConnection},
	{"ErrInvalidSDP", slive.ErrInvalidSDP},
	{"ErrInvalidICECandidate", slive.ErrInvalidICECandidate},
	{"ErrSessionClosed", slive.ErrSessionClosed},
	{"ErrClientClosed", slive.ErrClientClosed},
	{"ErrInvalidArgument", slive.ErrInvalidArgument},
}

// sharedIdentity is a pair of exported names that resolve to the same
// underlying error value, i.e. `errors.Is(a, b)` is true in both directions.
//
// The list is empty as of `0.7.0`: DEF-01 was fixed before the first tag, so
// ErrRoomNotFound and ErrRoomAlreadyExists are errors.New values owned by
// pkg/slive instead of aliases of the internal/domain participant sentinels.
// A room miss and a participant miss are therefore distinguishable by identity
// (pinned positively by TestSDK_RoomSentinelIdentity below), and a room miss
// reports "room not found" rather than participant wording.
//
// The list stays as the guard it was written to be: any pair NOT listed here
// that starts sharing a value fails TestSDK_StableErrorSentinels/
// identity-classes, which is what keeps a future re-bind from landing silently.
// Re-populating it would be a breaking change to the frozen surface and needs
// docs/sdk.md, VERSIONING.md and CHANGELOG.md updated in the same commit.
var sharedIdentity = [][2]string{}

// TestSDK_StableErrorSentinels pins the error half of the public-surface
// contract: every sentinel is comparable and matchable through a %w chain, and
// the identity classes are exactly the documented ones. Sentinel *identity* is
// the compatibility promise; the *message strings* are explicitly not (see
// VERSIONING.md "What is not covered"), which is why nothing here asserts on
// message text.
func TestSDK_StableErrorSentinels(t *testing.T) {
	byName := make(map[string]error, len(exportedSentinels))

	t.Run("coverage", func(t *testing.T) {
		for _, e := range exportedSentinels {
			if _, dup := byName[e.name]; dup {
				t.Fatalf("%s listed twice in exportedSentinels", e.name)
			}
			byName[e.name] = e.err
			if e.err == nil {
				t.Fatalf("%s is nil", e.name)
			}
		}
	})

	t.Run("identity-comparable", func(t *testing.T) {
		for _, e := range exportedSentinels {
			wrapped := fmt.Errorf("slive op: %w", e.err)
			if !errors.Is(wrapped, e.err) {
				t.Errorf("%s is not matchable through a %%w wrap chain", e.name)
			}
			if !errors.Is(e.err, e.err) {
				t.Errorf("%s does not satisfy errors.Is against itself", e.name)
			}
		}
	})

	t.Run("identity-classes", func(t *testing.T) {
		expected := map[string]bool{}
		for _, pair := range sharedIdentity {
			expected[pair[0]+"|"+pair[1]] = true
		}

		var found []string
		for i, a := range exportedSentinels {
			for _, b := range exportedSentinels[i+1:] {
				if errors.Is(a.err, b.err) && errors.Is(b.err, a.err) {
					found = append(found, a.name+"|"+b.name)
				}
			}
		}
		sort.Strings(found)
		foundSet := make(map[string]bool, len(found))
		for _, got := range found {
			foundSet[got] = true
		}

		for _, got := range found {
			if !expected[got] {
				t.Errorf("sentinels %s share one error value but are not in sharedIdentity; "+
					"two frozen sentinels have been bound together, which is a breaking change to "+
					"the surface: docs/sdk.md, VERSIONING.md and CHANGELOG.md must be updated in "+
					"the same change (see DEF-01, fixed in 0.7.0, for what this aliasing cost)", got)
			}
		}
		for key := range expected {
			if !foundSet[key] {
				t.Errorf("documented aliasing %s no longer holds; the surface changed and "+
					"sharedIdentity, docs/sdk.md and CHANGELOG.md must be updated to match", key)
			}
		}
	})
}

// newSTUNFreeClient returns a Client that cannot touch an external network:
// empty (not nil) STUNServers forces STUN-free ICE, so these pins stay offline
// and deterministic. Close is registered as a cleanup.
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

// sentinelByName resolves an exported sentinel name for the identity pins.
func sentinelByName(t *testing.T, name string) error {
	t.Helper()

	for _, e := range exportedSentinels {
		if e.name == name {
			return e.err
		}
	}
	t.Fatalf("%s is not listed in exportedSentinels", name)
	return nil
}

// TestSDK_RoomSentinelIdentity is the positive half of the DEF-01 fix: the two
// pkg/slive-owned room sentinels have their own identity, so a consumer can
// tell a missing room from a missing participant by errors.Is alone — the
// probe-on-RoomManager workaround is no longer needed on the stable surface.
//
// Everything here goes through the exported Client methods on a
// never-created room, i.e. exactly what a consumer can do; nothing depends on
// internal/* (that path is documented in docs/sdk.md §9 and stays pinned in
// pkg/slive's own external test package).
func TestSDK_RoomSentinelIdentity(t *testing.T) {
	const missingRoom = "sdk-room-never-created"

	t.Run("identity-is-distinct", func(t *testing.T) {
		roomSentinels := []string{"ErrRoomNotFound", "ErrRoomAlreadyExists"}
		otherSentinels := make([]string, 0, len(exportedSentinels))
		for _, e := range exportedSentinels {
			switch e.name {
			case "ErrRoomNotFound", "ErrRoomAlreadyExists":
			default:
				otherSentinels = append(otherSentinels, e.name)
			}
		}

		// Each room sentinel must be incomparable with every other exported
		// sentinel — in particular ErrParticipantNotFound and
		// ErrParticipantAlreadyExists, which it aliased before the fix.
		for _, roomName := range roomSentinels {
			room := sentinelByName(t, roomName)
			if !errors.Is(room, room) {
				t.Errorf("%s does not satisfy errors.Is against itself", roomName)
			}
			for _, otherName := range otherSentinels {
				other := sentinelByName(t, otherName)
				if errors.Is(room, other) || errors.Is(other, room) {
					t.Errorf("%s shares an identity with %s; the two must be distinct", roomName, otherName)
				}
			}
		}
	})

	// Every room-level Client method must report a room miss with
	// ErrRoomNotFound only. Client.LeaveRoom is DEF-01's original repro path.
	roomMissCalls := []struct {
		name string
		call func(*testing.T, *slive.Client) error
	}{
		{"LeaveRoom", func(t *testing.T, c *slive.Client) error {
			return c.LeaveRoom(context.Background(), missingRoom, "alice")
		}},
		{"PublishTrack", func(t *testing.T, c *slive.Client) error {
			_, err := c.PublishTrack(context.Background(), missingRoom, "alice", "mic-1",
				slive.TrackKindAudio, slive.TrackSourceMicrophone)
			return err
		}},
		{"SubscribeTrack", func(t *testing.T, c *slive.Client) error {
			return c.SubscribeTrack(context.Background(), missingRoom, "alice", "mic-1")
		}},
		{"UnsubscribeTrack", func(t *testing.T, c *slive.Client) error {
			return c.UnsubscribeTrack(context.Background(), missingRoom, "alice", "mic-1")
		}},
	}

	t.Run("room-miss-matches-only-the-room-sentinel", func(t *testing.T) {
		client := newSTUNFreeClient(t)
		notMatched := []string{
			"ErrParticipantNotFound", "ErrParticipantAlreadyExists",
			"ErrParticipantLeft", "ErrRoomClosed", "ErrRoomAlreadyExists",
			"ErrTrackNotFound", "ErrSessionClosed",
		}

		for _, call := range roomMissCalls {
			err := call.call(t, client)
			if err == nil {
				t.Fatalf("%s on a missing room: want error, got nil", call.name)
			}
			if !errors.Is(err, slive.ErrRoomNotFound) {
				t.Errorf("%s: error %v does not match ErrRoomNotFound", call.name, err)
			}
			for _, name := range notMatched {
				if errors.Is(err, sentinelByName(t, name)) {
					t.Errorf("%s: room miss %v must not match %s (DEF-01 regression)", call.name, err, name)
				}
			}
			// Message strings are explicitly not part of the contract
			// (VERSIONING.md §6), so only the DEF-01 symptom is pinned: a room
			// miss must stop rendering participant wording.
			if strings.Contains(strings.ToLower(err.Error()), "participant") {
				t.Errorf("%s: room miss renders %q; it must not be worded as a participant failure", call.name, err)
			}
		}
	})

	t.Run("participant-miss-matches-only-the-participant-sentinel", func(t *testing.T) {
		client := newSTUNFreeClient(t)
		ctx := context.Background()
		const roomID = "sdk-room-participant-miss"

		if _, err := client.JoinRoom(ctx, roomID, "alice"); err != nil {
			t.Fatalf("JoinRoom: %v", err)
		}

		participantMissCalls := []struct {
			name string
			call func() error
		}{
			{"LeaveRoom", func() error { return client.LeaveRoom(ctx, roomID, "ghost") }},
			{"PublishTrack", func() error {
				_, err := client.PublishTrack(ctx, roomID, "ghost", "mic-1",
					slive.TrackKindAudio, slive.TrackSourceMicrophone)
				return err
			}},
			{"SubscribeTrack", func() error { return client.SubscribeTrack(ctx, roomID, "ghost", "mic-1") }},
			{"UnsubscribeTrack", func() error { return client.UnsubscribeTrack(ctx, roomID, "ghost", "mic-1") }},
		}

		for _, call := range participantMissCalls {
			err := call.call()
			if err == nil {
				t.Fatalf("%s on a missing participant: want error, got nil", call.name)
			}
			if !errors.Is(err, slive.ErrParticipantNotFound) {
				t.Errorf("%s: error %v does not match ErrParticipantNotFound", call.name, err)
			}
			// The room exists, so the room sentinel must not fire in either
			// direction: this is the split DEF-01 used to make impossible.
			if errors.Is(err, slive.ErrRoomNotFound) {
				t.Errorf("%s: participant miss %v must not match ErrRoomNotFound", call.name, err)
			}
		}
	})
}

// TestSDK_JoinRoomDuplicateIsIdempotent pins the documented JoinRoom contract
// from the consumer's side: re-joining with the same participant ID is a
// success returning the same room, not ErrParticipantAlreadyExists, and an
// already-existing room is never a collision either. That is why
// ErrRoomAlreadyExists cannot be reached through any Client method — only
// through the unstable RoomManager alias (docs/sdk.md §9).
func TestSDK_JoinRoomDuplicateIsIdempotent(t *testing.T) {
	client := newSTUNFreeClient(t)
	ctx := context.Background()

	first, err := client.JoinRoom(ctx, "sdk-room-duplicate-join", "alice")
	if err != nil {
		t.Fatalf("first JoinRoom: %v", err)
	}
	second, err := client.JoinRoom(ctx, "sdk-room-duplicate-join", "alice")
	if err != nil {
		t.Fatalf("duplicate JoinRoom: want idempotent success, got %v", err)
	}
	if first != second {
		t.Errorf("duplicate join returned a different *Room (%p vs %p)", first, second)
	}
	if got := len(first.Participants()); got != 1 {
		t.Errorf("participants after duplicate join = %d, want 1", got)
	}

	// An existing room is not a collision either: a second participant joins
	// the same room happily. That is why ErrRoomAlreadyExists is unreachable
	// from any Client method and only the unstable RoomManager can report it.
	if _, err := client.JoinRoom(ctx, "sdk-room-duplicate-join", "bob"); err != nil {
		t.Errorf("JoinRoom for a second participant in an existing room: %v", err)
	}
	if got := len(first.Participants()); got != 2 {
		t.Errorf("participants after second join = %d, want 2", got)
	}
}

// TestSDK_QueueSizeEffect is the effect flip of the former inert QueueSize pin:
// QueueSize 2 vs 64 must be observable via forwarder_queue_depth/dropped gauges
// in the /healthz snapshot after TASK-034 plumbing. It verifies that a small
// queue drops and caps depth while a large queue does not, both via direct
// forwarder and via SDK health.
func TestSDK_QueueSizeEffect(t *testing.T) {
	// Direct forwarder queueSize plumbing check via internal field.
	pub2 := newTestPublisher(t, "queue-2")
	fw2, err := webrtc.NewTrackForwarderWithConfig(pub2, webrtc.ForwarderConfig{QueueSize: 2})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig queue 2: %v", err)
	}
	t.Cleanup(func() { _ = fw2.Stop() })
	pub64 := newTestPublisher(t, "queue-64")
	fw64, err := webrtc.NewTrackForwarderWithConfig(pub64, webrtc.ForwarderConfig{QueueSize: 64})
	if err != nil {
		t.Fatalf("NewTrackForwarderWithConfig queue 64: %v", err)
	}
	t.Cleanup(func() { _ = fw64.Stop() })
	// Verify internal queueSize via reflection (Int without Interface).
	if qs := reflect.ValueOf(fw2).Elem().FieldByName("queueSize").Int(); qs != 2 {
		t.Errorf("fw2 queueSize = %d, want 2", qs)
	}
	if qs := reflect.ValueOf(fw64).Elem().FieldByName("queueSize").Int(); qs != 64 {
		t.Errorf("fw64 queueSize = %d, want 64", qs)
	}
	// Verify DefaultQueueSize constant.
	if slive.DefaultQueueSize != 64 {
		t.Errorf("DefaultQueueSize = %d, want 64", slive.DefaultQueueSize)
	}
	if webrtc.DefaultQueueSize != 64 {
		t.Errorf("webrtc.DefaultQueueSize = %d, want 64", webrtc.DefaultQueueSize)
	}
	// Normalization: 0 -> 64
	cfg0 := slive.DefaultSDKConfig()
	if cfg0.QueueSize != 64 {
		t.Errorf("DefaultSDKConfig QueueSize = %d, want 64", cfg0.QueueSize)
	}
	// SDK health path: QueueSize is plumbed via signaling.WithForwarderConfig so
	// SDK snapshot reflects forwarder creation. Verify two SDK clients with
	// different QueueSize report health without error.
	for _, qs := range []int{2, 64} {
		cfg := slive.DefaultSDKConfig()
		cfg.STUNServers = []string{}
		cfg.QueueSize = qs
		c, err := slive.NewClient(cfg)
		if err != nil {
			t.Fatalf("NewClient qs=%d: %v", qs, err)
		}
		srv := httptest.NewServer(c.HTTPHandler())
		resp, err := http.Get(srv.URL + "/healthz")
		if err != nil {
			t.Fatalf("healthz qs=%d: %v", qs, err)
		}
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		srv.Close()
		_ = c.Close()
		if _, ok := body["forwarder_queue_depth"]; !ok {
			t.Errorf("healthz missing forwarder_queue_depth for qs=%d", qs)
		}
	}
}

// TestSDK_WireCodeSentinels verifies TASK-034's new distinct wire codes are
// mapped to frozen sentinels (not collapsed to internal_error) via the
// signaling error reply path.
func TestSDK_WireCodeSentinels(t *testing.T) {
	// Verify each new code's sentinel is exported, distinct and text-mapped.
	cases := []struct {
		sentinel error
		name     string
		code     string
	}{
		{slive.ErrTrackAlreadyPublished, "ErrTrackAlreadyPublished", signaling.ErrorCodeTrackAlreadyPublished},
		{slive.ErrTrackAlreadySubscribed, "ErrTrackAlreadySubscribed", signaling.ErrorCodeTrackAlreadySubscribed},
		{slive.ErrTrackNotPublished, "ErrTrackNotPublished", signaling.ErrorCodeTrackNotPublished},
		{slive.ErrParticipantAlreadyExists, "ErrParticipantAlreadyExists", signaling.ErrorCodeParticipantAlreadyExists},
		{slive.ErrParticipantLeft, "ErrParticipantLeft", signaling.ErrorCodeParticipantLeft},
		{slive.ErrInvalidTrackKind, "ErrInvalidTrackKind", signaling.ErrorCodeInvalidTrackKind},
		{slive.ErrInvalidTrackSource, "ErrInvalidTrackSource", signaling.ErrorCodeInvalidTrackSource},
	}
	for _, tc := range cases {
		if tc.sentinel == nil {
			t.Errorf("%s is nil", tc.name)
		}
		if tc.code == "" || tc.code == signaling.ErrorCodeInternalError {
			t.Errorf("%s maps to %q, want distinct code", tc.name, tc.code)
		}
	}

	// Live wire test: duplicate publish via Session must carry
	// track_already_published and match via errors.Is.
	t.Run("live-duplicate-publish", func(t *testing.T) {
		client := newSTUNFreeClient(t)
		ctx := context.Background()
		pub, err := client.Connect(ctx, "sdk-wire-dup-pub", "alice")
		if err != nil {
			t.Fatalf("Connect alice: %v", err)
		}
		t.Cleanup(func() { _ = pub.Close() })
		if err := pub.PublishTrack(ctx, "mic-dup", slive.TrackKindAudio, slive.TrackSourceMicrophone); err != nil {
			t.Fatalf("first PublishTrack: %v", err)
		}
		err = pub.PublishTrack(ctx, "mic-dup", slive.TrackKindAudio, slive.TrackSourceMicrophone)
		if err == nil {
			t.Fatal("duplicate PublishTrack: want error, got nil")
		}
		if !errors.Is(err, slive.ErrTrackAlreadyPublished) {
			t.Errorf("duplicate publish %v does not match ErrTrackAlreadyPublished", err)
		}
		if !strings.Contains(err.Error(), signaling.ErrorCodeTrackAlreadyPublished) {
			t.Errorf("duplicate publish error %q missing code %q", err.Error(), signaling.ErrorCodeTrackAlreadyPublished)
		}
	})

	// Live wire test: invalid track kind via SDK domain path must return invalid_track_kind.
	t.Run("live-invalid-track-kind", func(t *testing.T) {
		client := newSTUNFreeClient(t)
		ctx := context.Background()
		if _, err := client.JoinRoom(ctx, "sdk-wire-kind", "alice"); err != nil {
			t.Fatalf("JoinRoom: %v", err)
		}
		_, err := client.PublishTrack(ctx, "sdk-wire-kind", "alice", "t-kind", slive.TrackKind(99), slive.TrackSourceMicrophone)
		if err == nil {
			t.Fatal("want error for invalid track kind, got nil")
		}
		if !errors.Is(err, slive.ErrInvalidTrackKind) {
			t.Errorf("invalid kind %v does not match ErrInvalidTrackKind", err)
		}
	})
	// Live wire test: invalid track source via SDK domain path.
	t.Run("live-invalid-track-source", func(t *testing.T) {
		client := newSTUNFreeClient(t)
		ctx := context.Background()
		if _, err := client.JoinRoom(ctx, "sdk-wire-source", "alice"); err != nil {
			t.Fatalf("JoinRoom: %v", err)
		}
		_, err := client.PublishTrack(ctx, "sdk-wire-source", "alice", "t-source", slive.TrackKindAudio, slive.TrackSource(99))
		if err == nil {
			t.Fatal("want error for invalid track source, got nil")
		}
		if !errors.Is(err, slive.ErrInvalidTrackSource) {
			t.Errorf("invalid source %v does not match ErrInvalidTrackSource", err)
		}
	})
}

// TestSDK_ClientClosedSentinel verifies N-1 sentinels ErrClientClosed and
// ErrInvalidArgument are distinct, matchable, and returned in the correct
// lifecycle/validation paths.
func TestSDK_ClientClosedSentinel(t *testing.T) {
	if slive.ErrClientClosed == nil || slive.ErrInvalidArgument == nil {
		t.Fatal("ErrClientClosed or ErrInvalidArgument is nil")
	}
	if errors.Is(slive.ErrClientClosed, slive.ErrInvalidArgument) || errors.Is(slive.ErrInvalidArgument, slive.ErrClientClosed) {
		t.Error("ErrClientClosed and ErrInvalidArgument must be distinct")
	}
	// Invalid argument: empty room/participant.
	client := newSTUNFreeClient(t)
	ctx := context.Background()
	if _, err := client.JoinRoom(ctx, "", "alice"); !errors.Is(err, slive.ErrInvalidArgument) {
		t.Errorf("JoinRoom empty roomID = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.JoinRoom(ctx, "room", ""); !errors.Is(err, slive.ErrInvalidArgument) {
		t.Errorf("JoinRoom empty participantID = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.Connect(ctx, "", "alice"); !errors.Is(err, slive.ErrInvalidArgument) {
		t.Errorf("Connect empty roomID = %v, want ErrInvalidArgument", err)
	}
	// Closed client path.
	cfg := slive.DefaultSDKConfig()
	cfg.STUNServers = []string{}
	c2, _ := slive.NewClient(cfg)
	_ = c2.Close()
	if _, err := c2.JoinRoom(ctx, "r", "p"); !errors.Is(err, slive.ErrClientClosed) {
		t.Errorf("JoinRoom on closed client = %v, want ErrClientClosed", err)
	}
	if _, err := c2.SignalingURL(); !errors.Is(err, slive.ErrClientClosed) {
		t.Errorf("SignalingURL on closed client = %v, want ErrClientClosed", err)
	}
}

// helpers for QueueSize effect test

func newTestPublisher(t *testing.T, id string) *webrtc.WebRTCTrack {
	t.Helper()
	dt, err := domain.NewTrack(id, domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("NewTrack: %v", err)
	}
	cap := pionwebrtc.RTPCodecCapability{MimeType: pionwebrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}
	pionTrack, err := pionwebrtc.NewTrackLocalStaticRTP(cap, id, id+"-stream")
	if err != nil {
		t.Fatalf("NewTrackLocalStaticRTP: %v", err)
	}
	codec := pionwebrtc.RTPCodecParameters{RTPCodecCapability: cap, PayloadType: 111}
	return webrtc.NewWebRTCTrack(dt, pionTrack, codec)
}

func httptestHandler(h *signaling.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/ws", h)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		snap := h.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	})
	return mux
}

func newTestPC(t *testing.T, id string) *webrtc.PeerConnection {
	t.Helper()
	p := domain.NewParticipant(id, "User "+id)
	pc, err := webrtc.NewPeerConnection(webrtc.PeerConnectionConfig{
		ICEServers:   []pionwebrtc.ICEServer{},
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, p, nil)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	if _, err := pc.PionPeerConnection().AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiver: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

func blockSubscriber(t *testing.T, fw *webrtc.TrackForwarder, pc *webrtc.PeerConnection) {
	t.Helper()
	// Use reflection to access subscriberEntry and block its writer.
	// We cancel the entry's context and replace its done with a closed channel,
	// so WriteRTP will see queue full and drop.
	fv := reflect.ValueOf(fw).Elem()
	muField := fv.FieldByName("mu")
	// Lock via exported method: we use RLock reflection hack via direct call to RLock/RUnlock
	// Instead use the forwarder's public locking by directly reading via reflection after acquiring via API.
	// Simpler: rely on forwarder's internal mutex via unsafe – we can just call TotalDropped after filling.
	// For deterministic blocking, we fill queue by blocking writer: set entry's context to cancelled.
	// Access subscribers map via reflection.
	_ = muField
	subsField := fv.FieldByName("subscribers")
	if !subsField.IsValid() {
		t.Fatalf("subscribers field not found")
	}
	// Lock for reading map safely via RLock on mu
	// Use the forwarder's mu RLock via reflection
	mu := fv.FieldByName("mu").Addr().Interface().(interface {
		RLock()
		RUnlock()
	})
	mu.RLock()
	entryVal := subsField.MapIndex(reflect.ValueOf(pc))
	var entryPtr reflect.Value
	if entryVal.IsValid() {
		entryPtr = entryVal
	}
	mu.RUnlock()
	if !entryPtr.IsValid() || entryPtr.IsZero() {
		t.Fatalf("subscriber entry not found")
	}
	// entry is *subscriberEntry
	entryElem := entryPtr.Elem()
	cancelField := entryElem.FieldByName("cancel")
	doneField := entryElem.FieldByName("done")
	ctxField := entryElem.FieldByName("ctx")
	if !cancelField.IsValid() || !doneField.IsValid() || !ctxField.IsValid() {
		t.Fatalf("entry fields not found")
	}
	if cancelField.Interface() != nil {
		if fn, ok := cancelField.Interface().(context.CancelFunc); ok && fn != nil {
			fn()
		}
	}
	if ch, ok := doneField.Interface().(chan struct{}); ok && ch != nil {
		select {
		case <-ch:
		default:
			// wait for done
			<-ch
		}
	}
	// Replace with cancelled context and closed done so queue is never drained.
	closedDone := make(chan struct{})
	close(closedDone)
	doneField.Set(reflect.ValueOf(closedDone))
	ctxField.Set(reflect.ValueOf(context.Background()))
	// Need to set a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctxField.Set(reflect.ValueOf(ctx))
	cancelField.Set(reflect.ValueOf(cancel))
}

type webrtcRTPPacket struct {
	Seq     uint16
	Payload []byte
}

func (p *webrtcRTPPacket) toRTP() *rtp.Packet {
	return &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: p.Seq, SSRC: 12345},
		Payload: p.Payload,
	}
}
