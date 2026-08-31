package sdk

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

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
