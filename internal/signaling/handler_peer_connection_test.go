package signaling

import (
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
	webrtc "github.com/sajadbayatani/slive/internal/webrtc"
)

// newTestPeerConnectionConfig returns a STUN-free configuration so any
// negotiation triggered in these tests stays deterministic and offline.
func newTestPeerConnectionConfig() webrtc.PeerConnectionConfig {
	return webrtc.PeerConnectionConfig{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	}
}

// newTestHandler creates a handler wired with the offline peer connection
// config used throughout the signaling tests.
func newTestHandler() *Handler {
	return NewHandler(NewRoomManager(), WithPeerConnectionConfig(newTestPeerConnectionConfig()))
}

// joinParticipant creates and joins a participant in a room owned by the
// handler's room manager, mirroring the join branch of handleConnection.
func joinParticipant(t *testing.T, h *Handler, roomID, participantID string) (*domain.Room, *domain.Participant) {
	t.Helper()

	room, err := h.roomManager.GetOrCreateRoom(roomID)
	if err != nil {
		t.Fatalf("GetOrCreateRoom: %v", err)
	}

	participant := domain.NewParticipant(participantID, "User "+participantID)
	if err := room.Join(participant); err != nil {
		t.Fatalf("Join: %v", err)
	}
	participant.SetRoom(room)

	return room, participant
}

// channelSender returns a SignalingSender that records message types on the
// given channel, allowing tests to observe where outbound events land.
func channelSender(sink chan string) webrtc.SignalingSender {
	return func(msgType string, _ interface{}) error {
		select {
		case sink <- msgType:
			return nil
		default:
			return nil // drop if the sink is full; tests only need signals
		}
	}
}

// TestEnsurePeerConnectionCreatesAndReuses covers the join path (fresh
// connection) and the reconnect path (same instance reused, sender swapped).
func TestEnsurePeerConnectionCreatesAndReuses(t *testing.T) {
	h := newTestHandler()
	_, participant := joinParticipant(t, h, "reuse-room", "p-1")

	senderA := make(chan string, 8)
	pc1, err := h.ensurePeerConnection(participant, channelSender(senderA))
	if err != nil {
		t.Fatalf("ensurePeerConnection (create): %v", err)
	}
	if pc1 == nil {
		t.Fatal("expected a peer connection")
	}
	if pc1.State() != webrtc.PeerConnectionStateNew {
		t.Errorf("fresh PC state = %s, want new", pc1.State())
	}

	// Rejoin: the same instance must be reused, only the sender swapped.
	senderB := make(chan string, 8)
	pc2, err := h.ensurePeerConnection(participant, channelSender(senderB))
	if err != nil {
		t.Fatalf("ensurePeerConnection (rejoin): %v", err)
	}
	if pc2 != pc1 {
		t.Fatal("expected the existing peer connection instance to be reused")
	}

	// Prove the swap took effect: adding a transceiver fires
	// negotiation-needed, which pushes an offer plus its gathered ICE
	// candidates through the most recent sender. Candidates arrive while
	// gathering runs, so drain until the offer itself shows up.
	// STUN-free config keeps everything offline and deterministic.
	if _, err := pc2.PionPeerConnection().AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("AddTransceiverFromKind: %v", err)
	}

	gotOffer := false
	deadline := time.After(5 * time.Second)
	for !gotOffer {
		select {
		case msgType := <-senderB:
			if msgType == "webrtc:offer" {
				gotOffer = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for offer on the swapped-in sender")
		}
	}

	select {
	case msgType := <-senderA:
		t.Errorf("stale sender unexpectedly received %q", msgType)
	default:
		// Nothing on the stale sender is exactly right.
	}
}

// TestEnsurePeerConnectionReplaceAfterClose ensures unusable connections are
// replaced by fresh ones and the old instance is closed.
func TestEnsurePeerConnectionReplaceAfterClose(t *testing.T) {
	h := newTestHandler()
	_, participant := joinParticipant(t, h, "replace-room", "p-2")

	pc1, err := h.ensurePeerConnection(participant, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection (create): %v", err)
	}
	if err := pc1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	pc2, err := h.ensurePeerConnection(participant, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection (replace): %v", err)
	}

	if pc2 == pc1 {
		t.Fatal("expected a fresh peer connection after the old one closed")
	}
	if state := pc1.State(); state != webrtc.PeerConnectionStateClosed {
		t.Errorf("old PC state = %s, want closed", state)
	}
	if state := pc2.State(); state != webrtc.PeerConnectionStateNew {
		t.Errorf("replacement PC state = %s, want new", state)
	}

	h.peerConnectionsMutex.RLock()
	stored := h.peerConnections[participant.ID()]
	h.peerConnectionsMutex.RUnlock()
	if stored != pc2 {
		t.Error("registry must reference the replacement peer connection")
	}
}

// TestWithPeerConnectionConfigInjection verifies that the configured
// PeerConnectionConfig is actually used when creating peer connections, and
// that the zero-option constructor falls back to the default config.
func TestWithPeerConnectionConfigInjection(t *testing.T) {
	cfg := newTestPeerConnectionConfig()
	cfg.ICEServers = webrtc.ICEServersFromURLs([]string{"stun:test.slive.local:3478"}, nil)

	h := NewHandler(NewRoomManager(), WithPeerConnectionConfig(cfg))
	_, injectedParticipant := joinParticipant(t, h, "cfg-room", "p-cfg")

	pc, err := h.ensurePeerConnection(injectedParticipant, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection: %v", err)
	}
	got := pc.Config()
	if len(got.ICEServers) != 1 || len(got.ICEServers[0].URLs) != 1 ||
		got.ICEServers[0].URLs[0] != "stun:test.slive.local:3478" {
		t.Errorf("injected config not used, ICEServers = %+v", got.ICEServers)
	}

	// Without options the default configuration applies.
	fallback := NewHandler(NewRoomManager())
	_, defaultParticipant := joinParticipant(t, fallback, "cfg-room", "p-default")

	pcDefault, err := fallback.ensurePeerConnection(defaultParticipant, channelSender(make(chan string, 8)))
	if err != nil {
		t.Fatalf("ensurePeerConnection (default): %v", err)
	}
	want := webrtc.DefaultPeerConnectionConfig()
	if len(pcDefault.Config().ICEServers) != len(want.ICEServers) {
		t.Errorf("default config fallback failed, ICEServers = %+v", pcDefault.Config().ICEServers)
	}
}
