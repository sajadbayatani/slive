package webrtc

import (
	"strings"
	"testing"

	"github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// newNegotiationTestPeerConnection returns a peer connection suitable for SDP
// negotiation tests: no external ICE servers (keeps gathering fast and
// deterministic offline) and at least one media section, without which pion
// generates offers that carry no ICE credentials and cannot be applied remotely.
func newNegotiationTestPeerConnection(t *testing.T, id, name string) *PeerConnection {
	t.Helper()

	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, domain.NewParticipant(id, name), nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })

	if _, err := pc.PionPeerConnection().AddTransceiverFromKind(webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("Failed to add transceiver: %v", err)
	}

	return pc
}

func TestNewPeerConnection(t *testing.T) {
	// Create a participant
	participant := domain.NewParticipant("participant-1", "Alice")

	// Create a peer connection config
	config := DefaultPeerConnectionConfig()

	// Create a new peer connection
	pc, err := NewPeerConnection(config, participant, nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}
	defer pc.Close()

	// Verify the peer connection was created correctly
	if pc.Participant() != participant {
		t.Error("Expected participant to match")
	}

	if pc.State() != PeerConnectionStateNew {
		t.Errorf("Expected state to be 'new', got '%s'", pc.State())
	}

	if pc.PionPeerConnection() == nil {
		t.Error("Expected Pion peer connection to be set")
	}
}

func TestPeerConnectionStateString(t *testing.T) {
	tests := []struct {
		state    PeerConnectionState
		expected string
	}{
		{PeerConnectionStateNew, "new"},
		{PeerConnectionStateConnecting, "connecting"},
		{PeerConnectionStateConnected, "connected"},
		{PeerConnectionStateDisconnected, "disconnected"},
		{PeerConnectionStateFailed, "failed"},
		{PeerConnectionStateClosed, "closed"},
		{PeerConnectionState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("PeerConnectionState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestDefaultPeerConnectionConfig(t *testing.T) {
	config := DefaultPeerConnectionConfig()

	// Verify default ICE servers
	if len(config.ICEServers) == 0 {
		t.Error("Expected at least one ICE server in default config")
	}

	// Verify default SDP semantics
	if config.SDPSemantics != webrtc.SDPSemanticsUnifiedPlanWithFallback {
		t.Errorf("Expected SDP semantics to be UnifiedPlanWithFallback, got %v", config.SDPSemantics)
	}
}

func TestPeerConnectionAddTrack(t *testing.T) {
	participant := domain.NewParticipant("participant-1", "Alice")
	config := DefaultPeerConnectionConfig()

	pc, err := NewPeerConnection(config, participant, nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}
	defer pc.Close()

	// Create a domain track
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}

	// Create a Pion track
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio-1",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	// Create a WebRTCTrack
	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})

	// Add the track to the peer connection
	err = pc.AddTrack(webRTCTrack)
	if err != nil {
		t.Fatalf("Failed to add track: %v", err)
	}

	// Verify the track was added
	if pc.GetLocalTrack("audio-1") != webRTCTrack {
		t.Error("Expected track to be added")
	}
}

func TestPeerConnectionRemoveTrack(t *testing.T) {
	participant := domain.NewParticipant("participant-1", "Alice")
	config := DefaultPeerConnectionConfig()

	pc, err := NewPeerConnection(config, participant, nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}
	defer pc.Close()

	// Create and add a track
	domainTrack, err := domain.NewTrack("audio-1", domain.TrackKindAudio, domain.TrackSourceMicrophone)
	if err != nil {
		t.Fatalf("Failed to create domain track: %v", err)
	}
	pionTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio-1",
		"webrtc-audio",
	)
	if err != nil {
		t.Fatalf("Failed to create Pion track: %v", err)
	}

	webRTCTrack := NewWebRTCTrack(domainTrack, pionTrack, webrtc.RTPCodecParameters{})
	if err := pc.AddTrack(webRTCTrack); err != nil {
		t.Fatalf("Failed to add track: %v", err)
	}

	// Remove the track
	err = pc.RemoveTrack("audio-1")
	if err != nil {
		t.Fatalf("Failed to remove track: %v", err)
	}

	// Verify the track was removed
	if pc.GetLocalTrack("audio-1") != nil {
		t.Error("Expected track to be removed")
	}
}

func TestPeerConnectionGetTrackNotFound(t *testing.T) {
	participant := domain.NewParticipant("participant-1", "Alice")
	config := DefaultPeerConnectionConfig()

	pc, err := NewPeerConnection(config, participant, nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}
	defer pc.Close()

	// Get a non-existent track
	if pc.GetLocalTrack("non-existent") != nil {
		t.Error("Expected nil for non-existent track")
	}

	if pc.GetRemoteTrack("non-existent") != nil {
		t.Error("Expected nil for non-existent remote track")
	}
}

func TestPeerConnectionCreateOffer(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "participant-1", "Alice")

	// Create an offer
	offer, err := pc.CreateOffer()
	if err != nil {
		t.Fatalf("Failed to create offer: %v", err)
	}

	// Verify the offer was created
	if offer.Type() != webrtc.SDPTypeOffer {
		t.Errorf("Expected offer type to be 'offer', got '%s'", offer.Type())
	}

	if offer.SDP() == "" {
		t.Error("Expected SDP to be non-empty")
	}

	// The offer must carry ICE credentials to be applicable by a remote peer
	if !strings.Contains(offer.SDP(), "a=ice-ufrag") {
		t.Error("Expected offer SDP to contain ice-ufrag")
	}
}

func TestPeerConnectionCreateAnswer(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "participant-1", "Alice")

	// Create an offer first
	offer, err := pc.CreateOffer()
	if err != nil {
		t.Fatalf("Failed to create offer: %v", err)
	}

	// Create a new peer connection for the answerer
	pc2 := newNegotiationTestPeerConnection(t, "participant-2", "Bob")

	// Create an answer
	answer, err := pc2.CreateAnswer(offer)
	if err != nil {
		t.Fatalf("Failed to create answer: %v", err)
	}

	// Verify the answer was created
	if answer.Type() != webrtc.SDPTypeAnswer {
		t.Errorf("Expected answer type to be 'answer', got '%s'", answer.Type())
	}

	if answer.SDP() == "" {
		t.Error("Expected SDP to be non-empty")
	}
}

func TestPeerConnectionSetRemoteDescription(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "participant-1", "Alice")

	// Create an offer
	offer, err := pc.CreateOffer()
	if err != nil {
		t.Fatalf("Failed to create offer: %v", err)
	}

	// Create a new peer connection for the answerer
	pc2 := newNegotiationTestPeerConnection(t, "participant-2", "Bob")

	// Set the remote description on pc2
	err = pc2.SetRemoteDescription(offer)
	if err != nil {
		t.Fatalf("Failed to set remote description: %v", err)
	}

	// Verify the remote description was set
	// (We can't directly check the remote description, but if SetRemoteDescription
	// succeeded, it was set correctly)
}

func TestPeerConnectionClose(t *testing.T) {
	participant := domain.NewParticipant("participant-1", "Alice")
	config := DefaultPeerConnectionConfig()

	pc, err := NewPeerConnection(config, participant, nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}

	// Close the peer connection
	err = pc.Close()
	if err != nil {
		t.Fatalf("Failed to close PeerConnection: %v", err)
	}

	// Verify the state is closed
	if pc.State() != PeerConnectionStateClosed {
		t.Errorf("Expected state to be 'closed', got '%s'", pc.State())
	}

	// Verify operations on closed connection fail
	if _, err := pc.CreateOffer(); err == nil {
		t.Error("Expected CreateOffer to fail on closed connection")
	}
}

func TestPeerConnectionDoubleClose(t *testing.T) {
	participant := domain.NewParticipant("participant-1", "Alice")
	config := DefaultPeerConnectionConfig()

	pc, err := NewPeerConnection(config, participant, nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}

	// Close the peer connection twice
	err = pc.Close()
	if err != nil {
		t.Fatalf("First close failed: %v", err)
	}

	err = pc.Close()
	if err != nil {
		t.Fatalf("Second close failed: %v", err)
	}
}

func TestPeerConnectionConfig(t *testing.T) {
	participant := domain.NewParticipant("participant-1", "Alice")
	config := PeerConnectionConfig{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:custom.stun.server:19302"},
			},
		},
		SDPSemantics: webrtc.SDPSemanticsUnifiedPlan,
	}

	pc, err := NewPeerConnection(config, participant, nil)
	if err != nil {
		t.Fatalf("Failed to create PeerConnection: %v", err)
	}
	defer pc.Close()

	// Verify the config was set
	if len(pc.Config().ICEServers) != 1 {
		t.Error("Expected custom ICE servers")
	}

	if pc.Config().ICEServers[0].URLs[0] != "stun:custom.stun.server:19302" {
		t.Error("Expected custom STUN server URL")
	}

	if pc.Config().SDPSemantics != webrtc.SDPSemanticsUnifiedPlan {
		t.Error("Expected custom SDP semantics")
	}
}

func TestWebRTCTrackKindConversion(t *testing.T) {
	tests := []struct {
		webrtcKind webrtc.RTPCodecType
		domainKind domain.TrackKind
	}{
		{webrtc.RTPCodecTypeAudio, domain.TrackKindAudio},
		{webrtc.RTPCodecTypeVideo, domain.TrackKindVideo},
		{webrtc.RTPCodecType(99), domain.TrackKindAudio}, // Default case
	}

	for _, tt := range tests {
		if got := webrtcTrackKindToDomain(tt.webrtcKind); got != tt.domainKind {
			t.Errorf("webrtcTrackKindToDomain(%v) = %v, want %v", tt.webrtcKind, got, tt.domainKind)
		}
	}
}

func TestWebRTCTrackSourceConversion(t *testing.T) {
	tests := []struct {
		webrtcKind webrtc.RTPCodecType
		domainSource domain.TrackSource
	}{
		{webrtc.RTPCodecTypeAudio, domain.TrackSourceMicrophone},
		{webrtc.RTPCodecTypeVideo, domain.TrackSourceCamera},
		{webrtc.RTPCodecType(99), domain.TrackSourceMicrophone}, // Default case
	}

	for _, tt := range tests {
		if got := webrtcTrackSource(tt.webrtcKind); got != tt.domainSource {
			t.Errorf("webrtcTrackSource(%v) = %v, want %v", tt.webrtcKind, got, tt.domainSource)
		}
	}
}
