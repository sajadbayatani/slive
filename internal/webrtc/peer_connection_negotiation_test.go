package webrtc

import (
	"strings"
	"testing"

	"github.com/pion/webrtc/v3"
)

// mustSDPContain asserts that every fragment occurs in the SDP text.
func mustSDPContain(t *testing.T, sdp *SessionDescription, fragments ...string) {
	t.Helper()

	text := sdp.SDP()
	for _, fragment := range fragments {
		if !strings.Contains(text, fragment) {
			t.Errorf("expected SDP to contain %q, got:\n%s", fragment, text)
		}
	}
}

// TestPeerConnectionLoopbackNegotiation drives a complete offline SDP
// exchange between two peer connections: A creates an offer, B answers it,
// and A applies the answer as its remote description. Both configs are
// STUN-free so no network traffic leaves the process; each side still has a
// media section with ICE credentials because the helper adds a recvonly
// audio transceiver.
func TestPeerConnectionLoopbackNegotiation(t *testing.T) {
	offerer := newNegotiationTestPeerConnection(t, "offerer", "Alice")
	answerer := newNegotiationTestPeerConnection(t, "answerer", "Bob")

	offer, err := offerer.CreateOffer()
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}

	if offer.Type() != webrtc.SDPTypeOffer {
		t.Errorf("offer type = %q, want %q", offer.Type(), webrtc.SDPTypeOffer)
	}
	if offer.SDP() == "" {
		t.Fatal("expected non-empty offer SDP")
	}
	mustSDPContain(t, offer, "m=audio", "a=ice-ufrag")

	answer, err := answerer.CreateAnswer(offer)
	if err != nil {
		t.Fatalf("CreateAnswer: %v", err)
	}

	if answer.Type() != webrtc.SDPTypeAnswer {
		t.Errorf("answer type = %q, want %q", answer.Type(), webrtc.SDPTypeAnswer)
	}
	if answer.SDP() == "" {
		t.Fatal("expected non-empty answer SDP")
	}
	mustSDPContain(t, answer, "m=audio", "a=ice-ufrag")

	if err := offerer.SetRemoteDescription(answer); err != nil {
		t.Fatalf("SetRemoteDescription(answer): %v", err)
	}
}

// TestPeerConnectionOnICECandidateInvokesCallback verifies that the stored
// ICE-candidate callback receives candidates dispatched by the handler, and
// that the nil gathering-complete sentinel is not forwarded.
func TestPeerConnectionOnICECandidateInvokesCallback(t *testing.T) {
	pc := newNegotiationTestPeerConnection(t, "ice-reporter", "Carol")

	candidates := make(chan *ICECandidate, 16)
	pc.OnICECandidate(func(candidate *ICECandidate) {
		candidates <- candidate
	})

	wrapped, err := NewICECandidateFromString(
		"candidate:842163049 1 udp 1677729535 192.0.2.1 31102 typ srflx")
	if err != nil {
		t.Fatalf("NewICECandidateFromString: %v", err)
	}

	pc.handleICECandidate(nil) // sentinel must be swallowed
	pc.dispatchICECandidate(wrapped)

	select {
	case got := <-candidates:
		if got == nil {
			t.Fatal("expected a non-nil candidate")
		}
		if got.Candidate() != wrapped.Candidate() {
			t.Errorf("candidate = %q, want %q", got.Candidate(), wrapped.Candidate())
		}
	default:
		t.Fatal("expected the registered callback to receive the candidate")
	}
}
