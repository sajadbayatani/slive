package signaling

import (
	"testing"
)

// TestPublishTrackAttachesToExistingParticipant verifies the room geometry in
// which the first participant is already connected when the second publishes.
// The existing participant must receive the new track through the SFU without
// depending on a second client-side subscribe request.
func TestPublishTrackAttachesToExistingParticipant(t *testing.T) {
	// This is the two-party geometry in which the first participant is
	// already publishing and connected when the second participant publishes.
	// The helper drives real loopback ICE/DTLS/SRTP/RTP and deliberately omits
	// the reverse subscribe request, so the old one-way behavior fails at the
	// server-offer or first->second/second->first media assertion.
	runGlareLateJoiner(t, "bidirectional-room", "first", "second")
}
