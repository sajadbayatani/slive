package webrtc

import (
	"strings"
	"testing"

	pionwebrtc "github.com/pion/webrtc/v3"
	"github.com/sajadbayatani/slive/internal/domain"
)

// sdpMLineSections splits an SDP body into per-m-line sections (from each
// "m=" line up to the next one).
func sdpMLineSections(sdp string) []string {
	var sections []string
	var current []string
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "m=") && len(current) > 0 {
			sections = append(sections, strings.Join(current, "\n"))
			current = nil
		}
		current = append(current, line)
	}
	if len(current) > 0 {
		sections = append(sections, strings.Join(current, "\n"))
	}
	return sections
}

// firstMid extracts the a=mid value of a single m-line section.
func firstMid(section string) (string, bool) {
	for _, line := range strings.Split(section, "\n") {
		if after, ok := strings.CutPrefix(strings.TrimSpace(line), "a=mid:"); ok {
			return strings.TrimSpace(after), true
		}
	}
	return "", false
}

// newPublishRig is a glareTestRig without the pre-negotiated dummy
// transceiver: the publisher geometry under test starts from a clean stable
// PC whose only transceivers come from the exchanges the test drives.
func newPublishRig(t *testing.T, id string) *glareTestRig {
	t.Helper()
	rig := &glareTestRig{t: t}
	sender := func(msgType string, _ interface{}) error {
		switch msgType {
		case "webrtc:offer":
			rig.offers.Add(1)
		case "webrtc:answer":
			rig.answer.Add(1)
		}
		return nil
	}
	pc, err := NewPeerConnection(PeerConnectionConfig{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	}, domain.NewParticipant(id, "Publish"), sender)
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	rig.slive = pc

	brow, err := pionwebrtc.NewPeerConnection(pionwebrtc.Configuration{
		SDPSemantics: pionwebrtc.SDPSemanticsUnifiedPlanWithFallback,
	})
	if err != nil {
		t.Fatalf("browser pc: %v", err)
	}
	t.Cleanup(func() { _ = brow.Close() })
	if _, err := brow.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("browser transceiver: %v", err)
	}
	rig.brow = brow
	return rig
}

// Both tests in this file encode failures found by the live two-browser
// test (alice publish-first + bob late subscriber, both publishing and
// cross-subscribed on one unified PC per participant).

// TestPublishThenSubscribe_EgressKeepsOwnMIDs covers the publisher geometry:
// the browser's publish exchange runs first, so the server holds receive-only
// transceivers for the publisher's own media (MIDs 0..n). When a subscriber
// later triggers egress, pion's AddTrack must NOT attach the new sender to
// one of those publish transceivers — the subscriber offer would re-offer the
// publisher's own m-lines as sendrecv, the browser answers sendonly, and
// downstream media can never flow. Egress must land on a fresh transceiver
// carrying a reserved partition MID (>= egressMIDBase).
func TestPublishThenSubscribe_EgressKeepsOwnMIDs(t *testing.T) {
	rig := newPublishRig(t, "pub-then-sub")

	// Publisher exchange: browser offers (mid 0), server answers inline.
	answer, deferred, err := rig.slive.ProcessBrowserOffer(rig.browserOffer())
	if err != nil {
		t.Fatalf("ProcessBrowserOffer: %v", err)
	}
	if deferred {
		t.Fatal("publish offer deferred on stable PC, want inline answer")
	}
	rig.browserAcceptAnswer(answer)
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable after publish exchange", st)
	}

	// A remote track becomes available: egress AddTrack + sync drive.
	track := newTestLocalTrack(t, "pub-then-sub-audio", domain.TrackKindAudio)
	if err := rig.slive.AddTrack(track); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)

	// The subscriber offer legitimately re-offers the publisher's own m-line
	// (mid 0, recvonly). The regression is DIRECTION: without re-homing, the
	// egress sender upgrades that section to sendrecv/sendonly. Every
	// non-egress section must remain recvonly; egress rides partition MIDs.
	offerSDP := rig.slive.PionPeerConnection().LocalDescription().SDP
	for _, section := range sdpMLineSections(offerSDP) {
		mid, ok := firstMid(section)
		if !ok {
			continue
		}
		if isEgressMID(mid) {
			continue
		}
		if strings.Contains(section, "a=sendrecv") || strings.Contains(section, "a=sendonly") {
			t.Errorf("publish section mid %q was upgraded for egress (sendrecv/sendonly): %s", mid, section)
		}
	}
	if got := countEgressMIDs(t, offerSDP); got < 1 {
		t.Errorf("subscriber offer carries %d egress MIDs, want >= 1", got)
	}
	egress := egressSendersFor(rig.slive, track.ID())
	if len(egress) != 1 {
		t.Fatalf("egress senders for %s = %d, want 1", track.ID(), len(egress))
	}
	if !isEgressMID(egress[0].Mid()) {
		t.Errorf("egress transceiver MID %q outside egress partition", egress[0].Mid())
	}
	rig.answerSliveOffer()
}

// TestAnswerDTLSRoleStaysPassive covers the role-flip rejection: after the
// browser answers a server subscriber offer with a=setup:active (browser =
// DTLS client), the server's answer to a subsequent publish re-offer must
// keep a=setup:passive. Pion's default answer role (client/active) would flip
// the established role and Chrome rejects the whole answer with
// "Failed to set SSL role for the transport", deadlocking the publish
// exchange on a PC that ever carried a subscriber offer.
func TestAnswerDTLSRoleStaysPassive(t *testing.T) {
	rig := newGlareTestRig(t, "dtls-role")

	// Subscriber exchange first: server offers, browser answers
	// (a=setup:active -> browser is the DTLS client).
	if err := rig.slive.AddTrack(newTestLocalTrack(t, "dtls-role-audio", domain.TrackKindAudio)); err != nil {
		t.Fatalf("AddTrack: %v", err)
	}
	rig.slive.handleNegotiationNeeded()
	waitForOutstandingOffer(rig, 1)
	rig.answerOutstandingOffer()

	// Browser publish re-offer: existing subscriber receiver plus a new
	// send-only publish section.
	if _, err := rig.brow.AddTransceiverFromKind(pionwebrtc.RTPCodecTypeAudio,
		pionwebrtc.RTPTransceiverInit{Direction: pionwebrtc.RTPTransceiverDirectionSendonly}); err != nil {
		t.Fatalf("browser publish transceiver: %v", err)
	}
	answer, deferred, err := rig.slive.ProcessBrowserOffer(rig.browserOffer())
	if err != nil {
		t.Fatalf("ProcessBrowserOffer: %v", err)
	}
	if deferred {
		t.Fatal("publish re-offer deferred on stable PC, want inline answer")
	}

	ansSDP := answer.SDP()
	if strings.Contains(ansSDP, "a=setup:active") {
		t.Error("server answer declares a=setup:active; established role has the browser as DTLS client — Chrome would reject this answer")
	}
	if !strings.Contains(ansSDP, "a=setup:passive") {
		t.Error("server answer does not declare a=setup:passive")
	}

	// The browser side must accept it.
	rig.browserAcceptAnswer(answer)
	if st := glareState(rig); st != pionwebrtc.SignalingStateStable {
		t.Fatalf("slive state = %s, want stable after publish re-offer", st)
	}
}
